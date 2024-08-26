import os
import json
from typing import List
from typing import Tuple
from typing import Optional
from typing import Set
from tempfile import NamedTemporaryFile
from operator import attrgetter
from concurrent.futures import ThreadPoolExecutor

import polars as pl

from cubic_loader.utils.aws import s3_list_objects
from cubic_loader.utils.aws import s3_get_object
from cubic_loader.utils.aws import s3_object_exists
from cubic_loader.utils.aws import s3_split_object_path
from cubic_loader.utils.aws import s3_upload_file
from cubic_loader.utils.remote_locations import S3_ARCHIVE
from cubic_loader.utils.remote_locations import S3_ERROR
from cubic_loader.utils.remote_locations import QLIK
from cubic_loader.utils.remote_locations import ODS_STATUS
from cubic_loader.utils.remote_locations import ODS_SCHEMA
from cubic_loader.utils.postgres import DatabaseManager
from cubic_loader.utils.postgres import remote_csv_gz_copy
from cubic_loader.qlik.rds_utils import create_tables_from_schema
from cubic_loader.qlik.rds_utils import create_history_table_partitions
from cubic_loader.qlik.rds_utils import add_columns_to_table
from cubic_loader.qlik.rds_utils import drop_table
from cubic_loader.qlik.utils import DFMDetails
from cubic_loader.qlik.utils import DFMSchemaFields
from cubic_loader.qlik.utils import re_get_first
from cubic_loader.qlik.utils import RE_SNAPSHOT_TS
from cubic_loader.qlik.utils import RE_CDC_TS
from cubic_loader.qlik.utils import TableStatus
from cubic_loader.utils.logger import ProcessLogger


DFM_COLUMN_SCHEMA = pl.Schema(
    {
        "oridnal": pl.Int64(),
        "name": pl.String(),
        "type": pl.String(),
        "length": pl.Int64(),
        "precision": pl.Int64(),
        "scale": pl.Int64(),
        "primaryKeyPos": pl.Int64(),
    }
)
CDC_COLUMNS = (
    "header__change_seq",
    "header__change_oper",
    "header__timestamp",
)


def dfm_schema_to_json(dfm_file: DFMDetails) -> List[DFMSchemaFields]:
    """
    extract table schema from .dfm as json
    """
    dfm_json = json.load(s3_get_object(dfm_file.path))
    return dfm_json["dataInfo"]["columns"]


def dfm_schema_to_df(dfm_file: DFMDetails) -> pl.DataFrame:
    """
    extract table schema from .dfm and convert to Polars Dataframe
    """
    json_schema = dfm_schema_to_json(dfm_file)
    return pl.DataFrame(
        json_schema,
        schema=DFM_COLUMN_SCHEMA,
    )


def status_schema_to_df(status: TableStatus) -> pl.DataFrame:
    """
    extract table schema from TableStatus and convert to Polars Dataframe
    """
    return pl.DataFrame(
        status.last_schema,
        schema=DFM_COLUMN_SCHEMA,
    )


def get_snapshot_dfms(table: str) -> List[DFMDetails]:
    """find all available snapshot dfm files for a qlik table from Archive and Error buckets"""
    prefix = os.path.join(QLIK, f"{table}/")
    archive_dfms = s3_list_objects(S3_ARCHIVE, prefix, in_filter=".dfm")
    error_dfms = s3_list_objects(S3_ERROR, prefix, in_filter=".dfm")

    found_snapshots = []
    for dfm in archive_dfms + error_dfms:
        found_snapshots.append(DFMDetails(path=dfm, ts=re_get_first(dfm, RE_SNAPSHOT_TS)))

    assert len(found_snapshots) > 0

    return sorted(found_snapshots, key=attrgetter("ts"))


def get_cdc_dfms(etl_status: TableStatus, table: str) -> List[DFMDetails]:
    """
    find all available CDC dfm files for a Snapshot from Archive and Error buckets

    :return: List of ChangeFile objects sorted by 'ts' (Ascending)
    """
    prefix = os.path.join(QLIK, f"{table}__ct/")

    # add archive files from snapshot folder
    cdc_dfms: List[DFMDetails] = []
    for dfm_file in s3_list_objects(
        S3_ARCHIVE, f"{prefix}snapshot={etl_status.current_snapshot_ts}/", in_filter=".dfm"
    ):
        cdc_dfms.append(DFMDetails(path=dfm_file, ts=re_get_first(dfm_file, RE_CDC_TS)))

    # filter error files from table folder
    for dfm_file in s3_list_objects(S3_ERROR, prefix, in_filter=".dfm"):
        if re_get_first(dfm_file, RE_SNAPSHOT_TS) > etl_status.current_snapshot_ts:
            cdc_dfms.append(DFMDetails(path=dfm_file, ts=re_get_first(dfm_file, RE_CDC_TS)))

    cdc_dfms = [dfm for dfm in cdc_dfms if dfm.ts > etl_status.last_cdc_ts]

    return sorted(cdc_dfms, key=attrgetter("ts"))


def thread_load_cdc_file(args: Tuple[DFMDetails, TableStatus]) -> Tuple[Optional[str], Optional[List[DFMSchemaFields]]]:
    """
    work to load cdc file from S3 into RDS history table
    """
    dfm, status = args
    logger = ProcessLogger("load_cdc_file", dfm=dfm.path, table=status.db_fact_table)
    try:
        dfm_schema = dfm_schema_to_df(dfm)
        dfm_names = ",".join(dfm_schema.get_column("name"))
        dfm_name_set = set(dfm_schema.get_column("name"))

        # check dfm schema contains CDC_COLUMNS
        assert set(CDC_COLUMNS).issubset(dfm_name_set)
        dfm_schema = dfm_schema.filter(pl.col("name").is_in(CDC_COLUMNS).not_())

        truth_schema = status_schema_to_df(status)
        truth_name_set = set(truth_schema.get_column("name"))
        new_columns = dfm_schema.join(
            truth_schema,
            on=truth_schema.columns,
            how="anti",
            join_nulls=True,
        )

        # new_columns found, can not load csv
        if new_columns.shape[0] > 0:
            # check if new_columns contains columns in truth schema, would
            # indicate that a different dimension of the table changed (type, primary key)
            new_truth_common = truth_name_set.intersection(set(new_columns.get_column("name")))
            assert len(new_truth_common) == 0, f"column dimension changed for {new_truth_common}"

            new_col_list: List[DFMSchemaFields] = new_columns.to_dicts()  # type: ignore
            logger.log_complete(new_columns_found=str(new_col_list))
            return (dfm.path, new_col_list)

        csv_file = dfm.path.replace(".dfm", ".csv.gz")
        table = f"{ODS_SCHEMA}.{status.db_fact_table}_history"
        remote_csv_gz_copy(csv_file, table, dfm_names)

        logger.log_complete()

    except Exception as exception:
        logger.log_failure(exception)
        return (None, None)

    return (dfm.ts, None)


# pylint: disable=too-many-instance-attributes
class CubicODSQlik:
    """
    manager class for loading operations of Cubic ODS Qlik tables

    used to load ODS data from S3 bucket to RDS tables
    """

    def __init__(self, table: str, db: DatabaseManager):
        """
        :param table: Cubic ODS Table Name eg ("EDW.CARD_DIMENSION")
        """
        self.table = table
        self.db = db
        self.status_path = os.path.join(ODS_STATUS, f"{self.table}.json")
        self.db_fact_table = table.replace(".", "_").lower()
        self.db_history_table = f"{self.db_fact_table}_history"
        self.db_load_table = f"{self.db_fact_table}_load"
        self.s3_snapshot_dfms = get_snapshot_dfms(table)
        self.last_s3_snapshot_dfm = self.s3_snapshot_dfms[-1]
        self.etl_status = self.load_etl_status()

    def update_status(
        self,
        snapshot_ts: Optional[str] = None,
        last_cdc_ts: Optional[str] = None,
        last_schema: Optional[List[DFMSchemaFields]] = None,
    ) -> None:
        """update any etl_status field"""
        new_shapshot_ts = self.etl_status.current_snapshot_ts
        if snapshot_ts is not None:
            new_shapshot_ts = snapshot_ts
        new_cdc_ts = self.etl_status.last_cdc_ts
        if last_cdc_ts is not None:
            new_cdc_ts = last_cdc_ts
        new_last_schema = self.etl_status.last_schema
        if last_schema is not None:
            new_last_schema = last_schema

        self.etl_status = TableStatus(
            db_fact_table=self.db_fact_table,
            current_snapshot_ts=new_shapshot_ts,
            last_cdc_ts=new_cdc_ts,
            last_schema=new_last_schema,
        )

    def load_etl_status(self) -> TableStatus:
        """pull and verify table status from file"""
        if s3_object_exists(self.status_path):
            last_status = json.load(s3_get_object(self.status_path))
            return_satus = TableStatus(
                db_fact_table=self.db_fact_table,
                current_snapshot_ts=last_status["current_snapshot_ts"],
                last_cdc_ts=last_status["last_cdc_ts"],
                last_schema=last_status["last_schema"],
            )
        else:
            return_satus = TableStatus(
                db_fact_table=self.db_fact_table,
                current_snapshot_ts=self.last_s3_snapshot_dfm.ts,
                last_cdc_ts="",
                last_schema=dfm_schema_to_json(self.last_s3_snapshot_dfm),
            )
            self.save_status(return_satus)

        return return_satus

    def save_status(self, status: TableStatus) -> None:
        """write status to file path"""
        with NamedTemporaryFile(mode="w+") as f:
            json.dump(status._asdict(), f)
            f.flush()
            s3_upload_file(f.name, self.status_path)

    def rds_snapshot_load(self) -> None:
        """Perform load of initial load files to history table"""
        self.db.execute(create_history_table_partitions(self.db_history_table, self.last_s3_snapshot_dfm.ts))

        bucket, prefix = s3_split_object_path(self.last_s3_snapshot_dfm.path.rsplit("/", maxsplit=1)[0])
        for s3_path in s3_list_objects(bucket, prefix, in_filter=".csv.gz"):
            remote_csv_gz_copy(s3_path, f"{ODS_SCHEMA}.{self.db_load_table}")

        load_update = (
            f"UPDATE {ODS_SCHEMA}.{self.db_load_table} "
            f"SET header__timestamp=to_timestamp('{self.last_s3_snapshot_dfm.ts}','YYYYMMDDTHHMISSZ') "
            f", header__change_oper='L' "
            f", header__change_seq='SNAPSHOT_LOAD'"
            f"WHERE header__timestamp IS NULL;"
        )
        self.db.execute(load_update)
        table_copy = (
            f"INSERT INTO {ODS_SCHEMA}.{self.db_history_table} " f"SELECT * FROM {ODS_SCHEMA}.{self.db_load_table};"
        )
        self.db.execute(table_copy)
        self.db.truncate_table(f"{ODS_SCHEMA}.{self.db_load_table}")
        self.db.vaccuum_analyze(f"{ODS_SCHEMA}.{self.db_history_table}")

        self.update_status(
            self.etl_status.current_snapshot_ts,
            "0",
            self.etl_status.last_schema,
        )

    def rds_cdc_load(self) -> None:
        """Perform load of CDC files to history table"""
        current_cdc_ts = self.etl_status.last_cdc_ts
        current_schema = self.etl_status.last_schema
        error_files: List[str] = []
        new_column_set: Set[str] = set()
        work_files = [(wf, self.etl_status) for wf in get_cdc_dfms(self.etl_status, self.table)]
        with ThreadPoolExecutor(max_workers=os.cpu_count()) as pool:
            for result in pool.map(thread_load_cdc_file, work_files):
                dfm_result, new_columns = result
                if new_columns is None and dfm_result is not None:
                    # load was success, save cdc_ts if needed
                    current_cdc_ts = max(current_cdc_ts, dfm_result)
                elif new_columns is not None and dfm_result:
                    # handle file with new columns added
                    error_files.append(dfm_result)
                    for column in new_columns:
                        new_column_set.add(json.dumps(column))

        if len(error_files) > 0:
            # handle new column additions
            new_columns_to_add: List[DFMSchemaFields] = [json.loads(column) for column in new_column_set]
            self.db.execute(add_columns_to_table(new_columns_to_add, self.db_fact_table))
            for column in new_columns_to_add:
                current_schema.append(column)
            self.update_status(last_schema=current_schema)
            # re-process failed files with added columns
            work_files = [
                (DFMDetails(path=path, ts=re_get_first(path, RE_CDC_TS)), self.etl_status) for path in error_files
            ]
            with ThreadPoolExecutor(max_workers=os.cpu_count()) as pool:
                for result in pool.map(thread_load_cdc_file, work_files):
                    dfm_result, new_columns = result
                    if new_columns is None and dfm_result is not None:
                        current_cdc_ts = max(current_cdc_ts, dfm_result)

        # Delete header__change_oper='B' as they are redundant
        del_b_records = f"DELETE FROM {ODS_SCHEMA}.{self.db_history_table} WHERE header__change_oper = 'B';"
        self.db.execute(del_b_records)

        self.db.vaccuum_analyze(f"{ODS_SCHEMA}.{self.db_history_table}")

        self.update_status(
            last_cdc_ts=current_cdc_ts,
            last_schema=current_schema,
        )

    def rds_fact_table_load(self) -> None:
        """Load FACT Table records from History Table"""
        logger = ProcessLogger("load_fact_table", table=self.db_fact_table)
        schema = self.etl_status.last_schema

        table_columns = ",".join([col["name"] for col in schema])
        keys = ",".join([col["name"] for col in schema if col["primaryKeyPos"] > 0])

        fact_table = f"{ODS_SCHEMA}.{self.db_fact_table}"
        history_table = f"{ODS_SCHEMA}.{self.db_history_table}"

        fact_query = (
            f"INSERT INTO {fact_table} ({table_columns}) "
            f"SELECT {table_columns} FROM "
            f"("
            f"SELECT DISTINCT ON ({keys}) {table_columns},header__change_oper "
            f"FROM {history_table} "
            f"ORDER BY {keys},header__timestamp DESC "
            f") t_load "
            f"WHERE t_load.header__change_oper <> 'D'"
            ";"
        )

        self.db.truncate_table(fact_table)
        self.db.execute(fact_query)

        self.db.vaccuum_analyze(fact_table)

        logger.log_complete()

    def run_etl(self) -> None:
        """
        Run table ETL Process

        Currently this business logic will only process the latest QLIK "Snapshot" that has been issued
        a feature will need to be added that will handle the issuance of new snapshots that should include:
            - making sure all outstandind cdc files from last snapshot are loaded
            - truncating the fact table
            - resetting the table status file to the new snapshot
            - re-doing initial load operations to history and fact tables
        """
        logger = ProcessLogger(
            "ods_qlik_run_etl",
            table=self.table,
            load_snapshot_ts=self.etl_status.current_snapshot_ts,
            last_cdc_ts=self.etl_status.last_cdc_ts,
        )
        try:
            assert (
                self.etl_status.current_snapshot_ts == self.last_s3_snapshot_dfm.ts
            ), f"Expected LOAD SNAPSHOT has changed from {self.etl_status.current_snapshot_ts} to {self.last_s3_snapshot_dfm.ts}"

            # create tables and history table partitions
            # will be no-op if tables already exist
            self.db.execute(create_tables_from_schema(self.etl_status.last_schema, self.db_fact_table))
            self.db.execute(create_history_table_partitions(self.db_history_table))

            if self.etl_status.last_cdc_ts == "":
                self.rds_snapshot_load()

            self.rds_cdc_load()
            self.rds_fact_table_load()

            self.db.execute(drop_table(self.db_load_table))

            self.save_status(self.etl_status)
            logger.log_complete()

        except Exception as exception:
            logger.log_failure(exception)


# pylint: enable=too-many-instance-attributes
