import os
import json
import hashlib
import shutil
import tempfile
from itertools import batched
from typing import List
from typing import Tuple
from typing import Optional
from tempfile import NamedTemporaryFile
from operator import attrgetter
from concurrent.futures import ThreadPoolExecutor

import polars as pl

from cubic_loader.utils.aws import s3_list_objects
from cubic_loader.utils.aws import s3_get_object
from cubic_loader.utils.aws import s3_object_exists
from cubic_loader.utils.aws import s3_split_object_path
from cubic_loader.utils.aws import s3_upload_file
from cubic_loader.utils.aws import s3_delete_object
from cubic_loader.utils.aws import s3_download_object
from cubic_loader.utils.remote_locations import S3_ARCHIVE
from cubic_loader.utils.remote_locations import QLIK
from cubic_loader.utils.remote_locations import ODS_STATUS
from cubic_loader.utils.remote_locations import ODS_SCHEMA
from cubic_loader.utils.remote_locations import ODIN_PROCESSED
from cubic_loader.utils.postgres import DatabaseManager
from cubic_loader.utils.postgres import remote_csv_gz_copy
from cubic_loader.utils.postgres import header_from_csv_gz
from cubic_loader.qlik.rds_utils import create_tables_from_schema
from cubic_loader.qlik.rds_utils import create_history_table_partitions
from cubic_loader.qlik.rds_utils import add_columns_to_table
from cubic_loader.qlik.rds_utils import drop_table
from cubic_loader.qlik.rds_utils import bulk_delete_from_temp
from cubic_loader.qlik.rds_utils import bulk_update_from_temp
from cubic_loader.qlik.rds_utils import bulk_insert_from_temp
from cubic_loader.qlik.utils import key_column_join_type
from cubic_loader.qlik.utils import DFMDetails
from cubic_loader.qlik.utils import DFMSchemaFields
from cubic_loader.qlik.utils import re_get_first
from cubic_loader.qlik.utils import RE_SNAPSHOT_TS
from cubic_loader.qlik.utils import CDC_COLUMNS
from cubic_loader.qlik.utils import MERGED_FNAME
from cubic_loader.qlik.utils import RE_CDC_TS
from cubic_loader.qlik.utils import TableStatus
from cubic_loader.qlik.utils import threading_cpu_count
from cubic_loader.qlik.utils import merge_cdc_csv_gz_files
from cubic_loader.qlik.utils import dfm_schema_to_json
from cubic_loader.qlik.utils import status_schema_to_df
from cubic_loader.qlik.utils import dfm_schema_to_df
from cubic_loader.qlik.utils import dataframe_from_merged_csv
from cubic_loader.qlik.utils import s3_list_cdc_gz_objects
from cubic_loader.utils.logger import ProcessLogger


def get_snapshot_dfms(table: str) -> List[DFMDetails]:
    """find all available snapshot dfm files for a qlik table from Archive bucket"""
    prefix = os.path.join(ODIN_PROCESSED, QLIK, f"{table}/")
    archive_dfms = s3_list_objects(S3_ARCHIVE, prefix, in_filter=".dfm")

    found_snapshots = []
    for dfm in archive_dfms:
        found_snapshots.append(DFMDetails(path=dfm, ts=re_get_first(dfm, RE_SNAPSHOT_TS)))

    assert len(found_snapshots) > 0

    return sorted(found_snapshots, key=attrgetter("ts"))


def get_cdc_gz_csvs(etl_status: TableStatus, table: str) -> List[str]:
    """
    find all available CDC csv.gz files for a Snapshot from Archive and Error buckets
    :param etl_status: status of ETL operation
    :param table: CUBIC Table Name

    :return: List of ChangeFile objects sorted by 'ts' (Ascending)
    """
    table_prefix = os.path.join(ODIN_PROCESSED, QLIK, f"{table}__ct/")
    snapshot_prefix = f"{table_prefix}snapshot={etl_status.current_snapshot_ts}/"

    cdc_csvs = s3_list_cdc_gz_objects(S3_ARCHIVE, snapshot_prefix, min_ts=etl_status.last_cdc_ts)

    return sorted(cdc_csvs, key=lambda l: re_get_first(l, RE_CDC_TS))


def thread_save_csv_file(args: Tuple[str, str]) -> None:
    """
    work to download and partition cdc files

    - download csv.gz file to tmp_folder
    - encode header row as sha1 hash for foldername
    - move file into hash foldername
    """
    csv_object, tmp_dir = args
    logger = ProcessLogger("download_cdc_file", csv_object=csv_object)

    try:
        csv_local_file = csv_object.replace("s3://", "").replace("/", "|")
        csv_local_path = os.path.join(tmp_dir, csv_local_file)
        s3_download_object(csv_object, csv_local_path)

        csv_headers = header_from_csv_gz(csv_local_path)
        hash_folder = os.path.join(tmp_dir, hashlib.sha1(csv_headers.encode("utf8")).hexdigest())

        os.makedirs(hash_folder, exist_ok=True)
        os.rename(csv_local_path, os.path.join(hash_folder, csv_local_file))

        logger.log_complete()

    except Exception as exception:
        logger.log_failure(exception)


# pylint: disable=too-many-instance-attributes
class CubicODSQlik:
    """
    manager class for loading operations of Cubic ODS Qlik tables

    used to load ODS data from S3 bucket to RDS tables
    """

    def __init__(self, table: str, db: DatabaseManager, schema: str = ODS_SCHEMA):
        """
        :param table: Cubic ODS Table Name eg ("EDW.CARD_DIMENSION")
        """
        self.db = db
        self.table = table
        self.status_path = os.path.join(ODS_STATUS, f"{table}.json")
        self.db_fact_table = f"{schema}.{table.replace(".", "_").lower()}"
        self.db_history_table = f"{self.db_fact_table}_history"
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
                last_schema=dfm_schema_to_json(self.last_s3_snapshot_dfm.path),
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
        # Create _history partitions to cover header__timestamp values of initial load
        self.db.execute(create_history_table_partitions(self.db_history_table, self.last_s3_snapshot_dfm.ts))

        # Load all csv.gz files from snapshot folder into _load table
        bucket, prefix = s3_split_object_path(self.last_s3_snapshot_dfm.path.rsplit("/", maxsplit=1)[0])
        for s3_path in s3_list_objects(bucket, prefix, in_filter=".csv.gz"):
            remote_csv_gz_copy(s3_path, f"{self.db_fact_table}_load")

        # update header__ columns in _load table
        load_update = (
            f"UPDATE {self.db_fact_table}_load "
            f"SET header__timestamp=to_timestamp('{self.last_s3_snapshot_dfm.ts}','YYYYMMDDTHHMISSZ') "
            f", header__change_oper='L' "
            f", header__change_seq=rpad(regexp_replace('{self.last_s3_snapshot_dfm.ts}','\\D','','g'),35,'0')::numeric "
            f"WHERE header__timestamp IS NULL;"
        )
        self.db.execute(load_update)

        # Load records from _load table into _history table
        history_table_copy = f"INSERT INTO {self.db_history_table} SELECT * FROM {self.db_fact_table}_load;"
        self.db.execute(history_table_copy)
        self.db.vaccuum_analyze(f"{self.db_history_table}")

        # Load records from _load table into fact table
        table_columns = [col["name"] for col in self.etl_status.last_schema]
        table_column_str = ",".join(table_columns)
        fact_table_copy = (
            f"INSERT INTO {self.db_fact_table} ({table_column_str}) "
            f"SELECT {table_column_str} FROM {self.db_fact_table}_load;"
        )
        self.db.execute(fact_table_copy)
        self.db.vaccuum_analyze(f"{self.db_fact_table}")

        self.update_status(
            self.etl_status.current_snapshot_ts,
            "0",
            self.etl_status.last_schema,
        )

    def cdc_verify_schema(self, dfm_object: str) -> None:
        """
        Verify SCHEMA of merged csv file by inspected dfm_object file of one csv.gz file

        If new columns are found, add them to RDS tables.

        If column dimension changed (such as Type or Primary Key designation) raise Error

        :param dfm_object: S3 path of .dfm file that wil be used for verification
        """
        cdc_schema = dfm_schema_to_df(dfm_object)
        cdc_name_set = set(cdc_schema.get_column("name"))

        # check dfm schema contains CDC_COLUMNS
        assert set(CDC_COLUMNS).issubset(cdc_name_set)
        cdc_schema = cdc_schema.filter(pl.col("name").is_in(CDC_COLUMNS).not_())

        truth_schema = status_schema_to_df(self.etl_status)
        truth_name_set = set(truth_schema.get_column("name"))
        new_columns = cdc_schema.join(
            truth_schema,
            on=truth_schema.columns,
            how="anti",
            join_nulls=True,
        )

        # cdc_schema and truth_schema overlap, no action needed
        if new_columns.shape[0] == 0:
            return

        # check if new_columns contains columns in truth schema, would
        # indicate that a different dimension of the table changed (type, primary key)
        new_truth_common = truth_name_set.intersection(set(new_columns.get_column("name")))
        assert len(new_truth_common) == 0, f"column dimension changed for {new_truth_common} from {dfm_object}"

        add_columns: List[DFMSchemaFields] = new_columns.to_dicts()  # type: ignore
        self.db.execute(add_columns_to_table(add_columns, self.db_fact_table))
        current_schema = self.etl_status.last_schema
        for column in add_columns:
            current_schema.append(column)
        self.update_status(last_schema=current_schema)

    def cdc_update(self, cdc_df: pl.DataFrame, tmp_table: str, key_columns: List[str]) -> None:
        """
        Perform UPDATE from cdc dataframe
        """
        # Perform UPDATE Operations on fact table for each column indivduallly
        for update_col in cdc_df.columns:
            if update_col in key_columns or update_col in CDC_COLUMNS:
                continue

            update_df = (
                cdc_df.filter(
                    pl.col("header__change_oper").eq("U"),
                    pl.col(update_col).is_not_null(),
                )
                .sort(by="header__change_seq", descending=True)
                .unique(key_columns, keep="first")
                .select(key_columns + [update_col])
            )
            if update_df.shape[0] == 0:
                continue

            with tempfile.TemporaryDirectory() as tmp_dir:
                update_csv_path = os.path.join(tmp_dir, "update.csv")
                update_df.write_csv(update_csv_path, quote_style="necessary")
                self.db.truncate_table(tmp_table)
                remote_csv_gz_copy(update_csv_path, tmp_table)

            op_and_key = key_column_join_type(update_df, key_columns)
            update_q = bulk_update_from_temp(self.db_fact_table, update_col, op_and_key)
            self.db.execute(update_q)

    def cdc_delete(self, cdc_df: pl.DataFrame, tmp_table: str, key_columns: List[str]) -> None:
        """
        Perform DELETE from cdc dataframe
        """
        delete_df = (
            cdc_df.sort(by="header__change_seq", descending=True)
            .unique(key_columns, keep="first")
            .filter(pl.col("header__change_oper").eq("D"))
            .select(key_columns)
        )
        if delete_df.shape[0] == 0:
            return

        op_and_key = key_column_join_type(delete_df, key_columns)
        delete_q = bulk_delete_from_temp(self.db_fact_table, op_and_key)

        with tempfile.TemporaryDirectory() as tmp_dir:
            delete_csv_path = os.path.join(tmp_dir, "delete.csv")
            delete_df.write_csv(delete_csv_path, quote_style="necessary")
            self.db.truncate_table(tmp_table)
            remote_csv_gz_copy(delete_csv_path, tmp_table)
        self.db.execute(delete_q)

    def cdc_insert(self, cdc_df: pl.DataFrame, tmp_table: str) -> None:
        """
        Perform INSERT from cdc dataframe
        """
        insert_df = cdc_df.filter(pl.col("header__change_oper").eq("I")).drop(CDC_COLUMNS)
        if insert_df.shape[0] == 0:
            return

        insert_q = bulk_insert_from_temp(self.db_fact_table, tmp_table, insert_df.columns)
        with tempfile.TemporaryDirectory() as tmp_dir:
            insert_path = os.path.join(tmp_dir, "insert.csv")
            insert_df.write_csv(insert_path, quote_style="necessary")
            self.db.truncate_table(tmp_table)
            remote_csv_gz_copy(insert_path, tmp_table)
        self.db.execute(insert_q)

    def cdc_load_folder(self, load_folder: str) -> None:
        """
        load all cdc.csv.gz file from load_folder into RDS

        1. Merge all csv.gz files into one MERGED_FNAME csv file
        2. Verify SCHEMA of MERGED_FNAME matches RDS tables
        3. Load MERGED_FNAME csv file into self.db_history_table table
        4. Load INSERT records from MERGED_FNAME into self.db_fact_table
        5. For each non-key column of MERGED_FNAME, load UPDATE records into self.db_fact_table
        6. Perform DELETE operataions from MERGED_FNAME on self.db_fact_table
        7. Delete load_folder folder

        :param load_folder: folder containing all csv.gz files to be loaded
        """
        logger = ProcessLogger("cdc_load_folder", load_folder=load_folder, table=self.db_fact_table)
        try:
            dfm_object = os.listdir(load_folder)[0].replace(".csv.gz", ".dfm").replace("|", "/")
            merge_csv = os.path.join(load_folder, MERGED_FNAME)
            key_columns = [col["name"].lower() for col in self.etl_status.last_schema if col["primaryKeyPos"] > 0]
            load_table = f"{self.db_fact_table}_load"

            cdc_ts = merge_cdc_csv_gz_files(load_folder)
            self.cdc_verify_schema(dfm_object)
            cdc_df = dataframe_from_merged_csv(merge_csv, dfm_object)

            # Load records into _history table
            self.db.truncate_table(load_table)
            remote_csv_gz_copy(merge_csv, load_table)
            self.db.execute(bulk_insert_from_temp(self.db_history_table, load_table, cdc_df.columns))

            self.cdc_insert(cdc_df, load_table)

            self.cdc_update(cdc_df, load_table, key_columns)

            self.cdc_delete(cdc_df, load_table, key_columns)

            self.update_status(last_cdc_ts=max(cdc_ts, self.etl_status.last_cdc_ts))
            logger.log_complete()

        except Exception as exception:
            logger.log_failure(exception)

        shutil.rmtree(load_folder, ignore_errors=True)
        self.db.vaccuum_analyze(self.db_history_table)
        self.db.vaccuum_analyze(self.db_fact_table)

    def cdc_check_load_folders(self, tmp_dir: str, max_folder_bytes: int = 0) -> None:
        """
        Check all cdc hash folders in tmp_dir
        if
            size of hash folder is larger than max_folder_bytes
            or more than 5000 files in folder
        then load folder files into RDS

        :param tmp_dir: folder containing cdc hash folder partitions
        :param max_folder_bytes: folder size threshold to trigger load operation
        """
        for folder in os.listdir(tmp_dir):
            load_folder = os.path.join(tmp_dir, folder)
            if not os.path.isdir(load_folder):
                continue
            file_list = os.listdir(load_folder)
            folder_count = len(file_list)
            folder_bytes = sum(os.path.getsize(os.path.join(load_folder, f)) for f in file_list)
            if folder_bytes > max_folder_bytes or folder_count > 5_000:
                self.cdc_load_folder(load_folder)

    def process_cdc_files(self) -> None:
        """
        1. download cdc files in batches
        2. extract header row from each cdc file, convert it to a sha1 hash to be used as a folder name
        3. move cdc file to hash folder for later merging
        4. when hash folder size reaches threshold limit, load folder into RDS
        """
        pool = ThreadPoolExecutor(max_workers=threading_cpu_count())

        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
            work_files = [(wf, tmp_dir) for wf in get_cdc_gz_csvs(self.etl_status, self.table)]
            for batch in batched(work_files, 10):
                # Download batch of cdc csv.gz files
                for _ in pool.map(thread_save_csv_file, batch):
                    pass

                # load any cdc hash folder greater than max_folder_bytes
                self.cdc_check_load_folders(tmp_dir, max_folder_bytes=60_000_000)

            # load all remaining cdc hash folders
            self.cdc_check_load_folders(tmp_dir)

        pool.shutdown()

    def snapshot_reset(self) -> None:
        """
        Reset database tables and status files for new snapshot

        Those will lose all history from history table
        """

        s3_delete_object(self.status_path)
        self.etl_status = self.load_etl_status()

        self.db.execute(drop_table(self.db_history_table))
        self.db.truncate_table(self.db_fact_table, restart_identity=True, cascade=True)

    def run_etl(self) -> None:
        """
        Run table ETL Process

        Currently this business logic will only process the latest QLIK Snapshot that has been issued
        If a new QLIK Snapshot is detected, all existing tables will be dropped and whole process will be
        reset to load NEW Snapshot
        """
        logger = ProcessLogger(
            "ods_qlik_run_etl",
            table=self.db_fact_table,
            load_snapshot_ts=self.etl_status.current_snapshot_ts,
            last_cdc_ts=self.etl_status.last_cdc_ts,
        )
        try:
            if self.etl_status.current_snapshot_ts != self.last_s3_snapshot_dfm.ts:
                new_snapshot_logger = ProcessLogger(
                    "ods_snapshot_change",
                    table=self.db_fact_table,
                    old_shapshot=self.etl_status.current_snapshot_ts,
                    new_shapshot=self.last_s3_snapshot_dfm.ts,
                )
                self.snapshot_reset()
                new_snapshot_logger.log_complete()

            # create tables and history table partitions
            # will be no-op if tables already exist
            self.db.execute(create_tables_from_schema(self.etl_status.last_schema, self.db_fact_table))
            self.db.execute(create_history_table_partitions(self.db_history_table))

            if self.etl_status.last_cdc_ts == "":
                self.rds_snapshot_load()

            self.process_cdc_files()

            self.db.execute(drop_table(f"{self.db_fact_table}_load"))

            self.save_status(self.etl_status)
            logger.log_complete()

        except Exception as exception:
            logger.log_failure(exception)


# pylint: enable=too-many-instance-attributes
