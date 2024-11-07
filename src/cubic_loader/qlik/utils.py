import os
import re
import gzip
import json
from typing import NamedTuple
from typing import TypedDict
from typing import List

import polars as pl

from cubic_loader.utils.aws import running_in_aws
from cubic_loader.utils.aws import s3_get_object
from cubic_loader.utils.aws import s3_get_client
from cubic_loader.utils.logger import ProcessLogger


class DFMDetails(NamedTuple):
    """Fields for DFM Snapshot files"""

    path: str
    ts: str


class DFMSchemaFields(TypedDict):
    """Fields of ODS QLK DFM dataInfo columns"""

    ordinal: int
    name: str
    type: str
    length: int
    precision: int
    scale: int
    primaryKeyPos: int


class TableStatus(NamedTuple):
    """Fields to track progress of S3 to RDS ETL Operations"""

    current_snapshot_ts: str
    db_fact_table: str
    last_cdc_ts: str
    last_schema: List[DFMSchemaFields]


RE_SNAPSHOT_TS = re.compile(r"(\d{8}T\d{6}Z)")

RE_CDC_TS = re.compile(r"(\d{8}-\d{9})")

DFM_COLUMN_SCHEMA = pl.Schema(
    {
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

MERGED_FNAME = "cdc_merged.csv"


def re_get_first(string: str, pattern: re.Pattern) -> str:
    """
    pull first regex match from string

    this should not fail and will raise if no match is found

    :param string: string to perform search on
    :param pattern: compiled regex pattern

    :return: first found pattern match
    """
    match = pattern.search(string)
    if match is None:
        raise LookupError(f"regex pattern({pattern.pattern}) not found in {string}")

    return match.group(0)


def s3_list_cdc_gz_objects(
    bucket: str,
    prefix: str,
    min_ts: str = "",
) -> List[str]:
    """
    provide list of s3 objects based on bucket and prefix

    :param bucket: the name of the bucket with objects
    :param prefix: prefix for objs to return
    :param min_ts: filter for cdc.csv.gz objects

    :return: List[s3://bucket/key, ...]
    """
    logger = ProcessLogger(
        "s3_list_cdc_gz_objects",
        bucket=bucket,
        prefix=prefix,
        min_ts=min_ts,
    )
    try:
        s3_client = s3_get_client()
        paginator = s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

        filepaths = []
        for page in pages:
            if page["KeyCount"] == 0:
                continue
            for obj in page["Contents"]:
                if obj["Size"] == 0 or not str(obj["Key"]).lower().endswith(".csv.gz"):
                    continue
                try:
                    obj_ts = re_get_first(obj["Key"], RE_CDC_TS)
                    if obj_ts > min_ts:
                        filepaths.append(os.path.join("s3://", bucket, obj["Key"]))
                except Exception as _:
                    continue

        logger.log_complete(objects_found=len(filepaths))
        return filepaths

    except Exception as exception:
        logger.log_failure(exception)
        return []


def threading_cpu_count() -> int:
    """
    return an integer for the number of work threads to utilize
    """
    os_cpu_count = os.cpu_count()
    if os_cpu_count is None:
        return 4

    if running_in_aws():
        return os_cpu_count * 2

    return os_cpu_count


def dfm_schema_to_json(dfm_path: str) -> List[DFMSchemaFields]:
    """
    extract table schema from S3 .dfm path as json

    :param dfm_path: S3 path to .dfm file as s3://bucket/object_path
    """
    dfm_json = json.load(s3_get_object(dfm_path))
    return dfm_json["dataInfo"]["columns"]


def dfm_schema_to_df(dfm_path: str) -> pl.DataFrame:
    """
    extract table schema from .dfm and convert to Polars Dataframe

    :param dfm_path: S3 path to .dfm file as s3://bucket/object_path
    """
    json_schema = dfm_schema_to_json(dfm_path)
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


def merge_cdc_csv_gz_files(tmp_dir: str) -> str:
    """
    Merge all cdc csv.gz files in tmp_dir, will create MERGED_FNAME file in tmp_dir

    :param tmp_dir: folder containing all csv.gz files to be merged

    :return: greatest cdc ts from files in tmp_dir
    """
    merge_file = os.path.join(tmp_dir, MERGED_FNAME)
    cdc_paths = [os.path.join(tmp_dir, f) for f in os.listdir(tmp_dir)]

    max_ts = ""
    with open(merge_file, "wb") as fout:
        for cdc_path in cdc_paths:
            max_ts = max(max_ts, re_get_first(cdc_path, RE_CDC_TS))
            with gzip.open(cdc_path, "rb") as f:
                if fout.tell() > 0:
                    next(f)
                fout.write(f.read())

    return max_ts


def qlik_type_to_polars(field: DFMSchemaFields) -> pl.DataType:
    """
    convert QLIK datatypes to polars types

    :param field: QLIK .dfm Schema Field to be converted to Polars type

    :return: qlik type converted to polars
    """
    qlik_type = field["type"]

    if field["name"] == "header__change_seq":
        qlik_type = "CHANGE_SEQ"

    exact_type_matches = {
        "CHANGE_SEQ": pl.Decimal(35, 0),
        "REAL4": pl.Float32(),
        "REAL8": pl.Float64(),
        "BOOLEAN": pl.Boolean(),
        "DATE": pl.Date(),
        "TIME": pl.Time(),
        "DATETIME": pl.Datetime(),
    }
    # check for exacty type matching
    return_type = exact_type_matches.get(qlik_type, None)
    if return_type is not None:
        return return_type

    # continue with alternate type matching
    if "INT" in qlik_type:
        return_type = pl.Int64()
    elif "NUMERIC" in qlik_type and field["scale"] == 0:
        return_type = pl.Int64()
    elif "NUMERIC" in qlik_type:
        return_type = pl.Float64()
    else:
        return_type = pl.String()

    return return_type


def polars_schema_from_dfm(dfm_path: str) -> pl.Schema:
    """
    create polars schema based on column names and types from dfm_path

    :param dfm_path: S3 path to .dfm file as s3://bucket/object_path

    :return: polars schema of dfm_path
    """
    return pl.Schema({col["name"].lower(): qlik_type_to_polars(col) for col in dfm_schema_to_json(dfm_path)})


def dataframe_from_merged_csv(csv_path: str, dfm_path: str) -> pl.DataFrame:
    """
    load csv_path into dataframe with correct types
    types will be inferred from dfm_path (one .csv.gz file from csv_path)

    dataframe drops header__change_oper="B" because they are redundant

    :param csv_path: local path for merged csv file
    :param dfm_path: S3 path to .dfm file as s3://bucket/object_path

    :return: polars dataframe of csv_path file
    """
    schema = polars_schema_from_dfm(dfm_path)
    df = pl.read_csv(csv_path, schema=schema).filter(pl.col("header__change_oper").ne("B"))
    return df
