import os
import re
from typing import NamedTuple
from typing import TypedDict
from typing import List

from cubic_loader.utils.aws import running_in_aws


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
