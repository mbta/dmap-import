from typing import List
from typing import Optional
from datetime import date
from datetime import datetime
from dateutil.relativedelta import relativedelta

from cubic_loader.utils.remote_locations import ODS_SCHEMA
from cubic_loader.qlik.utils import DFMSchemaFields


# pylint: disable=too-many-branches
def qlik_type_to_pg(qlik_type: str, scale: int) -> str:
    """
    convert qlik datatype from DFM file to postgres type

    :param qlik_type: QLIK data type from DFM file
    :param scale: max number of digits to right of decimal

    :return: postgres type
    """
    return_type = "VARCHAR"

    if qlik_type == "CHANGE_OPER":
        return_type = "CHAR(1)"
    elif qlik_type == "CHANGE_SEQ":
        return_type = "NUMERIC(35,0)"
    elif "INT1" in qlik_type:
        return_type = "SMALLINT"
    elif "INT2" in qlik_type:
        return_type = "SMALLINT"
    elif "INT3" in qlik_type:
        return_type = "INTEGER"
    elif "INT4" in qlik_type:
        return_type = "BIGINT"
    elif qlik_type == "REAL4":
        return_type = "REAL"
    elif qlik_type == "REAL8":
        return_type = "DOUBLE PRECISION"
    elif "NUMERIC" in qlik_type and scale == 0:
        return_type = "BIGINT"
    elif "NUMERIC" in qlik_type:
        return_type = "DOUBLE PRECISION"
    elif qlik_type == "BOOLEAN":
        return_type = qlik_type
    elif qlik_type == "DATE":
        return_type = qlik_type
    elif qlik_type == "TIME":
        return_type = "TIME WITHOUT TIME ZONE"
    elif qlik_type == "DATETIME":
        return_type = "TIMESTAMP WITHOUT TIME ZONE"

    return return_type


# pylint: enable=too-many-branches


def create_tables_from_schema(schema: List[DFMSchemaFields], table_name: str) -> str:
    """
    produce CREATE table string for FACT and HISTORY tables from dfm snapshot path

    also CREATE INDEX for history table that will be used for inserting into FACT table.
    """
    ops: List[str] = []
    dfm_columns: List[str] = []
    dfm_keys: List[str] = []
    for column in schema:
        dfm_columns.append(f"{column['name']} {qlik_type_to_pg(column['type'], column['scale'])}")
        if column["primaryKeyPos"] > 0:
            dfm_keys.append(column["name"])

    assert len(dfm_keys) > 0

    # Create FACT Table
    fact_columns = dfm_columns + [f"PRIMARY KEY ({','.join(dfm_keys)})"]
    ops.append(f"CREATE TABLE IF NOT EXISTS {ODS_SCHEMA}.{table_name} ({",".join(fact_columns)});")

    # Create HISTORY Table
    # partitioned by header__timestamp
    header_fields = (
        ("header__timestamp", "DATETIME"),
        ("header__change_oper", "CHANGE_OPER"),
        ("header__change_seq", "CHANGE_SEQ"),
    )
    header_cols: List[str] = [f"{col[0]} {qlik_type_to_pg(col[1], 0)}" for col in header_fields]
    history_keys = ["header__timestamp"] + dfm_keys + ["header__change_oper", "header__change_seq"]
    history_columns = header_cols + dfm_columns + [f"PRIMARY KEY ({','.join(history_keys)})"]
    ops.append(
        (
            f"CREATE TABLE IF NOT EXISTS {ODS_SCHEMA}.{table_name}_history ({",".join(history_columns)}) "
            " PARTITION BY RANGE (header__timestamp);"
        )
    )

    # Create load Table for loading snapshot data
    load_columns = header_cols + dfm_columns
    ops.append(f"CREATE TABLE IF NOT EXISTS {ODS_SCHEMA}.{table_name}_load ({",".join(load_columns)});")

    # Create INDEX on HISTORY Table that will be used for creating FACT table
    index_columns = dfm_keys + ["header__change_oper", "header__change_seq DESC"]
    ops.append(
        f"CREATE INDEX IF NOT EXISTS {table_name}_to_fact_idx on {ODS_SCHEMA}.{table_name}_history "
        f"({','.join(index_columns)});"
    )

    return " ".join(ops)


def create_history_table_partitions(table: str, start_ts: Optional[str] = None) -> str:
    """
    produce CREATE partition table strings for history table

    if `start_ts` IS NOT provided, produce CREATE statements for next 3 months from today
    if `start_ts` IS provided, produce CREATE statements for month of `start_ts` to 3 months from today

    :param table: name of HISTORY table to create statements for
    :param start_ts: date timestamp as string starting with YYYYMMDD

    :return: all partition CREATE statments as single string
    """
    part_date = date.today()
    if start_ts is not None:
        ts_dt = datetime.strptime(start_ts[:8], "%Y%m%d")
        part_date = ts_dt.date()

    part_date = part_date.replace(day=1)
    part_end = date.today().replace(day=1) + relativedelta(months=3)

    part_tables: List[str] = []
    while part_date < part_end:
        part_table = f"{table}_y{part_date.year}m{part_date.month}"
        create_part = (
            f"CREATE TABLE IF NOT EXISTS {ODS_SCHEMA}.{part_table} PARTITION OF {ODS_SCHEMA}.{table} "
            f"FOR VALUES FROM ('{part_date}') TO ('{part_date + relativedelta(months=1)}');"
        )
        part_tables.append(create_part)
        part_date += relativedelta(months=1)

    return " ".join(part_tables)


def drop_table(table_name: str) -> str:
    """
    DROP table from RDS
    """
    return f"DROP TABLE IF EXISTS {ODS_SCHEMA}.{table_name};"


def add_columns_to_table(new_columns: List[DFMSchemaFields], fact_table: str) -> str:
    """
    produce ALTER table string to add columns to FACT, HISTORY and LOAD tables

    :param new_columns: List of dictionaries containing new column name and QLIK type
    :param fact_table: fact table for new columns

    :return: string to create all new columns
    """
    tables = (
        fact_table,
        f"{fact_table}_history",
    )
    alter_strings: List[str] = []
    for column in new_columns:
        for table in tables:
            alter_strings.append(
                (
                    f"ALTER TABLE {ODS_SCHEMA}.{table} ADD "
                    f"{column['name']} {qlik_type_to_pg(column['type'], column['scale'])};"
                )
            )

    return " ".join(alter_strings)
