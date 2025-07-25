from typing import List
from typing import Optional
from typing import Tuple
from datetime import date
from datetime import datetime
from dateutil.relativedelta import relativedelta

from cubic_loader.qlik.utils import DFMSchemaFields


def qlik_type_to_pg(qlik_type: str, scale: int, precision: int) -> str:
    """
    convert qlik datatype from DFM file to postgres type

    :param qlik_type: QLIK data type from DFM file
    :param scale: max number of digits to right of decimal

    :return: postgres type as str
    """
    exact_type_matches = {
        "CHANGE_OPER": "CHAR(1)",
        "CHANGE_SEQ": "NUMERIC(35,0)",
        "REAL4": "REAL",
        "REAL8": "DOUBLE PRECISION",
        "BOOLEAN": "BOOLEAN",
        "DATE": "DATE",
        "TIME": "TIME WITHOUT TIME ZONE",
        "DATETIME": "TIMESTAMP WITHOUT TIME ZONE",
    }
    # check for exacty type matching
    return_type = exact_type_matches.get(qlik_type, None)
    if return_type is not None:
        return return_type

    # continue with alternate type matching
    if "INT1" in qlik_type:
        return_type = "SMALLINT"
    elif "INT2" in qlik_type:
        return_type = "SMALLINT"
    elif "INT3" in qlik_type:
        return_type = "INTEGER"
    elif "INT4" in qlik_type:
        return_type = "BIGINT"
    elif "NUMERIC" in qlik_type and scale == 0 and precision < 19:
        return_type = "BIGINT"
    elif "NUMERIC" in qlik_type:
        return_type = f"NUMERIC({precision},{scale})"
    else:
        return_type = "VARCHAR"

    return return_type


def create_tables_from_schema(schema: List[DFMSchemaFields], schema_and_table: str) -> str:
    """
    produce CREATE table string for FACT and HISTORY tables from dfm snapshot path

    also CREATE INDEX for history table that will be used for inserting into FACT table.

    :param schema: Schema List from DFM file
    :schema_and_table: Schema and Table as 'schema.table'

    :return: SQL Statements to CREATE FACT and HISTORY tables and any associated indexes
    """
    ops: List[str] = []
    dfm_columns: List[str] = []
    dfm_keys: List[str] = []
    for column in schema:
        dfm_columns.append(f"{column['name']} {qlik_type_to_pg(column['type'], column['scale'], column['precision'])}")
        if column["primaryKeyPos"] > 0:
            dfm_keys.append(column["name"])

    assert len(dfm_keys) > 0

    # Create FACT Table
    # FACT table is created without a primary key
    # Tables coming from CUBIC ODS system are Oracle based which allows NULL values in Primary Key columns
    # Postgres does not allow NULL in Primary Key columns, instead a standard INDEX on the Key columns is created
    ops.append(f"CREATE TABLE IF NOT EXISTS {schema_and_table} ({",".join(dfm_columns)});")

    # FACT Table Index on Primary Key columns
    ops.append(
        f"CREATE INDEX IF NOT EXISTS {schema_and_table.replace('.','_')}_fact_pk_idx on {schema_and_table} "
        f"({','.join(dfm_keys)});"
    )

    # Create HISTORY Table
    # partitioned by header__timestamp
    header_fields = (
        ("header__timestamp", "DATETIME"),
        ("header__change_oper", "CHANGE_OPER"),
        ("header__change_seq", "CHANGE_SEQ"),
    )
    header_cols: List[str] = [f"{col[0]} {qlik_type_to_pg(col[1], 0, 0)}" for col in header_fields]
    history_columns = header_cols + dfm_columns
    ops.append(
        (
            f"CREATE TABLE IF NOT EXISTS {schema_and_table}_history ({",".join(history_columns)}) "
            " PARTITION BY RANGE (header__timestamp);"
        )
    )

    # Create load Table for loading snapshot data
    load_columns = header_cols + dfm_columns
    ops.append(f"CREATE TABLE IF NOT EXISTS {schema_and_table}_load ({",".join(load_columns)});")

    # Create INDEX on HISTORY Table that will be used for creating FACT table
    index_columns = dfm_keys + ["header__change_oper", "header__change_seq DESC"]
    ops.append(
        f"CREATE INDEX IF NOT EXISTS {schema_and_table.replace('.','_')}_to_fact_idx on {schema_and_table}_history "
        f"({','.join(index_columns)});"
    )

    return " ".join(ops)


def create_history_table_partitions(schema_and_table: str, start_ts: Optional[str] = None) -> str:
    """
    produce CREATE partition table strings for history table

    if `start_ts` IS NOT provided, produce CREATE statements for next 3 months from today
    if `start_ts` IS provided, produce CREATE statements for month of `start_ts` to 3 months from today

    :param schema_and_table: name and schema of HISTORY table as 'schema.table'
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
        part_table = f"{schema_and_table}_y{part_date.year}m{part_date.month}"
        create_part = (
            f"CREATE TABLE IF NOT EXISTS {part_table} PARTITION OF {schema_and_table} "
            f"FOR VALUES FROM ('{part_date}') TO ('{part_date + relativedelta(months=1)}');"
        )
        part_tables.append(create_part)
        part_date += relativedelta(months=1)

    return " ".join(part_tables)


def drop_table(schema_and_table: str) -> str:
    """
    DROP table from RDS

    :param schema_and_table: name and schema of table to DROP as 'schema.table'

    :return: DROP TABLE command
    """
    return f"DROP TABLE IF EXISTS {schema_and_table} CASCADE;"


def add_columns_to_table(new_columns: List[DFMSchemaFields], schema_and_table: str) -> str:
    """
    produce ALTER table string to add columns to FACT and HISTORY tables

    :param new_columns: List of dictionaries containing new column name and QLIK type
    :param schema_and_table: name and schema of table as 'schema.table'

    :return: string to create all new columns
    """
    tables = (
        schema_and_table,
        f"{schema_and_table}_history",
        f"{schema_and_table}_load",
    )
    alter_strings: List[str] = []
    for column in new_columns:
        for table in tables:
            alter_strings.append(
                f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column['name']} {qlik_type_to_pg(column['type'], column['scale'], column['precision'])};"
            )

    return " ".join(alter_strings)


def convert_cols_to_string(new_columns: list[str], schema_and_table: str) -> str:
    """
    produce ALTER table statement to convert columns to VARCHAR type

    :param new_columns: List of column names to convert to VARCHAR
    :param schema_and_table: name and schema of table as 'schema.table'

    :return: string to create all new columns
    """
    tables = (
        schema_and_table,
        f"{schema_and_table}_history",
        f"{schema_and_table}_load",
    )
    alter_strings: List[str] = []
    for column in new_columns:
        for table in tables:
            alter_strings.append(f"ALTER TABLE {table} ALTER {column} TYPE VARCHAR;")

    return " ".join(alter_strings)


def bulk_delete_from_temp(schema_and_table: str, op_and_keys: List[Tuple[str, str]]) -> str:
    """
    create query to DELETE records from table based on key columns
    """
    tmp_table = f"{schema_and_table}_load"
    where_clause = " AND ".join([f"{schema_and_table}.{t} {op} {tmp_table}.{t}" for op, t in op_and_keys])
    delete_query = f"DELETE FROM {schema_and_table} USING {tmp_table} WHERE {where_clause};"

    return delete_query


def bulk_update_from_temp(schema_and_table: str, update_column: str, op_and_keys: List[Tuple[str, str]]) -> str:
    """
    create query to UPDATE records from table based on key columns
    """
    tmp_table = f"{schema_and_table}_load"
    where_clause = " AND ".join([f"{schema_and_table}.{t} {op} {tmp_table}.{t}" for op, t in op_and_keys])
    update_query = (
        f"UPDATE {schema_and_table} SET {update_column}={tmp_table}.{update_column} "
        f"FROM {tmp_table} WHERE {where_clause};"
    )

    return update_query


def bulk_insert_from_temp(insert_table_and_schema: str, temp_table_and_schema: str, columns: List[str]) -> str:
    """
    create query to INSERT records from temp table to fact table
    """
    columns_str = ",".join(columns)
    insert_query = (
        f"INSERT INTO {insert_table_and_schema} ({columns_str}) "
        f"SELECT {columns_str} FROM {temp_table_and_schema} "
        f"ON CONFLICT DO NOTHING;"
    )
    return insert_query
