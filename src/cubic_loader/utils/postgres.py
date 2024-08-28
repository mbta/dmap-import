import os
import time
import gzip
import platform
import subprocess
import urllib.parse as urlparse
from typing import Any
from typing import Optional
from typing import Dict
from typing import List
from typing import Tuple
from typing import Union

import boto3
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.sql.expression import TextClause
from sqlalchemy.sql.schema import Table
from sqlalchemy.engine import CursorResult
from alembic.config import Config
from alembic import command

from cubic_loader.utils.aws import running_in_aws
from cubic_loader.utils.logger import ProcessLogger


def running_in_docker() -> bool:
    """
    return True if running inside of a docker container, else False
    """
    path = "/proc/self/cgroup"
    return (
        os.path.exists("/.dockerenv")
        or os.path.isfile(path)
        and any("docker" in line for line in open(path, encoding="UTF-8"))
    )


def get_db_host() -> str:
    """
    get current db_host string
    """
    db_host = os.environ.get("DB_HOST", "")

    # on mac, when running in docker locally db is accessed by "0.0.0.0" ip
    if db_host == "cubic_local_rds" and "macos" in platform.platform().lower():
        db_host = "0.0.0.0"

    # when running application locally in CLI for configuration
    # and debugging, db is accessed by localhost ip
    if not running_in_docker() and not running_in_aws():
        db_host = "127.0.0.1"

    return db_host


def get_db_password() -> str:
    """
    function to provide rds password

    used to refresh auth token, if required
    """
    db_password = os.environ.get("DB_PASSWORD", None)
    db_host = os.environ.get("DB_HOST")
    db_port = os.environ.get("DB_PORT")
    db_user = os.environ.get("DB_USER")
    db_region = os.environ.get("DB_REGION", None)

    if db_password is None:
        # generate aws db auth token if in rds
        client = boto3.client("rds")
        return client.generate_db_auth_token(
            DBHostname=db_host,
            Port=db_port,
            DBUsername=db_user,
            Region=db_region,
        )

    return db_password


def create_db_connection_string() -> str:
    """
    produce database connection string from environment
    """
    process_log = ProcessLogger("create_db_connection_string")

    db_host = get_db_host()
    db_name = os.environ.get("DB_NAME")
    db_password = os.environ.get("DB_PASSWORD", None)
    db_port = os.environ.get("DB_PORT")
    db_user = os.environ.get("DB_USER")
    db_ssl_options = ""

    assert db_host is not None
    assert db_name is not None
    assert db_port is not None
    assert db_user is not None

    process_log.add_metadata(host=db_host, database_name=db_name, user=db_user, port=db_port)

    # use presence of DB_PASSWORD env var as indicator of connection type.
    #
    # if not available, assume cloud database where ssl is used and
    # passwords are generated on the fly
    #
    # if is available, assume local dev usage
    if db_password is None:
        db_password = get_db_password()
        db_password = urlparse.quote_plus(db_password)

        assert db_password is not None
        assert db_password != ""

        # set the ssl cert path to the file that should be added to the
        # lambda function at deploy time
        db_ssl_cert = os.path.abspath(os.path.join("/", "usr", "local", "share", "amazon-certs.pem"))

        assert os.path.isfile(db_ssl_cert)

        # update the ssl options string to add to the database url
        db_ssl_options = f"?sslmode=verify-full&sslrootcert={db_ssl_cert}"

    process_log.log_complete()

    return f"{db_user}:{db_password}@{db_host}:{db_port}/{db_name}{db_ssl_options}"


def postgres_event_update_db_password(
    _: sa.engine.interfaces.Dialect,
    __: Any,
    ___: Tuple[Any, ...],
    cparams: Dict[str, Any],
) -> None:
    """
    update database passord on every new connection attempt
    this will refresh db auth token passwords
    """
    process_logger = ProcessLogger("password_refresh")
    cparams["password"] = get_db_password()
    process_logger.log_complete()


def get_local_engine(
    echo: bool = False,
) -> sa.future.engine.Engine:
    """
    Get an SQL Alchemy engine that connects to a locally Postgres RDS stood up
    via docker using env variables
    """
    process_logger = ProcessLogger("create_postgres_engine")
    try:
        database_url = f"postgresql+psycopg2://{create_db_connection_string()}"

        engine = sa.create_engine(
            database_url,
            echo=echo,
            future=True,
            pool_pre_ping=True,
            pool_use_lifo=True,
            pool_size=3,
            max_overflow=2,
            connect_args={
                "keepalives": 1,
                "keepalives_idle": 60,
                "keepalives_interval": 60,
            },
        )

        process_logger.log_complete()
        return engine
    except Exception as exception:
        process_logger.log_failure(exception)
        raise exception


AnyQuery = Union[
    sa.sql.selectable.Select,
    sa.sql.dml.Update,
    sa.sql.dml.Delete,
    sa.sql.dml.Insert,
    TextClause,
]
PreAnyQuery = Union[str, AnyQuery]
SelectQuery = Union[TextClause, sa.sql.selectable.Select]
PreSelectQuery = Union[str, TextClause, sa.sql.selectable.Select]


class DatabaseManager:
    """
    manager class for rds application operations
    """

    def __init__(self, verbose: bool = False):
        """
        initialize db manager object, creates engine and sessionmaker
        """
        self.engine = get_local_engine(echo=verbose)

        sa.event.listen(
            self.engine,
            "do_connect",
            postgres_event_update_db_password,
        )

        self.session = sessionmaker(bind=self.engine)

    def _get_schema_table(self, table: Any) -> Union[Table, str]:
        if isinstance(table, (Table, str)):
            return table
        if isinstance(
            table,
            (
                sa.orm.decl_api.DeclarativeMeta,
                sa.orm.decl_api.DeclarativeAttributeIntercept,
            ),
        ):
            # mypy error: "DeclarativeMeta" has no attribute "__table__"
            return table.__table__  # type: ignore

        raise TypeError(f"can not pull schema table from {type(table)} type")

    def _to_text_any(self, query: PreAnyQuery) -> AnyQuery:
        """Auto convert str to TextClase"""
        if isinstance(query, str):
            return sa.text(query)
        return query

    def _to_text_select(self, query: PreSelectQuery) -> SelectQuery:
        """Auto convert str to TextClase"""
        if isinstance(query, str):
            return sa.text(query)
        return query

    def get_session(self) -> sessionmaker:
        """
        get db session for performing actions
        """
        return self.session

    def execute(self, statement: PreAnyQuery) -> CursorResult:
        """
                execute SQL Statement with no return data

        :param statement: SQL Statement to execute
        """
        statement = self._to_text_any(statement)
        with self.session() as cursor:
            result: CursorResult = cursor.execute(statement)  # type: ignore
            cursor.commit()
        return result

    def select(self, query: PreSelectQuery) -> Dict[str, Any]:
        """
        execute SQL SELECT Query and return first result as dictionary

        :param query: SQL SELECT Query to execute
        :return: first result as dictionary or None if no result
        """
        query = self._to_text_select(query)

        with self.session() as cursor:
            result = cursor.execute(query)
            first = result.first()
        if first is None:
            return {"none": None}
        return first._asdict()

    def select_as_list(self, query: PreSelectQuery) -> Union[List[Any], List[Dict[str, Any]]]:
        """
        execute SQL SELECT Query and produce list of dictionaries

        :param query: SQL SELECT Query to execute
        :return: Results of query as list of dictionaries
        """
        query = self._to_text_select(query)
        with self.session() as cursor:
            return [row._asdict() for row in cursor.execute(query)]

    def vaccuum_analyze(self, table: Any) -> None:
        """RUN VACUUM (ANALYZE) on table"""
        table_as = self._get_schema_table(table)

        with self.session() as cursor:
            cursor.execute(sa.text("END TRANSACTION;"))
            cursor.execute(sa.text(f"VACUUM (ANALYZE) {table_as};"))
            cursor.commit()

    def truncate_table(
        self,
        table: Any,
        restart_identity: bool = False,
        cascade: bool = False,
    ) -> None:
        """
        truncate db table

        restart_identity: Automatically restart sequences owned by columns of the truncated table(s).
        cascade: Automatically truncate all tables that have foreign-key references to any of the named tables, or to any tables added to the group due to CASCADE.
        """
        table_as = self._get_schema_table(table)

        truncate_query = f"TRUNCATE {table_as}"

        if restart_identity:
            truncate_query = f"{truncate_query} RESTART IDENTITY"

        if cascade:
            truncate_query = f"{truncate_query} CASCADE"

        self.execute(f"{truncate_query};")

        # Execute VACUUM to avoid non-deterministic behavior during testing
        self.vaccuum_analyze(table_as)

    def schema_exists(self, schema: str, create: bool = True) -> bool:
        """
        check if schema exists

        :param schema: schema to check
        :param create: bool for creating schema if does not exist (default=True)
        """
        schema = schema.lower()
        exists = self.select(sa.text(f"SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = '{schema}')"))
        if "exists" not in exists:
            raise LookupError("Very bad schema check")
        schema_check: bool = exists.get("exists", True)
        if create is False or schema_check is True:
            return schema_check
        self.execute(sa.text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
        return True

    def table_exists(self, schema: str, table: str) -> bool:
        """
        check if table exists in schema

        :param schema: schema of table
        :param create: table to check for
        """
        schema = schema.lower()
        table = table.lower()
        query = sa.text(
            f"SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = '{schema}' AND tablename  = '{table}')"
        )
        return self.select(query)["exists"]


def copy_local_to_db(local_path: str, destination_table: DeclarativeBase) -> None:
    """
    Load local file into DB using psql COPY command
    will throw if psql command does not exit with code 0

    :param local_path: path to local file that will be loaded
    :param destination_table: SQLAlchemy table object for COPY destination
    """
    copy_log = ProcessLogger(
        "psql_copy",
        destination_table=str(destination_table.__table__),
    )

    with gzip.open(local_path, "rt") as gzip_file:
        local_columns = gzip_file.readline().strip().lower().split(",")

    copy_command = (
        f"\\COPY {destination_table.__table__} "
        f"({','.join(local_columns)}) "
        "FROM PROGRAM "
        f"'gzip -dc {local_path}' "
        "WITH CSV HEADER"
    )

    psql = [
        "psql",
        f"postgresql://{create_db_connection_string()}",
        "-c",
        f"{copy_command}",
    ]

    process_result = subprocess.run(psql, check=True)

    copy_log.add_metadata(exit_code=process_result.returncode)
    copy_log.log_complete()


def header_from_csv_gz(obj_path: str) -> str:
    """
    extract header columns from local or remote csv.gz file
    """
    if obj_path.lower().startswith("s3://"):
        s3_cmd = "aws s3 cp " f"{obj_path} - " "| gzip -dc " "| head -n 1"
        ps = subprocess.run(
            s3_cmd,
            shell=True,
            capture_output=True,
            text=True,
            check=True,
        )
        header_str = ps.stdout
    else:
        with gzip.open(obj_path, "rt") as gzip_file:
            header_str = gzip_file.readline()

    return header_str.strip().lower().replace('"', "")


def remote_csv_gz_copy(obj_path: str, destination_table: str, column_str: Optional[str] = None) -> None:
    """
    load local (or s3 remote) csv.gz file into DB using psql COPY command

    correct headers can to be provided, otherwise they will be pulled from the first row of obj_path

    will throw if psql command does not exit with code 0

    :param local_path: path to local file that will be loaded
    :param destination_table: table name for COPY destination
    :param column_str: columns in the order they occur in obj_path as comma-seperated string
    """
    copy_log = ProcessLogger(
        "remote_csv_gz_copy",
        obj_path=obj_path,
        destination_table=destination_table,
    )

    if column_str is None:
        column_str = header_from_csv_gz(obj_path)

    copy_from = f"FROM {obj_path} "
    if obj_path.lower().startswith("s3://") and obj_path.lower().endswith(".gz"):
        copy_from = f"FROM PROGRAM 'aws s3 cp {obj_path} - | gzip -dc' "
    elif obj_path.lower().endswith(".gz"):
        copy_from = f"FROM PROGRAM 'gzip -dc {obj_path}' "

    copy_command = f"\\COPY {destination_table} ({column_str}) {copy_from} WITH CSV HEADER"

    psql = [
        "psql",
        f"postgresql://{create_db_connection_string()}",
        "-c",
        f"{copy_command}",
    ]

    run_psql_subprocess(psql, copy_log, max_retries=0)


def run_psql_subprocess(psql_cmd: List[str], logger: ProcessLogger, max_retries: int = 2) -> None:
    """
    run psql command with retry logic
    """
    logger.add_metadata(max_retries=max_retries)

    for retry_attempts in range(max_retries + 1):
        try:
            logger.add_metadata(retry_attempts=retry_attempts)
            process_result = subprocess.run(psql_cmd, check=True)
            break
        except Exception as exception:
            if retry_attempts == max_retries:
                logger.log_failure(exception=exception)
                raise exception
            time.sleep(5)

    logger.add_metadata(exit_code=process_result.returncode)
    logger.log_complete()


def get_alembic_config() -> Config:
    """
    return alembic configuration for  project
    """
    config_log = ProcessLogger("alembic_config")

    here = os.path.dirname(os.path.abspath(__file__))
    alembic_cfg_file = os.path.join(here, "..", "..", "..", "alembic.ini")
    alembic_cfg_file = os.path.abspath(alembic_cfg_file)

    config_log.add_metadata(cfg_file=alembic_cfg_file)

    config_log.log_complete()

    return Config(alembic_cfg_file)


def alembic_upgrade_to_head() -> None:
    """
    upgrade rds to head revision
    """
    upgrade_log = ProcessLogger("alembic_upgrade")
    # load alembic configuation
    alembic_cfg = get_alembic_config()

    command.upgrade(alembic_cfg, revision="head")

    upgrade_log.log_complete()


def alembic_downgrade_to_base() -> None:
    """
    downgrade rds to base revision
    """
    # load alembic configuation for db_name
    downgrade_log = ProcessLogger("alembic_downgrade")
    alembic_cfg = get_alembic_config()

    command.downgrade(alembic_cfg, revision="base")

    downgrade_log.log_complete()
