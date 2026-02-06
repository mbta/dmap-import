import os
from multiprocessing import get_context

from cubic_loader.utils.aws import check_for_parallel_tasks
from cubic_loader.utils.logger import ProcessLogger
from cubic_loader.utils.postgres import alembic_upgrade_to_head
from cubic_loader.utils.postgres import DatabaseManager
from cubic_loader.utils.runtime import validate_environment
from cubic_loader.utils.remote_locations import ODS_SCHEMA


from cubic_loader.dmap.api_copy_job import run_api_copy
from cubic_loader.dmap.api_job_list import produce_job_list

from cubic_loader.qlik.ods_tables import CUBIC_ODS_TABLES
from cubic_loader.qlik.ods_qlik import CubicODSQlik


def start_dmap() -> None:
    """
    Upgrade DB and run DMAP api jobs.

    Jobs are run without any re-try logic

    For any API URL, if a result loading throws, no more results are processed
    for that URL. During the next processing loop, the result that threw should
    be re-processed based on filtering and ordering of `last_updated` result
    field.
    """
    os.environ["SERVICE_NAME"] = "dmap_loader"

    alembic_upgrade_to_head()

    for job in produce_job_list():
        job_log = ProcessLogger(
            "run_api_copy",
            url=job["url"],
            destination_table=str(job["table"].__tablename__),
        )
        try:
            run_api_copy(url=job["url"], destination_table=job["table"])
            job_log.log_complete()
        except Exception as exception:
            job_log.log_failure(exception)


def start_qlik_load() -> None:
    """
    Load ODS QLIK tables from S3 Buckets into RDS
    """
    os.environ["SERVICE_NAME"] = "qlik_loader"

    for cubic_table in CUBIC_ODS_TABLES:
        log = ProcessLogger("CubicODSQlik", cubic_table=cubic_table)

        try:
            qlik_table = CubicODSQlik(cubic_table)
            proc = get_context("spawn").Process(target=qlik_table.run_etl)
            proc.start()
            proc.join()
            if proc.exitcode == 0:
                log.log_complete()
            else:
                raise SystemError(f"CubicODSQlik Job died with exitcode={proc.exitcode}")
        except Exception as exception:
            log.log_failure(exception)

    db = DatabaseManager()
    db.refresh_mat_views(ODS_SCHEMA)


def main() -> None:
    """
    initialize and validate environment, then start running the application
    """
    os.environ["SERVICE_NAME"] = "validate_env"

    validate_environment(
        required_variables=[
            "CONTROLLED_KEY",
            "PUBLIC_KEY",
            "DMAP_BASE_URL",
            "ARCHIVE_BUCKET",
            "ERROR_BUCKET",
        ],
        private_variables=[
            "CONTROLLED_KEY",
            "PUBLIC_KEY",
        ],
        aws_variables=["ECS_CLUSTER", "ECS_TASK_GROUP"],
        validate_db=True,
    )

    check_for_parallel_tasks()

    start_dmap()

    #start_qlik_load()


if __name__ == "__main__":
    main()
