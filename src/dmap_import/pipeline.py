import os
from typing import List

from dmap_import.util_rds import alembic_upgrade_to_head
from dmap_import.api_job_list import produce_job_list
from dmap_import.api_copy_job import run_api_copy
from dmap_import.util_logging import ProcessLogger


def validate_environment(
    required_variables: List[str],
    private_variables: List[str] = [],
    validate_db: bool = False,
) -> None:
    """
    ensure that the environment has all the variables its required to have
    before starting triggering main, making certain errors easier to debug.
    """
    process_logger = ProcessLogger("validate_env")
    process_logger.log_start()

    # every pipeline needs a service name for logging
    required_variables.append("SERVICE_NAME")

    # add required database variables
    if validate_db:
        required_variables += [
            "DB_HOST",
            "DB_NAME",
            "DB_PORT",
            "DB_USER",
        ]

    # check for missing variables. add found variables to our logs.
    missing_required = []
    for key in required_variables:
        value = os.environ.get(key, None)
        if value is None:
            missing_required.append(key)
        if key not in private_variables:
            process_logger.add_metadata(**{key: value})
        else:
            process_logger.add_metadata(**{key: "**********"})

    # if db password is missing, db region is required to generate a token to
    # use as the password to the cloud database
    if validate_db:
        if os.environ.get("DB_PASSWORD", None) is None:
            value = os.environ.get("DB_REGION", None)
            if value is None:
                missing_required.append("DB_REGION")
            process_logger.add_metadata(DB_REGION=value)

    # if required variables are missing, log a failure and throw.
    if missing_required:
        exception = EnvironmentError(
            f"Missing required environment variables {missing_required}"
        )
        process_logger.log_failure(exception)
        raise exception

    process_logger.log_complete()


def start() -> None:
    """
    Upgrade DB and run api jobs.

    Jobs are run without any re-try logic

    For any API URL, if a result loading throws, no more results are processed
    for that URL. During the next processing loop, the result that threw should
    be re-processed based on filtering and ordering of `last_updated` result
    field.
    """
    alembic_upgrade_to_head()

    for job in produce_job_list():
        job_log = ProcessLogger(
            "run_api_copy",
            url=job["url"],
            destination_table=str(job["table"].__tablename__),
        )
        job_log.log_start()

        try:
            run_api_copy(url=job["url"], destination_table=job["table"])
        except Exception as exception:
            job_log.log_failure(exception)

        job_log.log_complete()


def main() -> None:
    """
    initialize and validate environment, then start running the application
    """
    os.environ["SERVICE_NAME"] = "dmap_loader"

    validate_environment(
        required_variables=[
            "CONTROLLED_KEY",
            "PUBLIC_KEY",
            "DMAP_BASE_URL",
        ],
        private_variables=[
            "CONTROLLED_KEY",
            "PUBLIC_KEY",
        ],
        validate_db=True,
    )

    start()


if __name__ == "__main__":
    main()
