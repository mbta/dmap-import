import logging

import sentry_sdk
from sentry_sdk.integrations.logging import LoggingIntegration

from cubic_loader.utils.aws import AWS_ENV

SENTRY_DSN = "https://efc993f5531cda9baac9d78356f7d859@o89189.ingest.us.sentry.io/4512069275942912"


def init_sentry() -> None:
    """
    Initialize the Sentry SDK for the current process.

    This must be called once per process. The qlik ETL jobs are run in "spawn" subprocesses,
    which start a fresh interpreter, so the initialization performed by the main pipeline
    process is not inherited and must be repeated in the child.

    Errors are reported to Sentry by explicit `capture_exception` calls in
    `ProcessLogger.log_failure`, not by the logging integration. `ProcessLogger.log_failure`
    issues two `logging.exception` calls per failure, which the default LoggingIntegration
    would turn into two duplicate Sentry events, so `event_level` is disabled here. Log
    records are still collected as breadcrumbs for context on captured exceptions.
    """
    sentry_sdk.init(
        dsn=SENTRY_DSN,
        send_default_pii=False,
        environment=AWS_ENV,
        integrations=[
            LoggingIntegration(
                level=logging.INFO,
                event_level=None,
            )
        ],
    )
