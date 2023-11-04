import os
import gzip
import datetime
from typing import Any

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy.engine import CursorResult

from dmap_import.schemas.api_metadata import ApiMetadata
from dmap_import.util_api import (
    download_from_url,
    get_api_results,
    ApiResult,
)
from dmap_import.util_rds import (
    copy_local_to_db,
    DatabaseManager,
)
from dmap_import.util_logging import ProcessLogger

LAST_UPDATED_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"


def schema_compare(local_path: str, destination_table: Any) -> None:
    """
    Compare columns of local file, downloaded from API, to expected columns
    from SQLAlchemy schema.

    throw if columns in local file don't exist in expected schema

    if columns in expected schema, but not in local file, just log

    :param local_path: local file path of API dataset file
    :param destination_table: SQLAlchemy table object to compare
    """
    schema_compare_log = ProcessLogger(
        "schema_compare",
        local_path=local_path,
        destination_table=str(destination_table.__table__),
    )
    schema_compare_log.log_start()

    table = destination_table.__table__
    ignore_columns = (
        "pk_id",
        "dataset_id",
    )
    # table.columns object produces column names with table name pre-pended
    # must remove table pre-pend for comparison
    destination_columns = [
        str(col).replace(f"{table}.", "") for col in table.columns
    ]
    # drop ignore_columns from comparison list, as they won't exist in
    # downloaded file
    destination_columns = [
        col for col in destination_columns if col not in ignore_columns
    ]

    with gzip.open(local_path, "rt") as gzip_file:
        local_columns = gzip_file.readline().strip().lower().split(",")

    # columns in defined schema, but not in local file
    not_in_local = set(destination_columns).difference(set(local_columns))

    # column in local file, but not in defined schema
    not_in_dest = set(local_columns).difference(set(destination_columns))

    schema_compare_log.add_metadata(
        local_col_count=len(local_columns),
        dest_col_count=len(destination_columns),
        not_in_local_count=len(not_in_local),
        not_in_dest_count=len(not_in_dest),
        not_in_local_columns=" | ".join(not_in_local),
        not_in_dest_columns=" | ".join(not_in_dest),
    )

    if not_in_dest:
        exception = IndexError(
            (
                f"Columns: '{' | '.join(not_in_dest)}' "
                f"found in {local_path} but not in {table}"
            )
        )
        schema_compare_log.log_failure(exception=exception)
        raise exception

    schema_compare_log.log_complete()


def insert_update_last_updated(
    url: str, result: ApiResult, db_manager: DatabaseManager
) -> None:
    """
    Set 'last_updated' value for 'url' in ApiMetadata table
    Attempts INSERT, if url already in table will conflict on url UNIQUE
    constraint and fall back to update of 'last_updated' value

    :param url: url key for ApiMetadata Table
    :param result: ApiResult object from Cubic API Endpoint
    :param db_manager: database interaction class
    """
    # create datetime object from `last_updated` ApiResult value
    result_last_updated = datetime.datetime.strptime(
        result["last_updated"], LAST_UPDATED_FORMAT
    )

    insert = postgresql.insert(ApiMetadata).values(
        url=url,
        last_updated=result_last_updated,
    )
    insert = insert.on_conflict_do_update(
        constraint="api_metadata_url_key",
        set_={"last_updated": result_last_updated},
        where=ApiMetadata.url == url,
    )
    db_manager.execute(insert)


def run_api_copy(url: str, destination_table: Any) -> None:
    """
    Perform DELETE and INSERT on DB destination_table using files retrieved from
    CUBIC API Endpoint URL

    If exception is thrown during processing of a result, no more results are
    processed. Results will be re-processed on next event loop in order of
    'last_updated" result field.

    :param url: CUBIC API Endpoint URL for dataset
    :param destination_table: SQLAlchemy target table object
    """
    db_manager = DatabaseManager()

    for result in get_api_results(url, db_manager):
        api_result_log = ProcessLogger(
            "load_api_result",
            api_url=url,
            db_table=str(destination_table.__table__),
            result_url=result["url"],
            dataset_id=result["dataset_id"],
            last_updated=result["last_updated"],
            start_date=result["start_date"],
            end_date=result["end_date"],
        )
        api_result_log.log_start()
        # temporary local file path for API Result file download
        temp_file_path = f"/tmp/{url.split('/')[-1]}.csv.gz"

        download_from_url(result["url"], temp_file_path)

        # verify that schema of file recieved from API download matches
        # expected schema of destination_table
        # will throw if schemas do not match
        schema_compare(temp_file_path, destination_table)

        # to prepare for copy of API file download into DB,
        # delete any records with matching dataset_id in destination_table
        # also delete any records with no dataset_id, as they should be a
        # related to a previous processing error
        delete_dataset_id = sa.delete(destination_table).where(
            sa.or_(
                destination_table.dataset_id == result["dataset_id"],
                destination_table.dataset_id.is_(None),
            )
        )
        delete_result: CursorResult = db_manager.execute(delete_dataset_id)

        copy_local_to_db(temp_file_path, destination_table)
        db_manager.vaccuum_analyze(destination_table)

        # update dataset_id for all records just loaded into DB from
        # API downloaded file
        update_dataset_id = (
            sa.update(destination_table)
            .where(destination_table.dataset_id.is_(None))
            .values(dataset_id=result["dataset_id"])
        )
        update_result: CursorResult = db_manager.execute(update_dataset_id)

        insert_update_last_updated(url, result, db_manager)

        # clean up temp API file download
        os.remove(temp_file_path)

        api_result_log.add_metadata(
            db_records_deleted=delete_result.rowcount,
            db_records_added=update_result.rowcount,
        )

        api_result_log.log_complete()
