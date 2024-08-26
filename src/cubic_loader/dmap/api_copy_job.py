import os
import gzip
import datetime
from typing import Any
from tempfile import TemporaryDirectory

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from cubic_loader.dmap.schemas import ApiMetadata
from cubic_loader.dmap.dmap_api import download_from_url
from cubic_loader.dmap.dmap_api import get_api_results
from cubic_loader.dmap.dmap_api import ApiResult
from cubic_loader.utils.postgres import copy_local_to_db
from cubic_loader.utils.postgres import DatabaseManager
from cubic_loader.utils.logger import ProcessLogger

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

    table = destination_table.__table__
    ignore_columns = (
        "pk_id",
        "dataset_id",
    )
    # table.columns object produces column names with table name pre-pended
    # must remove table pre-pend for comparison
    destination_columns = [str(col).replace(f"{table}.", "") for col in table.columns]
    # drop ignore_columns from comparison list, as they won't exist in
    # downloaded file
    destination_columns = [col for col in destination_columns if col not in ignore_columns]

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
        exception = IndexError((f"Columns: '{' | '.join(not_in_dest)}' " f"found in {local_path} but not in {table}"))
        schema_compare_log.log_failure(exception=exception)
        raise exception

    schema_compare_log.log_complete()


def insert_update_last_updated(url: str, result: ApiResult, db_manager: DatabaseManager) -> None:
    """
    Set 'last_updated' value for 'url' in ApiMetadata table
    Attempts INSERT, if url already in table will conflict on url UNIQUE
    constraint and fall back to update of 'last_updated' value

    :param url: url key for ApiMetadata Table
    :param result: ApiResult object from Cubic API Endpoint
    :param db_manager: database interaction class
    """
    # create datetime object from `last_updated` ApiResult value
    result_last_updated = datetime.datetime.strptime(result["last_updated"], LAST_UPDATED_FORMAT)

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


def drop_dataset_id_null(table: Any, db_manager: DatabaseManager) -> int:
    """
    DELETE any records from table where dataset_id is NULL

    :param table: DELETE target table object
    :param db_manager: database interaction class

    :return count of records removed from table
    """
    delete_dataset_id = sa.delete(table).where(table.dataset_id.is_(None))
    delete_result = db_manager.execute(delete_dataset_id)

    return delete_result.rowcount


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

        try:
            # temporary local file path for API Result file download
            temp_file = result["url"].split("?", 1)[0].split("/")[-1]
            with TemporaryDirectory(ignore_cleanup_errors=True) as tempdir:
                temp_file_path = os.path.join(tempdir, temp_file)

                download_from_url(result["url"], temp_file_path)

                # compare schema of API file download to destination_table
                # throw if additional columns found in file download
                # otherwise will log differences
                schema_compare(temp_file_path, destination_table)

                # delete any records with dataset_id=NULL, as they should be a
                # related to a previous processing error
                drop_dataset_id_null(destination_table, db_manager)

                copy_local_to_db(temp_file_path, destination_table)

            db_manager.vaccuum_analyze(destination_table)

            delete_dataset_id = sa.delete(destination_table).where(destination_table.dataset_id == result["dataset_id"])
            delete_result = db_manager.execute(delete_dataset_id)

            # update dataset_id for all records just loaded into DB from
            # API downloaded file
            update_dataset_id = (
                sa.update(destination_table)
                .where(destination_table.dataset_id.is_(None))
                .values(dataset_id=result["dataset_id"])
            )
            update_result = db_manager.execute(update_dataset_id)

            insert_update_last_updated(url, result, db_manager)

            api_result_log.add_metadata(
                db_records_deleted=delete_result.rowcount,
                db_records_added=update_result.rowcount,
            )

            api_result_log.log_complete()

        except Exception as exception:
            api_result_log.log_failure(exception)
            raise exception

        finally:
            drop_dataset_id_null(destination_table, db_manager)
