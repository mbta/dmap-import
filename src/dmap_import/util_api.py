import os
import time
from typing import Optional, List, TypedDict
import datetime
import requests

import sqlalchemy as sa

from dmap_import.util_rds import DatabaseManager
from dmap_import.schemas.api_metadata import ApiMetadata
from dmap_import.util_logging import ProcessLogger


class ApiResult(TypedDict):
    """CUBIC API Result Model"""

    id: str
    dataset_id: str
    url: str
    start_date: str
    end_date: str
    last_updated: str


class ApiResponse(TypedDict):
    """CUBIC API Response Model"""

    success: bool
    results: List[ApiResult]


def download_from_url(url: str, local_path: str) -> Optional[str]:
    """
    Download file from url to local_path.
    will throw for HTTP error

    :param url: CUBIC API URL string of file
    :param local_path: local file path to save downloaded file to

    :return local file path of successfully downloaded file
    """
    download_log = ProcessLogger(
        "download_from_url",
        url=url,
        local_path=local_path,
    )
    download_log.log_start()

    max_retries = 3
    for retry_count in range(max_retries + 1):
        download_log.add_metadata(retry_count=retry_count)
        try:
            response = requests.get(url, stream=True, timeout=15)
            response.raise_for_status()

            # download file
            with open(local_path, "wb") as local_file:
                for file_chunk in response.iter_content(chunk_size=None):
                    # chunk_size=None will write chunks in whatever size they are received
                    local_file.write(file_chunk)

            response.close()
            break

        except Exception as exception:
            if retry_count == max_retries:
                download_log.add_metadata(
                    staus_code=response.status_code,
                    response=response.text,
                )
                download_log.log_failure(exception)
                raise exception
            # wait and try again
            time.sleep(15)

    file_size_mb = os.path.getsize(local_path) / (1024 * 1024)
    download_log.add_metadata(file_size_mb=f"{file_size_mb:.4f}")
    download_log.log_complete()

    return local_path


def get_api_results(url: str, db_manager: DatabaseManager) -> List[ApiResult]:
    """
    Execute GET request against CUBIC API URL using last_updated param to
    filter results based on last_updated timestamp from ApiMetadata DB table

    will throw if GET request result for 'success' is not True

    :param url: full URL of CUBIC API Endpoint

    :return list of filtered and sorted CUBIC API Results

    All CUBIC API Endpoints appear to offer the following paramters:
        - apikey: str:  API Authentication
        - start_date: str: filter by date range <YYYY-MM-DD>
        - end_date: str: filter by date range <YYYY-MM-DD>
        - limit: int: reduce feteched result count
        - offset: int: skip n records
        - last_updated: str: filter by last updated <YYYY-MM-DD>

    API Endpoints appear to produce a maximum of 100 records,
    any `limit` value > 100 is ignored.

    `offset` parameters appears to be non-functional, utilizing `offset` had no
    impact on returned results during API testing

    """
    api_results_log = ProcessLogger(
        "get_api_results",
        url=url,
    )
    api_results_log.log_start()

    last_update_query = sa.select(ApiMetadata.last_updated).where(
        ApiMetadata.url == url,
    )
    db_result = db_manager.select_as_list(last_update_query)

    # add params to GET request
    # last_updated if last_update_dt available from ApiMetadata table
    # apikey based on contents of url endpoint string
    params = {}
    if db_result:
        last_updated_dt: datetime.datetime = db_result[0]["last_updated"]
        params["last_updated"] = (
            last_updated_dt.date() - datetime.timedelta(days=1)
        ).strftime("%Y-%m-%d")
        api_results_log.add_metadata(
            last_updated_dt=last_updated_dt.isoformat()
        )

    if "datasetpublicusersapi" in url:
        params["apikey"] = os.getenv("PUBLIC_KEY", "")
    elif "datasetcontrolleduserapi" in url:
        params["apikey"] = os.getenv("CONTROLLED_KEY", "")

    # execute GET request from CUBIC API Endpoint
    # will log and throw if 200 status_code not recieved
    # or if "success" attribute of ApiResponse is not True
    max_retries = 3
    for retry_count in range(max_retries + 1):
        api_results_log.add_metadata(retry_count=retry_count)
        try:
            response = requests.get(url, params=params, timeout=15)
            response.raise_for_status()
            response.close()

            json_response: ApiResponse = response.json()

            if not json_response["success"]:
                raise AttributeError("No Results object recieved.")
            break

        except Exception as exception:
            if retry_count == max_retries:
                api_results_log.add_metadata(
                    staus_code=response.status_code,
                    response=response.text,
                )
                api_results_log.log_failure(exception)
                raise exception
            # wait and try again
            time.sleep(15)

    # API Results appear to be sorted by `last_updated` by default, but this
    # sorting is required for proper updated of `last_updated` column
    # in ApiMetadata RDS table
    api_results = sorted(
        json_response["results"], key=lambda result: result["last_updated"]
    )

    api_results_log.add_metadata(original_result_count=len(api_results))

    # filter by last_updated_dt, to avoid re-processing
    # this is a result of `last_updated` API parameter only
    # accepting a date and not a datetime
    if db_result:
        api_results = [
            result
            for result in api_results
            if result["last_updated"]
            > last_updated_dt.strftime("%Y-%m-%dT%H:%M:%S.%f")
        ]

    api_results_log.add_metadata(filter_result_count=len(api_results))
    api_results_log.log_complete()

    return api_results
