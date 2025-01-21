import os
import time
from typing import List, TypedDict
import datetime
import requests

import sqlalchemy as sa

from cubic_loader.utils.postgres import DatabaseManager
from cubic_loader.dmap.schemas.api_metadata import ApiMetadata
from cubic_loader.utils.logger import ProcessLogger


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


def apikey_from_environment(url: str) -> str:
    """
    Get the `apikey` value from the environment
    """
    default = "NOKEY"
    if "datasetpublicusersapi" in url:
        return os.getenv("PUBLIC_KEY", default)
    if "datasetcontrolleduserapi" in url:
        return os.getenv("CONTROLLED_KEY", default)
    return default


def download_from_url(url: str, local_path: str) -> bool:
    """
    Download file from url to local_path.
    will throw for HTTP error (Except Auth Error 403)

    :param url: CUBIC API URL string of file
    :param local_path: local file path to save downloaded file to

    :return: True if download success, False if Authentication Error
    """
    download_log = ProcessLogger(
        "download_from_url",
        url=url,
        local_path=local_path,
    )

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

        except Exception as _:
            if response.status_code == 403:
                # handle Authentication Error
                download_log.log_complete(status_code=response.status_code)
                return False
            if retry_count < max_retries:
                # wait and try again
                time.sleep(15)

    else:
        download_log.add_metadata(
            status_code=response.status_code,
            response=response.text,
        )
        exception = requests.HTTPError(response.text)
        download_log.log_failure(exception)
        raise exception

    file_size_mb = os.path.getsize(local_path) / (1024 * 1024)
    download_log.add_metadata(file_size_mb=f"{file_size_mb:.4f}")
    download_log.log_complete()

    return True


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

    as of Jan 21, 2025 `offset` parameters appears to be functional, will be utilized to capture
    more than 100 results from API
    """
    api_results_log = ProcessLogger(
        "get_api_results",
        url=url,
    )

    last_update_query = sa.select(ApiMetadata.last_updated).where(
        ApiMetadata.url == url,
    )
    db_result = db_manager.select_as_list(last_update_query)

    # add params to GET request
    # last_updated if last_update_dt available from ApiMetadata table
    # apikey based on contents of url endpoint string
    headers = {"apikey": apikey_from_environment(url)}
    params = {"apikey": apikey_from_environment(url), "limit": "100"}
    if db_result:
        last_updated_dt: datetime.datetime = db_result[0]["last_updated"]
        params["last_updated"] = (last_updated_dt.date() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        api_results_log.add_metadata(last_updated_dt=last_updated_dt.isoformat())

    # execute GET request from CUBIC API Endpoint
    # will log and throw if 200 status_code not recieved
    # or if "success" attribute of ApiResponse is not True
    max_retries = 3
    api_results = []
    for limit_offset in range(10):
        params["offset"] = str(int(params["limit"]) * limit_offset)
        for retry_count in range(max_retries + 1):
            api_results_log.add_metadata(retry_count=retry_count)
            try:
                response = requests.get(url, headers=headers, params=params, timeout=15)
                response.raise_for_status()
                response.close()

                json_response: ApiResponse = response.json()

                if not json_response["success"]:
                    raise AttributeError("No Results object recieved.")
                break

            except Exception as _:
                if retry_count < max_retries:
                    # wait and try again
                    time.sleep(15)
        else:
            api_results_log.add_metadata(
                status_code=response.status_code,
                response=response.text,
            )
            exception = requests.HTTPError(response.text)
            api_results_log.log_failure(exception)
            raise exception

        if len(json_response["results"]) == 0:
            break
        api_results += json_response["results"]

    # API Results appear to be sorted by `last_updated` by default, but this
    # sorting is required for proper updated of `last_updated` column
    # in ApiMetadata RDS table
    api_results.sort(key=lambda result: result["last_updated"])

    api_results_log.add_metadata(original_result_count=len(api_results))

    # filter by last_updated_dt, to avoid re-processing
    # this is a result of `last_updated` API parameter only
    # accepting a date and not a datetime
    if db_result:
        api_results = [
            result
            for result in api_results
            if result["last_updated"] > last_updated_dt.strftime("%Y-%m-%dT%H:%M:%S.%f")
        ]

    api_results_log.add_metadata(filter_result_count=len(api_results))
    api_results_log.log_complete()

    return api_results
