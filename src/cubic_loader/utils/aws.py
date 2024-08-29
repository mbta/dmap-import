import os
from typing import List
from typing import Tuple
from typing import Optional
from typing import Dict
from functools import lru_cache

import boto3
from botocore.exceptions import ClientError
from botocore.client import BaseClient
from botocore.config import Config
from botocore.response import StreamingBody

from cubic_loader.utils.logger import ProcessLogger

S3_POOL_COUNT = 50


def running_in_aws() -> bool:
    """
    True if running on AWS Infrastructure, else False
    """
    return bool(os.getenv("AWS_DEFAULT_REGION"))


@lru_cache
def s3_get_client() -> BaseClient:
    """Thin function needed for stubbing tests"""
    aws_profile = os.getenv("AWS_PROFILE", None)
    client_config = Config(max_pool_connections=S3_POOL_COUNT)

    return boto3.Session(profile_name=aws_profile).client("s3", config=client_config)


def s3_split_object_path(obj: str) -> Tuple[str, str]:
    """
    split s3 object as "s3://bucket/object_key" into Tuple[bucket, key]

    :param obj: s3 object as "s3://bucket/object_key" or "bucket/object_key"

    :return: Tuple[bucket, key]
    """
    obj = obj.replace("s3://", "")
    bucket, key = obj.split("/", 1)

    return (bucket, key)


def s3_list_objects(
    bucket: str,
    prefix: str,
    max_objects: int = 5_000_000,
    in_filter: Optional[str] = None,
) -> List[str]:
    """
    provide list of s3 objects based on bucket and prefix
    will not include folder paths

    :param bucket: the name of the bucket with objects
    :param prefix: prefix for objs to return
    :param max_objects: maximum number of objects to return
    :param in_filter: will filter for objects containing string

    :return: List[s3://bucket/key, ...]
    """
    logger = ProcessLogger(
        "s3_list_objects",
        bucket=bucket,
        prefix=prefix,
        max_objects=max_objects,
    )
    try:
        s3_client = s3_get_client()
        paginator = s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

        filepaths = []
        for page in pages:
            if page["KeyCount"] == 0:
                continue
            for obj in page["Contents"]:
                if obj["Size"] == 0:
                    continue
                if in_filter is None or in_filter in obj["Key"]:
                    filepaths.append(os.path.join("s3://", bucket, obj["Key"]))

            if len(filepaths) >= max_objects:
                break

        logger.log_complete(objects_found=len(filepaths))
        return filepaths

    except Exception as exception:
        logger.log_failure(exception)
        return []


def s3_object_exists(obj: str) -> bool:
    """
    check if s3 object exists

    will raise on any error other than "NoSuchKey"

    :param obj - expected as 's3://bucket/object' or 'bucket/object'

    :return: True if object exists, otherwise false
    """
    try:
        s3_client = s3_get_client()
        bucket, obj = s3_split_object_path(obj)
        s3_client.head_object(Bucket=bucket, Key=obj)
        return True

    except ClientError as exception:
        if exception.response["Error"]["Code"] == "404":
            return False
        raise exception


def s3_upload_file(file_name: str, object_path: str, extra_args: Optional[Dict] = None) -> bool:
    """
    Upload a local file to an S3 Bucket

    :param file_name: local file path to upload
    :param object_path: S3 object path to upload to (including bucket)
    :param extra_agrs: additional upload ags available per: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/customizations/s3.html#boto3.s3.transfer.S3Transfer.ALLOWED_UPLOAD_ARGS

    :return: True if file was uploaded, else False
    """
    upload_log = ProcessLogger(
        "s3_upload_file",
        file_name=file_name,
        object_path=object_path,
        auto_start=False,
    )
    if isinstance(extra_args, dict):
        upload_log.add_metadata(**extra_args)
    upload_log.log_start()

    try:
        if not os.path.exists(file_name):
            raise FileNotFoundError(f"{file_name} not found locally")

        bucket, object_name = s3_split_object_path(object_path)

        s3_client = s3_get_client()

        s3_client.upload_file(file_name, bucket, object_name, ExtraArgs=extra_args)

        upload_log.log_complete()

        return True

    except Exception as exception:
        upload_log.log_failure(exception=exception)
        return False


def s3_download_object(obj: str, file_name: str) -> bool:
    """
    Download an S3 object to a local file
    will overwrite local file, if exists

    :param obj: S3 object path to download from (including bucket)
    :param file_name: local file path to save object to

    :return: True if file was downloaded, else False
    """
    logger = ProcessLogger("s3_download_object", obj=obj, file_name=file_name)
    try:
        if os.path.exists(file_name):
            os.remove(file_name)

        bucket, key = s3_split_object_path(obj)
        s3_client = s3_get_client()
        s3_client.download_file(bucket, key, file_name)
        logger.log_complete()
        return True

    except Exception as exception:
        logger.log_failure(exception=exception)
        return False


def s3_get_object(obj: str) -> StreamingBody:
    """
    Get an S3 object as StreamingBody

    :param obj: S3 object path to get (including bucket)
    """
    logger = ProcessLogger("s3_get_object", obj=obj)
    try:
        bucket, key = s3_split_object_path(obj)
        s3_client = s3_get_client()
        obj = s3_client.get_object(Bucket=bucket, Key=key)["Body"]
        logger.log_complete()
        return obj

    except Exception as exception:
        logger.log_failure(exception=exception)
        raise exception


def s3_delete_object(del_obj: str) -> bool:
    """
    delete s3 object

    :param del_obj - expected as 's3://bucket/object' or 'bucket/object'

    :return: True if file success, else False
    """
    logger = ProcessLogger("delete_s3_object", del_obj=del_obj)
    try:
        s3_client = s3_get_client()
        bucket, key = s3_split_object_path(del_obj)
        s3_client.delete_object(
            Bucket=bucket,
            Key=key,
        )
        logger.log_complete()
        return True

    except Exception as error:
        logger.log_failure(error)
        return False


def s3_rename_object(source_obj: str, dest_obj: str) -> bool:
    """
    rename source_obj to dest_obj as copy and delete operation

    :param source_obj - expected as 's3://bucket/object' or 'bucket/object'
    :param dest_obj - expected as 's3://bucket/object' or 'bucket/object'

    :return: True if file success, else False
    """
    logger = ProcessLogger("s3_rename_object", source_obj=source_obj, dest_obj=dest_obj)
    try:
        s3_client = s3_get_client()
        # trim off leading s3://
        source_obj = source_obj.replace("s3://", "")
        # split into bucket and object name
        to_bucket, to_key = s3_split_object_path(dest_obj)
        # copy object
        s3_client.copy_object(
            Bucket=to_bucket,
            CopySource=source_obj,
            Key=to_key,
        )
        if not s3_delete_object(source_obj):
            raise FileExistsError(f"failed to delete {source_obj}")

        logger.log_complete()
        return True

    except Exception as error:
        logger.log_failure(error)
        return False


def check_for_parallel_tasks() -> None:
    """
    Check that that this task is not already running on ECS
    """
    if not running_in_aws():
        return

    logger = ProcessLogger("check_for_parallel_tasks")

    ecs_client = boto3.client("ecs")
    ecs_cluster = os.environ["ECS_CLUSTER"]
    ecs_task_group = os.environ["ECS_TASK_GROUP"]

    # get all of the tasks running on the cluster
    task_arns = ecs_client.list_tasks(cluster=ecs_cluster)["taskArns"]

    # if tasks are running on the cluster, get their descriptions and check to
    # count matches the ecs task group.
    match_count = 0
    if task_arns:
        for task in ecs_client.describe_tasks(cluster=ecs_cluster, tasks=task_arns)["tasks"]:
            if ecs_task_group == task["group"]:
                match_count += 1

    # if the group matches, raise an exception that will terminate the process
    if match_count > 1:
        exception = SystemError(f"Multiple ECS tasks running in {ecs_cluster} cluster")
        logger.log_failure(exception)
        raise exception

    logger.log_complete()
