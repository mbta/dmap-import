import os
import boto3

from dmap_import.util_logging import ProcessLogger


def running_in_aws() -> bool:
    """
    return True if running on aws, else False
    """
    return bool(os.getenv("AWS_DEFAULT_REGION"))


def check_for_parallel_tasks() -> None:
    """
    Check that that this task is not already running on ECS
    """
    if not running_in_aws():
        return

    process_logger = ProcessLogger("check_for_tasks")
    process_logger.log_start()

    client = boto3.client("ecs")
    dmap_ecs_cluster = "dmap-import"
    environment_name = os.environ["ENVIRONMENT"]

    # get all of the tasks running on the cluster
    task_arns = client.list_tasks(cluster=dmap_ecs_cluster)["taskArns"]

    # if tasks are running on the cluster, get their descriptions and check to
    # see if any match the group for this task.
    #
    # if the group matches, raise an exception that will terminate the process
    if task_arns:
        running_tasks = client.describe_tasks(
            cluster=dmap_ecs_cluster, tasks=task_arns
        )["tasks"]

        for task in running_tasks:
            if f"family:dmap-import-{environment_name}" == task["group"]:
                exception = Exception("Multiple Tasks Running")
                process_logger.log_failure(exception)
                raise exception

    process_logger.log_complete()
