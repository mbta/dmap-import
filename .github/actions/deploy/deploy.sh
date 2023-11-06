#!/bin/sh
set -e -u

# this was based off of the deploy script in the mbta/actions/deploy-ecs
# action.
#
# Register a new ECS Task Definition using the new Docker Image
# Update the ECS Service with this new Task Definition
#
# Required Arguments
# ECS_CLUSTER: ecs cluster for service being updated
# ECS_SERVICE: ecs service containing task being updated
# ECS_TASK_DEF: task definition to update revision for
# DOCKER_TAG: tag for docker image to use in new task definition

# set default region so we don't have to specify --region everywhere
export AWS_DEFAULT_REGION="us-east-1"

# get the contents of the template task definition from ECS 
echo "Retrieving ${ECS_TASK_DEF}-template task definition..."
taskdefinition="$(aws ecs describe-task-definition --task-definition "${ECS_TASK_DEF}-template")"

# use retrieved task definition as basis for new revision, but replace image
echo "Updating container image to ${DOCKER_TAG}."
newcontainers="$(echo "${taskdefinition}" | \
  jq '.taskDefinition.containerDefinitions' | \
  jq --arg tag "${DOCKER_TAG}" 'map(.image="\($tag)")')"

# check to make sure the secrets are included in the new container definition
if (echo "${newcontainers}" | jq '.[0] | .secrets' | grep '^null$'); then
  echo "Error: The container definition is missing its 'secrets' block. Deploy cannot proceed."
  exit 1
fi

# this task uses fargate, so publish the new task definition
echo "Publishing new Task Definition."
aws ecs register-task-definition \
--family "${ECS_TASK_DEF}" \
--task-role-arn "$(echo "${taskdefinition}" | jq -r '.taskDefinition.taskRoleArn')" \
--execution-role-arn "$(echo "${taskdefinition}" | jq -r '.taskDefinition.executionRoleArn')" \
--network-mode "$(echo "${taskdefinition}" | jq -r '.taskDefinition.networkMode')" \
--container-definitions "${newcontainers}" \
--volumes "$(echo "${taskdefinition}" | jq '.taskDefinition.volumes')" \
--placement-constraints "$(echo "${taskdefinition}" | jq '.taskDefinition.placementConstraints')" \
--requires-compatibilities "$(echo "${taskdefinition}" | jq -r '.taskDefinition.requiresCompatibilities')" \
--cpu "$(echo "${taskdefinition}" | jq -r '.taskDefinition.cpu')" \
--memory "$(echo "${taskdefinition}" | jq -r '.taskDefinition.memory')"

# get the new revision of this task and redeploy the cluster with it
newrevision="$(aws ecs describe-task-definition --task-definition "${ECS_TASK_DEF}" | \
  jq -r '.taskDefinition.revision')"

echo "Updating Service ${ECS_SERVICE} to use Task Definition ${newrevision}..."
aws ecs update-service --cluster="${ECS_CLUSTER}" --service="${ECS_SERVICE}" --task-definition "${ECS_TASK_DEF}:${newrevision}"
