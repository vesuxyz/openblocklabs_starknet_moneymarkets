#!/bin/sh
set -e

IMAGE_NAME=$1
ENVIRONMENT=$2
DOMAIN_OWNER=522495932155

cd ../../../; # go to root of project

# login to ECR
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin ${DOMAIN_OWNER}.dkr.ecr.us-east-1.amazonaws.com;

# Build the docker image
docker build --platform=linux/amd64 -t starknet-openblocklabs/${IMAGE_NAME}-${ENVIRONMENT} -f devops/ecs/Dockerfile .;

# Tag the image
docker tag starknet-openblocklabs/${IMAGE_NAME}-${ENVIRONMENT}:latest ${DOMAIN_OWNER}.dkr.ecr.us-east-1.amazonaws.com/starknet-openblocklabs/${IMAGE_NAME}-${ENVIRONMENT}:latest;

# Push the image to ECR
docker push ${DOMAIN_OWNER}.dkr.ecr.us-east-1.amazonaws.com/starknet-openblocklabs/${IMAGE_NAME}-${ENVIRONMENT}:latest;
