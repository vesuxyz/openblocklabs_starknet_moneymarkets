SHELL := /bin/bash
ENV ?= dev
DOCKER_IMAGE_NAME = starknet-openblocklabs/starknet-lending-interface-${ENV}
AWS_ACCOUNT_ID = 522495932155
AWS_REGION = us-east-1
ECR_URI = ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${DOCKER_IMAGE_NAME}

build_image:
	docker build --platform linux/arm64 -t ${DOCKER_IMAGE_NAME} -f devops/ecs/Dockerfile .

setup_airflow_locally:
	cd ../../devops/airflow-local-runner && docker compose up -d && ./config/setup_aws_connection.sh

login_ecr_docker_registry:
	aws ecr get-login-password --region ${AWS_REGION} | \
	docker login --username AWS --password-stdin ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com

build_and_push_docker_image:
	docker build --platform=linux/amd64 -t ${DOCKER_IMAGE_NAME} -f devops/ecs/Dockerfile .
	printf "Publishing ${ECR_REPOSITORY_NAME} docker image to ${ENV} ECR repository"
	docker tag ${DOCKER_IMAGE_NAME}:latest ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${DOCKER_IMAGE_NAME}:latest
	docker push ${ECR_URI}:latest

initialize_terraform:
	cd devops/ecs/infrastructure && \
	terraform init

create_ecs_stack:
	cd devops/ecs/infrastructure && \
	terraform workspace select -or-create ${ENV}  && \
	terraform apply

upload_dag_to_mwaa:
	aws s3 cp devops/airflow-local-runner/dags/dag_starknet_lending_interface.py s3://openblocklabs-airflow-mwaa-prod/dags/starknet/