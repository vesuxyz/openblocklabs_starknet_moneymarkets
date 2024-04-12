# Airflow for Starknet Lending

## How to deploy this project in development environment?

To deploy this project in dev environment is necessary to execute these steps:

* Create the ECS stack using Terraform on dev workspace
* Build the docker image
* Start Airflow locally

#### Initialize Terraform

```
make initialize_terraform
```
#### Create ECS Stack on dev Workspace

```
make create_ecs_stack ENV=dev
```

This command will create a entire ECS stack on AWS: ECS Cluster, IAM role, ECS Task definition, and AWS ECR repository.

#### Build the docker image and push to ECR

```
make login_ecr_docker_registry  build_and_push_docker_image ENV=dev
```

This step will build the docker image locally and push to AWS ECR to be used later when the Airflow DAG execute the pipeline, that will create a container on AWS ECS based on this image from ECR.

#### Run Airflow locally to execute the DAG

```
make setup_airflow_locally
```

Now you can access the Airflow ULR using these credentials:
* Airflow URL: http://localhost:8080
* user: **admin**
* password: **admin**

The DAG file is in the folder devops/airflow-local-runner/dags and after running the Airflow, this dag will be loaded automatically, you will see the DAG in the Airflow UI.

Now the DAG it's available and you can run it.

## Run locally with Docker

```
 docker build --platform=linux/amd64 -t starknet-openblocklabs/starknet-lending-interface-dev -f devops/ecs/Dockerfile .
```
```
docker run -e ENV=dev -e AWS_ACCESS_KEY_ID={INSERT KEY} -e AWS_SECRET_ACCESS_KEY={INSERT KEY} -e AWS_DEFAULT_REGION=us-east-1 -it starknet-openblocklabs/starknet-lending-interface-dev /bin/bash
```
## How to deploy this project in production environment?

To deploy this project in production environment is necessary to execute these steps:

* Create the ECS stack using Terraform on dev workspace
* Build the docker image
* Upload DAG to S3

#### Create ECS Stack on dev Workspace and build the docker image

The steps is the same when we are deploying on dev, you will need just to change the ENV var value:

```
# Create ECS stack:
make create_ecs_stack ENV=prod

# Build docker image and push to ECR:
make login_ecr_docker_registry build_and_push_docker_image ENV=prod
``` 
#### Upload DAG to Airflow MWAA (Prod ENV)

```
make upload_dag_to_mwaa
```