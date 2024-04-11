import os
os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'

from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.operators.ecs import (
    EcsRunTaskOperator,
)
from utils.slack_messenger import task_fail_slack_alert, task_success_slack_alert

ARGS = {
    "owner": "airflow",
    "description": "Ingestion of Starknet Lending protocol data",
    "retry": 3,
    "retry_delay": timedelta(minutes=1),
    "start_date": datetime(2024, 4, 2),
    "depend_on_past": False,
    "on_failure_callback": task_fail_slack_alert,
    "on_success_callback": task_success_slack_alert,
}

CHAIN = "starknet"
TYPE = "lending" # dex, perps, moneymarkets
PROTOCOLS = ["haskstack", "nimbora", "nostra", "zklend"] # list of participating protocols/folders

ENV = Variable.get("environment")
PROJECT_NAME = f"{CHAIN}-{TYPE}-interface"
CLUSTER_NAME = f"{PROJECT_NAME}-{ENV}-fargate-cluster"
TASK_DEFINITION = f"{PROJECT_NAME}-{ENV}-task-definition"
NETWORK_CONFIGURATION = eval(Variable.get("ecs_network_configurations"))
LOG_GROUP = f"/ecs/log-group-{PROJECT_NAME}-{ENV}"

with DAG(
    dag_id=f"dag_{CHAIN}_{TYPE}_interface",
    default_args=ARGS,
    schedule_interval="0 0 * * *", # daily, "0 * * * *" = hourly
    tags=[CHAIN],
    catchup=False,
    max_active_runs=1,
) as dag:

    start_task = DummyOperator(task_id="start", dag=dag)
    end_task = DummyOperator(task_id="end", dag=dag)
      
    for protocol in PROTOCOLS:
        
        command = f"python main.py --protocol {protocol}".split()
        
        run_ecs_task = EcsRunTaskOperator(
            task_id=f"{CHAIN}_{TYPE}_interface_{protocol}",
            cluster=CLUSTER_NAME,
            task_definition=TASK_DEFINITION,
            launch_type="FARGATE",
            network_configuration=NETWORK_CONFIGURATION,
            overrides={
                "containerOverrides": [
                    {
                        "name": "main-container",
                        "command": command,
                         "environment": [{"name": "ENV", "value": ENV}],
                    },
                ],
            },
            awslogs_group=LOG_GROUP,
            awslogs_region="us-east-1",
            awslogs_stream_prefix=f"{PROJECT_NAME}/main-container",
            aws_conn_id="aws_default",
            dag=dag,
        )
        
        start_task >> run_ecs_task >> end_task