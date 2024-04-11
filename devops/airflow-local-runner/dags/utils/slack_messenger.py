import logging
from airflow.hooks.base import BaseHook
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

SLACK_CONN_ID = "airflow-monitor-slack-channel"

failed_tasks_slack_channel = "#airflow_dev_failed_tasks"
succeeded_tasks_slack_channel = "#airflow_dev_successful_tasks"


def message_slack(context, title: str, message: str = "", channel: str = succeeded_tasks_slack_channel):
    """
    Callback task that can be used in DAG to alert of failed task completion
    Calls the Slack Webclient chat_postMessage method internally

    Args:
        context (dict): Context variable passed in from Airflow
        title (str): Message title
        message (str): Message body
        channel (str): Slack channel to send message in

    Returns:
        None
    """
    slack_token = BaseHook.get_connection(SLACK_CONN_ID).password

    if context.get("dag"):
        dag_tags = context.get("dag").tags
    else:
        dag_tags = "None"

    client = WebClient(token=slack_token)

    message_content = """
        {title}
        {message}
        *Task*: {task}
        *Dag*: {dag}
        *Execution Time*: {exec_date}
        *Log Url*: {log_url}
    """.format(
        title=title,
        message=message,
        task=context.get("task_instance").task_id,
        dag=context.get("task_instance").dag_id,
        exec_date=context.get("execution_date"),
        log_url=context.get("task_instance").log_url
    )

    try:
        logging.info('Sending slack message')
        client.chat_postMessage(
            channel=channel,
            text=message_content,
            username="Airflow Monitor",
            link_names=True
        )
    except SlackApiError as e:
        logging.error(e.response["error"])


def generic_slack_message(title: str, message: str = "", channel: str = succeeded_tasks_slack_channel):
    slack_token = BaseHook.get_connection(SLACK_CONN_ID).password

    client = WebClient(token=slack_token)
    message_content = """
        {title}
        {message}
    """.format(
        title=title,
        message=message
    )

    try:
        logging.info('Sending slack message')
        client.chat_postMessage(
            channel=channel,
            text=message_content,
            username="Airflow Monitor",
            link_names=True
        )
    except SlackApiError as e:
        logging.error(e.response["error"])


def task_fail_slack_alert(context):
    message_slack(context, ":red_circle: Task Failed", channel=failed_tasks_slack_channel)


def task_success_slack_alert(context):
    message_slack(context, ":large_green_circle: Task Succeeded", channel=succeeded_tasks_slack_channel)
