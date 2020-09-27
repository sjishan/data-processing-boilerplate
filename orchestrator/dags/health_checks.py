from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from common import utils
from orchestrator import callbacks


default_args = {
    "owner": "",
    "start_date": "",
    "on_failure_callback": callbacks.failure,
    "on_success_callback": callbacks.success,
}

ENV = utils.load_environment()


def ping():
    sqs = utils.get_aws_client("sqs", ENV)
    queue = sqs.get_queue_url(QueueName="queue")["QueueUrl"]

    sqs.send_message(QueueUrl=queue, MessageBody="ping")

    # also send requests to various health/status endpoints in other applications


with DAG(
    "health_check", default_args=default_args, schedule_interval="*/10 * * * *",
) as dag:
    PythonOperator(task_id="ping", python_callable=ping)
