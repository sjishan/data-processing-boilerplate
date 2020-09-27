import json

from airflow.operators.python_operator import PythonOperator

from common import utils

ENV = utils.load_environment()


def ding_sqs(message):
    sqs = utils.get_aws_client("sqs", ENV)

    queue = sqs.get_queue_url(QueueName=ENV["MONITOR_AIRFLOW_QUEUE"])["QueueUrl"]
    sqs.send_message(QueueUrl=queue, MessageBody=json.dumps(message))


def success(context):
    return PythonOperator(
        task_id="callback_success",
        python_callable=ding_sqs,
        op_kwargs={
            "message": {
                "dag": context["task_instance"].dag_id,
                "task": context["task_instance_key_str"],
                "execution_date": context["execution_date"].strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
                "status": "success",
            }
        },
    ).execute(context)


def failure(context):
    return PythonOperator(
        task_id="callback_success",
        python_callable=ding_sqs,
        op_kwargs={
            "message": {
                "dag": context["task_instance"].dag_id,
                "task": context["task_instance_key_str"],
                "execution_date": context["execution_date"].strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
                "status": "failure",
            }
        },
    ).execute(context)
