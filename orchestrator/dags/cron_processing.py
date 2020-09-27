import pandas as pd
import s3fs

from datetime import timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from common import utils
from orchestrator import callbacks


default_args = {
    "owner": "",
    "start_date": "",
    "on_failure_callback": callbacks.failure,
    "on_success_callback": callbacks.success,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

ENV = utils.load_environment()


# Athena based data processing using presto queries


def submit_query(**context):
    athena = utils.get_aws_client("athena", ENV)

    # dt = context["execution_date"]

    query = utils.get_query_template('query_name')

    return utils.query_athena(athena, query, ENV["ATHENA_STAGING_LOC"])


def process_results(**context):
    job_id = context["ti"].xcom_pull(task_ids="submit_query")

    # and further additional processing for when data from outside of athena is required


with DAG(
    "batch_job", default_args=default_args, schedule_interval="3 * * * *",
) as dag:
    a = PythonOperator(
        task_id="submit_query", python_callable=submit_query, provide_context=True,
    )

    b = PythonOperator(
        task_id="process_results",
        python_callable=process_results,
        provide_context=True,
    )

    a >> b
