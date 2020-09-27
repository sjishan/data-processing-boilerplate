import json

import pandas as pd
import requests
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
}

ENV = utils.load_environment()


def submit_query(**context):
    athena = utils.get_aws_client("athena", ENV)

    # dt = context["execution_date"]

    query = utils.get_query_template('anomaly_processing')

    return utils.query_athena(athena, query, ENV["ATHENA_STAGING_LOC"])


def validate_downstream_pipelines(**context):
    # check anomaly affect on downstream pipelines (eg. health checks)
    pass


def anomaly_handling(**context):
    # logic for handling anomaly depending on situation
    pass


def notify(**context):
    # notify team depending on issue
    pass


with DAG(
    "trends",
    default_args=default_args,
    schedule_interval="*/10 * * * *",
) as dag:
    a = PythonOperator(
        task_id="submit_query", python_callable=submit_query, provide_context=True,
    )

    b = PythonOperator(
        task_id="notify",
        python_callable=notify,
        provide_context=True,
    )

    c = PythonOperator(
        task_id="validate_downstream_pipelines",
        python_callable=validate_downstream_pipelines,
        provide_context=True,
    )

    d = PythonOperator(
        task_id="anomaly_handling",
        python_callable=anomaly_handling,
        provide_context=True,
    )


    a >> b >> [c, d]
