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
    "retry_delay": timedelta(minutes=1),
}

ENV = utils.load_environment()

# Partition management

def submit_query(**context):
    athena = utils.get_aws_client("athena", ENV)

    dt = context["execution_date"] + timedelta(hours=1)

    tables = athena.list_table_metadata(
        CatalogName="AwsDataCatalog", DatabaseName="analytics"
    )["TableMetadataList"]

    for t in tables:
        if sorted([p["Name"] for p in t["PartitionKeys"]]) != ["collected", "hour"]:
            continue

        athena.start_query_execution(
            QueryString=f"""
            ALTER TABLE analytics.{t['Name']} ADD IF NOT EXISTS
                PARTITION (collected='{dt.strftime('%Y-%m-%d')}', hour={dt.hour})
            """,
            ResultConfiguration={"OutputLocation": ENV["ATHENA_STAGING_LOC"]},
            WorkGroup="",
        )


with DAG(
    "", default_args=default_args, schedule_interval="0 * * * *"
) as dag:
    PythonOperator(
        task_id="submit_query", python_callable=submit_query, provide_context=True,
    )
