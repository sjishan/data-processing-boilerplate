from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from common import schemas, utils
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

MAX_AGE = 7  # days
MAX_SIZE = "47gb"  # aiming for 50gb shards


def refresh_shards(url):
    es = utils.get_es_connection(url, ENV)

    es.delete_by_query(
        index=".tasks",
        body={
            "query": {
                "bool": {
                    "must": {"match_all": {}},
                    "filter": {"term": {"completed": "true"}},
                }
            }
        },
    )

    indices = []

    for i in es.cat.indices(format="JSON"):
        if int(i["docs.count"]) == 0:
            es.indices.delete(i["index"])

            continue

        if not i["index"].startswith(".") and "deduped" not in i["index"]:
            indices.append(i["index"].split("-")[0])

    for index in set(indices):
        shards = es.cat.indices(index=f"{index}*", format="JSON")

        es.delete_by_query(
            index=min([x["index"] for x in shards]),
            body={
                "query": {
                    "range": {
                        "serverTimestamp": {
                            "lte": utils.to_unix_ms(
                                datetime.now() - timedelta(days=MAX_AGE)
                            )
                        }
                    }
                }
            },
            conflicts="proceed",
            wait_for_completion=False,
        )

        es.indices.rollover(
            index,
            body=dict(
                {"conditions": {"max_size": MAX_SIZE}},
                **utils.get_index_template(schemas.get(index, flavor="elasticsearch")),
            ),
        )


with DAG(
    "", default_args=default_args, schedule_interval="0 * * * *"
) as dag:
    PythonOperator(
        task_id="",
        python_callable=refresh_shards,
        op_kwargs={"url": ENV["ES_HOST"]},
    )
