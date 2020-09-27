import importlib
import json
import os
import time

import boto3
import numpy as np
import pandas as pd
import requests

from datetime import datetime, timedelta

from elasticsearch import Elasticsearch, RequestsHttpConnection, helpers
from requests_aws4auth import AWS4Auth
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.dialects.postgresql import insert

from . import queries


LOG_PRIORITY = ["INFO", "WARN", "ERROR", "CRIT"]
LOG_LEVEL = LOG_PRIORITY.index(os.environ.get("LOG_LEVEL", "WARN"))


#
# Basic utilities
#


def log(msg, level="INFO"):
    if LOG_PRIORITY.index(level) >= LOG_LEVEL:
        print(" | ".join([level, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), msg]))


def get_today():
    return datetime.now().strftime("%Y-%m-%d")


def load_environment():
    if importlib.util.find_spec("local_settings") is not None:
        return importlib.import_module("local_settings").environ

    return os.environ


def to_records(df):
    return [
        {k: v if not pd.isnull(v) else None for k, v in r.items()}
        for r in df.to_dict("records")
    ]


def to_unix_ms(dt):
    return int(dt.timestamp() * 1000)


#
# ElasticSearch
#


def get_es_connection(url, access):
    return Elasticsearch(
        hosts=[{"host": url, "port": 443}],
        http_auth=AWS4Auth(
            access["AWS_ACCESS_KEY_ID"],
            access["AWS_SECRET_ACCESS_KEY"],
            access["AWS_DEFAULT_REGION"],
            "es",
        ),
        use_ssl=True,
        verify_certs=True,
        timeout=30,
        connection_class=RequestsHttpConnection,
    )


def get_index_template(schema):  # default index settings
    return {
        "settings": {"index.number_of_shards": 1, "index.number_of_replicas": 0},
        "mappings": {"properties": {f["name"]: {"type": f["type"]} for f in schema}},
    }


def to_es_records(df, index, pk=None):
    records = []
    for r in df.to_dict("records"):
        doc = {"_index": index, "_source": {}}

        for k, v in r.items():
            isnull = pd.isnull(v)
            if not isinstance(isnull, bool):
                isnull = isnull.any()

            if not isnull:
                doc["_source"][k] = v

        if pk is not None:
            doc["_id"] = r[pk]

        records.append(doc)

    return records


def create_index(es, name, schema):
    old_indices = [i["index"] for i in es.cat.indices(index=f"{name}*", format="JSON")]

    nth = (
        max([int("".join([x for x in i if x.isdigit()])) + 1 for i in old_indices])
        if len(old_indices)
        else 1
    )

    index = f"{name}-{nth:06}"

    es.indices.create(index=index, body=get_index_template(schema))

    if es.indices.exists_alias(name):
        for i, _ in es.indices.get_alias(name).items():
            es.indices.delete_alias(index=i, name=name)

    es.indices.update_aliases(
        body={"actions": [{"add": {"index": index, "alias": name}}]}
    )

    return index


def insert_es(instances, index, schema, data, pk=None):
    if data.empty:
        return

    for es in instances:
        if "streams" in es and index not in es["streams"]:
            continue

        content = (
            data[data["domain"].isin(es["domains"])].copy()
            if "domain" in data.columns and len(es.get("domains", []))
            else data.copy()
        )

        if content.empty:
            continue

        index_write = (
            max(es["client"].indices.get_alias(index).keys())
            if es["client"].indices.exists_alias(index)
            else create_index(es["client"], index, schema)
        )

        records = to_es_records(content, index_write, pk)

        helpers.bulk(es["client"], records, request_timeout=30)


#
# AWS
#


def get_aws_client(service, access):
    return boto3.client(
        service,
        aws_access_key_id=access["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=access["AWS_SECRET_ACCESS_KEY"],
        region_name=access["AWS_DEFAULT_REGION"],
    )


def get_s3_object(s3, bucket, key):
    content = s3.get_object(Bucket=bucket, Key=key)["Body"]

    if key.endswith(".json"):
        return json.load(content)

    return content


def get_s3_tag(s3, bucket, key):
    tags = s3.get_object_tagging(Bucket=bucket, Key=key)

    return {x["Key"]: x["Value"] for x in tags.get("TagSet", [])}


def put_s3_tag(s3, bucket, key, tag):
    s3.put_object_tagging(
        Bucket=bucket,
        Key=key,
        Tagging={"TagSet": [{"Key": str(k), "Value": str(v)} for k, v in tag.items()]},
    )


def put_sqs_msg(sqs, queue, msg):
    q = sqs.get_queue_url(QueueName=queue)
    sqs.send_message(QueueUrl=q["QueueUrl"], MessageBody=msg)


#
# Athena
#


def get_dt_filter(start, hours):
    recent_dates = {}
    for x in range(hours):
        d = start + timedelta(hours=x)
        s = d.strftime("%Y-%m-%d")

        if s not in recent_dates:
            recent_dates[s] = []

        recent_dates[s].append(f"{d.hour}")

    return " OR ".join(
        [
            f"(collected = '{d}' AND hour IN ({', '.join(h)}))"
            for d, h in recent_dates.items()
        ]
    )


def get_query_template(name, placeholders=None):
    with importlib.resources.open_text(queries, f"{name}.sql") as f:
        query = f.read()

    if placeholders is not None:
        query = query.format(**placeholders)

    return query


def query_athena(athena, query, path, wait=True, wait_time=90, queue_time=120):
    job_id = athena.start_query_execution(
        QueryString=query,
        WorkGroup="",
        ResultConfiguration={"OutputLocation": path},
    )["QueryExecutionId"]

    if not wait:
        return job_id

    job_status = "QUEUED"

    while job_status != "SUCCEEDED" and wait_time > 0 and queue_time > 0:
        response = athena.get_query_execution(QueryExecutionId=job_id)
        job_status = response["QueryExecution"]["Status"]["State"]

        if job_status == "QUEUED":
            queue_time -= 1
        elif job_status == "RUNNING":
            wait_time -= 1
        elif job_status in ["CANCELLED", "FAILED"]:
            raise RuntimeError(f"Execution returned with status {job_status}")

        time.sleep(1)

    if job_status != "SUCCEEDED":
        raise RuntimeError(f"Athena query ran for longer than wait time")

    return job_id


#
# SQL
#


def get_sql_connection(url, isolation_level="READ UNCOMMITTED"):
    return (
        create_engine(url, isolation_level=isolation_level),
        MetaData(),
    )


def query_sql(query, engine):
    with engine.connect() as con:
        data = pd.read_sql(query, con=con)

    return data


def upsert_sql(df, table, engine, metadata, chunksize=5000, pk=[]):
    if df.empty:
        return

    tbl = Table(table, metadata, autoload=True, autoload_with=engine)

    with engine.connect() as con:
        for chunk in np.array_split(df, np.ceil(len(df) / chunksize)):
            query = insert(tbl).values(to_records(chunk))

            if len(pk):
                query = query.on_conflict_do_update(
                    constraint=tbl.primary_key,
                    set_={
                        col.name: col for col in query.excluded if col.name not in pk
                    },
                )

            con.execute(query)