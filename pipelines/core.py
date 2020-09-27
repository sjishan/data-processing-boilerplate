import json
import os
import time
import urllib

import pandas as pd
import redis
import sentry_sdk
import yaml

from common import utils, schemas
from pipelines import transform

ENV = utils.load_environment()

CONFIGS = yaml.load(ENV.get("POD", ""), Loader=yaml.SafeLoader)
DEFAULTS = {}

RESOURCES = {}

engine, metadata = utils.get_sql_connection(ENV["DB"])

r = redis.StrictRedis(host=ENV["REDIS"], port=6379, db=0)

s3 = utils.get_aws_client("s3", ENV)
sqs = utils.get_aws_client("sqs", ENV)


def create_cached_resources(context):
    # Fetch external resources that are applicable for in memory processing
    # eg. data enhancements, defaults, etc.
    pass
    

def create_or_update_state_resources(context):
    # Cache pipeline state for stateful processing at a set interval
    pass
    


def parse_and_validate_batch(s3_object_key):
    # Parse info from s3 object key name and validate structure
    pass


def message_handler(index, schema, packet):
    # tag object as processing initiated
    utils.put_s3_tag(s3, ENV["S3_BUCKET"], packet, {"start": time.time()})

    create_cached_resources(RESOURCES)  # run once at start since mostly static

    df = transform.standarize_pandas_df(
        pd.read_json(
            f"s3://{ENV['S3_BUCKET']}/{packet}", convert_dates=False, lines=True
        ),
        schema,
        defaults=DEFAULTS,
        requirements=CONFIGS.get("columns"),
    )

    if df is None:  # NOTE: occurs when must have columns are missing
        return index

    # handle sqs message duplication
    df["is_x_pkt_dup"] = df["id"].apply(lambda x: r.exists(f"{index}_{x}"))
    df = df.drop(df[df["is_x_pkt_dup"] == 1].index).drop("is_x_pkt_dup", axis=1)

    if df.empty:
        return index

    # additional data processing dependent on pipeline type
    if CONFIGS.get("group") == "group1":
        pass
    elif CONFIGS.get("group") == "group2":
        pass

    # NOTE: on retries es will duplicate but s3 will be replaced if previously ran
    dump_es, dump_s3 = False, False

    try:
        utils.insert_es(RESOURCES["es"], index, schema, df)
        dump_es = True
    except Exception as e:
        sentry_sdk.capture_exception(e)

    fn = packet.split("/")[-1]
    collected, hour = "-".join(fn.split("-")[2:5]), int(fn.split("-")[5])

    path = f"s3://{ENV['S3_BUCKET']}/athena/{index}/stream/collected={collected}/hour={hour}/{fn}"

    for field in schema:
        if field.get("type") == "object" and field["name"] in df.columns:
            df[field["name"]] = df[field["name"]].apply(lambda x: json.dumps(x))

    try:
        df.to_parquet(path)  # dump final result as parquet for athena
        dump_s3 = True
    except Exception as e:
        sentry_sdk.capture_exception(e)

    # handle data duplication
    df["id"].apply(lambda x: r.setex(f"{index}_{x}", 15 * 60, 0))

    # tag object as processing completed
    if dump_es and dump_s3:
        utils.put_s3_tag(s3, ENV["S3_BUCKET"], packet, {"done": time.time()})

    return index


def run():
    queue = sqs.get_queue_url(QueueName=CONFIGS["queue"])["QueueUrl"]
    monitor = sqs.get_queue_url(QueueName=ENV["MONITOR_PIPELINE_QUEUE"])["QueueUrl"]

    while True:  # process one message (packet) per loop
        response = sqs.receive_message(
            QueueUrl=queue,
            MessageAttributeNames=["All"],
            VisibilityTimeout=CONFIGS["times"]["visible"],
            MaxNumberOfMessages=1,
            WaitTimeSeconds=20,  # 20 is max
        )

        if "Messages" not in response:  # empty queue
            continue

        start = time.time()

        index, status = None, "failure"

        msg = response["Messages"][0]  # receive_message only fetches 1 msg at a time
        content = json.loads(msg["Body"])

        if "TestEvent" in content.get("Event", ""):
            sqs.delete_message(QueueUrl=queue, ReceiptHandle=msg["ReceiptHandle"])
            continue

        try:
            packet = urllib.parse.unquote(content["Records"][0]["s3"]["object"]["key"])
            results = parse_index(packet)

            if results is not None:
                index, schema = results
                message_handler(index, schema, packet)

            status = "success"
        except Exception as e:
            sentry_sdk.capture_exception(e)

        # track pipeline performance metric
        sqs.send_message(
            QueueUrl=monitor,
            MessageBody=json.dumps(
                {
                    "stream": index,
                    "packet": packet,
                    "latency": time.time() - start,
                    "status": status,
                }
            ),
        )

        sqs.delete_message(QueueUrl=queue, ReceiptHandle=msg["ReceiptHandle"])


if __name__ == "__main__":
    sentry_sdk.init(ENV["SENTRY_URL"])

    run()
