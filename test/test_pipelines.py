import json
import os
import urllib

import boto3

from elasticsearch.client import Elasticsearch

from pipelines import core


def test_env():
    sources = [] # ENV vars or local_settings.py

    assert all(s in os.environ for s in sources)


def test_resources():
    results = {}
    core.create_cached_resources(results)

    assert results # check resources in expected structure


def test_validation():
    key = "key_1"

    results = core.parse_and_validate_batch(key)

    assert results is not None

    index, schema = results

    assert isinstance(schema, list)  # Not the best..


def test_batch_processing_1():
    sqs = boto3.client("sqs")
    queue = sqs.get_queue_url(QueueName="es-logger")["QueueUrl"]

    response = sqs.receive_message(
        QueueUrl=queue,
        MessageAttributeNames=["All"],
        VisibilityTimeout=30,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=20,  # 20 is max
    )

    assert "Messages" in response, "No messages for testing"

    msg = response["Messages"][0]
    content = json.loads(msg["Body"])

    packet = urllib.parse.unquote(content["Records"][0]["s3"]["object"]["key"])
    results = core.parse_and_validate_batch(packet)

    assert results is not None, "Invalid batch"

    index, schema = results
    core.message_handler(index, schema, packet)

    sqs.delete_message(QueueUrl=queue, ReceiptHandle=msg["ReceiptHandle"])

# And additional processing based on specific needs