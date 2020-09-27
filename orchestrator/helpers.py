import json

from datetime import timedelta

from common import utils


def build_s3_path(bucket, context, fmt):
    return "/".join(
        [
            bucket,
            "airflow",
            "tmp",
            f"{context['task_instance_key_str']}-{context['execution_date'].strftime('%Y-%m-%d %H:%M:%S')}.{fmt}",
        ]
    )


def read_messages(sqs, queue, fs, bucket, ack=False, **context):
    cutoff = utils.to_unix_ms(context["execution_date"] + timedelta(days=1))

    msgs = []
    has_more = True
    while has_more:
        response = sqs.receive_message(
            QueueUrl=queue, AttributeNames=["SentTimestamp"], MaxNumberOfMessages=10,
        )

        if "Messages" in response:
            page = [
                m
                for m in response["Messages"]
                if int(m["Attributes"]["SentTimestamp"]) < cutoff
            ]

            # NOTE: to avoid hitting the 120000 cap for pipeline queue
            if len(page) and ack:
                sqs.delete_message_batch(
                    QueueUrl=queue,
                    Entries=[
                        {"Id": m["MessageId"], "ReceiptHandle": m["ReceiptHandle"]}
                        for m in page
                    ],
                )

            msgs.extend(page)

        has_more = "Messages" in response and len(page) == 10

    path = build_s3_path(bucket, context, "json")
    with fs.open(path, "w") as f:
        json.dump(msgs, f)

    return path


def ack_messages(sqs, queue, fs, bucket, **context):
    path = context["ti"].xcom_pull(task_ids="read_messages")

    with fs.open(path, "r") as f:
        msgs = json.load(f)

    batch_size = 10
    for i in range(0, len(msgs), batch_size):
        sqs.delete_message_batch(
            QueueUrl=queue,
            Entries=[
                {"Id": m["MessageId"], "ReceiptHandle": m["ReceiptHandle"]}
                for m in msgs[i : i + batch_size]
            ],
        )
