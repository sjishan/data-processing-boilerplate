import io
import json
import pickle
import zipfile

import boto3
import pandas as pd

from pipelines import transform

s3 = boto3.client(
    "s3",
    aws_access_key_id="",
    aws_secret_access_key="",
    region_name="",
)


# Load test cases from S3
f = s3.get_object(Bucket="", Key="")
with zipfile.ZipFile(io.BytesIO(f["Body"].read()), "r") as content:
    content.extractall("./test")


def test_case_1():
    data = pd.read_json("batch_1.json", convert_dates=False, lines=True)
    with open("schema_1.json", "r") as f:
        schema = json.load(f)

    with open("result_1.pickle", "rb") as f:
        a = pickle.load(f)

    pd.testing.assert_frame_equal(a, transform.standarize_pandas_df(data, schema))

# And similar steps for testing additional batch specific functions