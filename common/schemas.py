from . import models, utils

ENV = utils.load_environment()


def get(stream, flavor="pandas"):
    schema = getattr(models, stream)()
    return schema.describe(flavor)


def sync(stream):
    schema = getattr(models, stream)()

    athena = utils.get_aws_client("athena", ENV)
    s3 = utils.get_aws_client("s3", ENV)

    utils.create_index(
        utils.get_es_connection(ENV["ES_HOST"], ENV),
        stream,
        schema.describe("elasticsearch"),
    )

    utils.query_athena(
        athena, f"DROP TABLE IF EXISTS {stream}", ENV["ATHENA_STAGING_LOC"]
    )

    utils.query_athena(
        athena,
        f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {stream} (
            {schema.describe("athena")}
        ) PARTITIONED BY (
            collected string,
            hour int
        )
        ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
        WITH SERDEPROPERTIES (
            'serialization.format' = '1'
        ) LOCATION ''
        TBLPROPERTIES ('has_encrypted_data'='false');
        """,
        ENV["ATHENA_STAGING_LOC"],
    )

    utils.query_athena(
        athena,
        f"MSCK REPAIR TABLE {stream}",
        ENV["ATHENA_STAGING_LOC"],
        wait=False,
    )
