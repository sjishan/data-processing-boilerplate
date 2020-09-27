import hashlib
import json

import numpy as np
import pandas as pd

from datetime import datetime, timedelta


def match_columns(df, schema):
    return df[[x["name"] for x in schema if x["name"] in df.columns]]


def truncate_bytes(s, length=31765, encoding="utf-8"):
    encoded = s.encode(encoding)[:length]
    return encoded.decode(encoding, "ignore")


def hash(r):
    return hashlib.sha256("|".join([str(v) for v in r.values()]).encode()).hexdigest()


def standarize_pandas_df(data, schema, defaults=None, requirements=None):
    """Standarize data types and format"""

    if requirements is not None:
        for col in requirements.get("must_have", []):
            if col not in data.columns:
                return

            data = data[~pd.isnull(data[col])]  # NOTE: data loss can happen here

        for col in requirements.get("should_have", []):
            if col not in data.columns:
                data[col] = None

    uniq_on = []
    for field in schema:
        column, dtype = field["name"], field.get("type", "keyword")
        if column not in data.columns:
            continue

        if defaults is not None and column in defaults:
            data[column] = data[column].fillna(defaults[column])

        if dtype not in ["object", "text"]:
            uniq_on.append(column)

        if dtype == "boolean":
            data[column] = data[column].apply(
                lambda x: x in [True, "true", 1, "1"] if not pd.isnull(x) else None
            )
        elif dtype == "text":
            data[column] = data[column].apply(
                lambda x: truncate_bytes(x) if isinstance(x, str) else None
            )

        if dtype not in TYPES:
            # NOTE: Need to cast only the non-null values or else None converts to "None"
            if "mode" not in field:
                data.loc[~pd.isnull(data[column]), column] = data.loc[
                    ~pd.isnull(data[column]), column
                ].astype("str")
            else:
                data[column] = data[column].astype("object")

            data.loc[pd.isnull(data[column]), column] = data.loc[
                pd.isnull(data[column]), column
            ].replace({np.nan: None})
        elif TYPES[dtype] == "Int64":
            # NOTE: Bug in pandas causes crashes when converting str values directly to Int64
            data[column] = data[column].astype("float").astype(TYPES[dtype])
        else:
            data[column] = data[column].astype(TYPES[dtype])

    return match_columns(data.drop_duplicates(subset=uniq_on, keep="first"), schema)


# Additional specific processing beyond generic data standarization

def processing_group1(**kwargs):
    pass

def processing_group2(**kwargs):
    pass

