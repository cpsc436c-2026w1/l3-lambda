"""Compaction, pandas, one Parquet per day: the layout that does not rewrite the past.

The same trigger and the same engine as compact/handler.py, one change: the day file the event
names becomes that day's own Parquet, under <DAILY_PREFIX>/day=YYYY-MM-DD/part.parquet, and no
other day is read or written. Memory is bounded by one day whatever the history holds. The chart
reads the folder and skips the days outside its window. Needs the AWS SDK for pandas layer.
"""
import io
import json
import os
from datetime import datetime, timedelta, timezone

import boto3
import pandas as pd

BUCKET = os.environ["BUCKET"]
PREFIX = os.environ.get("PREFIX", "gdelt/events")
DAILY_PREFIX = os.environ.get("DAILY_PREFIX", "gdelt/events-daily")
NUMERIC = {"GlobalEventID": "int64", "Day": "int64", "IsRootEvent": "int64", "QuadClass": "int64",
           "GoldsteinScale": "float64", "NumMentions": "int64", "NumSources": "int64", "NumArticles": "int64",
           "AvgTone": "float64", "Actor1Geo_Lat": "float64", "Actor1Geo_Long": "float64",
           "Actor2Geo_Lat": "float64", "Actor2Geo_Long": "float64", "ActionGeo_Lat": "float64", "ActionGeo_Long": "float64"}

s3 = boto3.client("s3")


def key_from(event) -> str:
    key = ((event or {}).get("detail") or {}).get("object", {}).get("key")
    return key or f"{PREFIX}/{datetime.now(timezone.utc):%Y-%m-%d}.csv"


def typed(df: pd.DataFrame) -> pd.DataFrame:
    for col, t in NUMERIC.items():
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype(t if t == "float64" else "Int64")
    return df


def lambda_handler(event, context):
    key = key_from(event)
    day = os.path.basename(key)[:-4]
    body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    df = typed(pd.read_csv(io.BytesIO(body), dtype=str, keep_default_na=False))
    df = df.drop_duplicates(subset=["GlobalEventID"], keep="last").sort_values("GlobalEventID")
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    out_key = f"{DAILY_PREFIX}/day={day}/part.parquet"
    s3.put_object(Bucket=BUCKET, Key=out_key, Body=buf.getvalue(), ContentType="application/x-parquet")
    out = {"rows_after": int(len(df)), "bytes": buf.getbuffer().nbytes, "key": out_key}
    print(json.dumps(out))
    return out
