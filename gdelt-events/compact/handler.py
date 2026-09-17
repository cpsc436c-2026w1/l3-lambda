"""Compaction, pandas: fold the day's GDELT CSVs into this engine's own Parquet file.

Trigger: the CSV write, through EventBridge. The same code shape as the weather compaction:
read the day file the event names (about 45 MB) and the existing Parquet, keep one row per event id,
write the Parquet back. The day files here are about 45 MB each and 61 columns wide, so the
whole-in-memory model meets the function's memory setting. Needs the AWS SDK for pandas layer.
"""
import io
import json
import os
from datetime import datetime, timedelta, timezone

import boto3
import pandas as pd

BUCKET = os.environ["BUCKET"]
PREFIX = os.environ.get("PREFIX", "gdelt/events")
PARQUET_KEY = os.environ.get("PARQUET_KEY_PANDAS", "gdelt/events-pandas.parquet")
NUMERIC = {"GlobalEventID": "int64", "Day": "int64", "IsRootEvent": "int64", "QuadClass": "int64",
           "GoldsteinScale": "float64", "NumMentions": "int64", "NumSources": "int64", "NumArticles": "int64",
           "AvgTone": "float64", "Actor1Geo_Lat": "float64", "Actor1Geo_Long": "float64",
           "Actor2Geo_Lat": "float64", "Actor2Geo_Long": "float64", "ActionGeo_Lat": "float64", "ActionGeo_Long": "float64"}

s3 = boto3.client("s3")


def keys_from(event) -> list[str]:
    """The day file the S3 event names; without an event, today's and yesterday's."""
    key = ((event or {}).get("detail") or {}).get("object", {}).get("key")
    if key:
        return [key]
    return [f"{PREFIX}/{(datetime.now(timezone.utc) - timedelta(days=d)):%Y-%m-%d}.csv" for d in (1, 0)]


def read_csvs(keys: list[str]) -> pd.DataFrame:
    frames = []
    for key in keys:
        try:
            body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        except s3.exceptions.NoSuchKey:
            continue
        frames.append(pd.read_csv(io.BytesIO(body), dtype=str, keep_default_na=False))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def read_parquet() -> pd.DataFrame:
    try:
        body = s3.get_object(Bucket=BUCKET, Key=PARQUET_KEY)["Body"].read()
    except s3.exceptions.NoSuchKey:
        return pd.DataFrame()
    return pd.read_parquet(io.BytesIO(body))


def typed(df: pd.DataFrame) -> pd.DataFrame:
    for col, t in NUMERIC.items():
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype(t if t == "float64" else "Int64")
    return df


def merge(existing: pd.DataFrame, new: pd.DataFrame) -> pd.DataFrame:
    """One row per event id; the newest copy wins."""
    both = pd.concat([existing, typed(new)], ignore_index=True)
    if both.empty:
        return both
    return both.drop_duplicates(subset=["GlobalEventID"], keep="last").sort_values("GlobalEventID").reset_index(drop=True)


def lambda_handler(event, context):
    before = read_parquet()
    merged = merge(before, read_csvs(keys_from(event)))
    buf = io.BytesIO()
    merged.to_parquet(buf, index=False)
    s3.put_object(Bucket=BUCKET, Key=PARQUET_KEY, Body=buf.getvalue(), ContentType="application/x-parquet")
    out = {"rows_before": int(len(before)), "rows_after": int(len(merged)), "bytes": buf.getbuffer().nbytes, "key": PARQUET_KEY}
    print(json.dumps(out))
    return out
