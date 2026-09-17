"""Hourly function, pandas: fold the day's CSV pulls into this engine's own Parquet file.

Trigger: an EventBridge rule, rate(1 hour). Reads today's and yesterday's day files (UTC), keeps
one row per city and reading time, merges them into <PARQUET_KEY>, writes it back. The Parquet
file is the dataset the later blocks query; the CSVs are the raw pulls. Its twin,
compact-duckdb/handler.py, does the same job with DuckDB. Needs the AWS SDK for pandas layer.
"""
import io
import json
import os
from datetime import datetime, timedelta, timezone

import boto3
import pandas as pd

BUCKET = os.environ["BUCKET"]
PREFIX = os.environ.get("PREFIX", "weather/cities")
PARQUET_KEY = os.environ.get("PARQUET_KEY_PANDAS", "weather/cities-pandas.parquet")
COLS = ["city", "time", "temperature_c", "humidity_pct", "precipitation_mm", "wind_kmh", "weather_code"]
DTYPES = {"temperature_c": "float64", "humidity_pct": "float64", "precipitation_mm": "float64",
          "wind_kmh": "float64", "weather_code": "int64"}

s3 = boto3.client("s3")


def read_csv_days(days_back: int = 1) -> pd.DataFrame:
    frames = []
    for d in range(days_back + 1):
        day = (datetime.now(timezone.utc) - timedelta(days=d)).strftime("%Y-%m-%d")
        try:
            body = s3.get_object(Bucket=BUCKET, Key=f"{PREFIX}/{day}.csv")["Body"].read()
        except s3.exceptions.NoSuchKey:
            continue
        frames.append(pd.read_csv(io.BytesIO(body)))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=COLS)


def read_parquet() -> pd.DataFrame:
    try:
        body = s3.get_object(Bucket=BUCKET, Key=PARQUET_KEY)["Body"].read()
    except s3.exceptions.NoSuchKey:
        return pd.DataFrame(columns=COLS)
    return pd.read_parquet(io.BytesIO(body))


def merge(existing: pd.DataFrame, new: pd.DataFrame) -> pd.DataFrame:
    """One row per city and reading time, sorted; the source's own timestamp is the key."""
    both = pd.concat([existing, new], ignore_index=True)
    both["time"] = pd.to_datetime(both["time"])
    both = both.drop_duplicates(subset=["city", "time"], keep="last").sort_values(["time", "city"]).reset_index(drop=True)
    return both[COLS].astype(DTYPES)


def lambda_handler(event, context):
    before = read_parquet()
    merged = merge(before, read_csv_days())
    buf = io.BytesIO()
    merged.to_parquet(buf, index=False)
    s3.put_object(Bucket=BUCKET, Key=PARQUET_KEY, Body=buf.getvalue(),
                  ContentType="application/x-parquet")
    out = {"rows_before": int(len(before)), "rows_after": int(len(merged)), "bytes": buf.getbuffer().nbytes, "key": PARQUET_KEY}
    print(json.dumps(out))
    return out
