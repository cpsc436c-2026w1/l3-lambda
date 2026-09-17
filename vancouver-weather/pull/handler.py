"""Scheduled function: pull the current weather for ten cities and append the rows to today's file.

Trigger: an EventBridge rule, rate(15 minutes), which is how often Open-Meteo refreshes its
current conditions. Source: Open-Meteo (no key; free tier 10,000 calls a day, we make 96), all
ten cities in one call. Store: one CSV per day under s3://<BUCKET>/<PREFIX>/YYYY-MM-DD.csv, one
row per city per pull, times in UTC. Nothing runs between pulls.
"""
import csv
import io
import json
import os
import urllib.request
from datetime import datetime, timezone

import boto3

BUCKET = os.environ["BUCKET"]                # config comes from the console, not the code
PREFIX = os.environ.get("PREFIX", "weather/cities")
CITIES = [  # name, latitude, longitude
    ("Vancouver", 49.2827, -123.1207), ("Victoria", 48.4284, -123.3656), ("Kelowna", 49.8880, -119.4960),
    ("Prince George", 53.9171, -122.7497), ("Whistler", 50.1163, -122.9574), ("Toronto", 43.6532, -79.3832),
    ("Montreal", 45.5019, -73.5674), ("Calgary", 51.0447, -114.0719), ("Bergen", 60.3913, 5.3221),
    ("Reykjavik", 64.1466, -21.9426),
]
FIELDS = ["city", "time", "temperature_c", "humidity_pct", "precipitation_mm", "wind_kmh", "weather_code"]
URL = ("https://api.open-meteo.com/v1/forecast?latitude=" + ",".join(str(c[1]) for c in CITIES)
       + "&longitude=" + ",".join(str(c[2]) for c in CITIES)
       + "&current=temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m,weather_code&timezone=UTC&forecast_days=1")

s3 = boto3.client("s3")                      # module level, so it runs once per environment


def fetch() -> list:
    with urllib.request.urlopen(URL, timeout=10) as r:
        return json.load(r)                     # one object per city, in the order asked


def rows_from(payload: list) -> list[dict]:
    """One CSV row per city from Open-Meteo's per-location 'current' blocks."""
    out = []
    for (city, _, _), p in zip(CITIES, payload):
        c = p["current"]
        out.append({"city": city, "time": c["time"], "temperature_c": c["temperature_2m"],
                    "humidity_pct": c["relative_humidity_2m"], "precipitation_mm": c["precipitation"],
                    "wind_kmh": c["wind_speed_10m"], "weather_code": c["weather_code"]})
    return out


def append_rows(rows: list[dict]) -> str:
    """Append the rows to today's CSV (read, add, write back); return the key."""
    key = f"{PREFIX}/{datetime.now(timezone.utc):%Y-%m-%d}.csv"
    try:
        existing = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read().decode()
    except s3.exceptions.NoSuchKey:
        existing = ""
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=FIELDS)
    if not existing:
        w.writeheader()
    w.writerows(rows)
    s3.put_object(Bucket=BUCKET, Key=key, Body=(existing + buf.getvalue()).encode(),
                  ContentType="text/csv")
    return key


def lambda_handler(event, context):          # the entry point. Its name is a setting
    # event carries what triggered us, and a schedule sends an empty one. context carries
    # the request id and how many milliseconds are left before the timeout.
    rows = rows_from(fetch())
    key = append_rows(rows)
    print(json.dumps({"wrote": key, "rows": len(rows), "time": rows[0]["time"]}))
    # print goes to CloudWatch Logs. The return value goes back to whoever invoked us,
    # and a schedule throws it away.
    return {"key": key, "rows": len(rows)}
