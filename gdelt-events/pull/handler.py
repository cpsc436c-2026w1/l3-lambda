"""Scheduled function: pull the latest GDELT 2.0 event file and append its rows to today's CSV.

Trigger: an EventBridge rule, rate(15 minutes), which is how often GDELT publishes a new file
(http://data.gdeltproject.org/gdeltv2/lastupdate.txt names it). Source: the export file, about
1,200 events and 61 columns per quarter hour, tab-separated, no header, zipped, no key needed.
Store: one CSV per day under s3://<BUCKET>/<PREFIX>/YYYY-MM-DD.csv (UTC), the same layout as the
weather pipeline, so a day file grows to about 45 MB. A backfill event {"day": "YYYY-MM-DD"}
writes one whole past day in one call (96 files).
"""
import csv
import io
import json
import os
import urllib.request
import zipfile
from datetime import datetime, timedelta, timezone

import boto3

BUCKET = os.environ["BUCKET"]
PREFIX = os.environ.get("PREFIX", "gdelt/events")
BASE = "http://data.gdeltproject.org/gdeltv2/"
FIELDS = ["GlobalEventID", "Day", "MonthYear", "Year", "FractionDate",
          "Actor1Code", "Actor1Name", "Actor1CountryCode", "Actor1KnownGroupCode", "Actor1EthnicCode",
          "Actor1Religion1Code", "Actor1Religion2Code", "Actor1Type1Code", "Actor1Type2Code", "Actor1Type3Code",
          "Actor2Code", "Actor2Name", "Actor2CountryCode", "Actor2KnownGroupCode", "Actor2EthnicCode",
          "Actor2Religion1Code", "Actor2Religion2Code", "Actor2Type1Code", "Actor2Type2Code", "Actor2Type3Code",
          "IsRootEvent", "EventCode", "EventBaseCode", "EventRootCode", "QuadClass", "GoldsteinScale",
          "NumMentions", "NumSources", "NumArticles", "AvgTone",
          "Actor1Geo_Type", "Actor1Geo_Fullname", "Actor1Geo_CountryCode", "Actor1Geo_ADM1Code", "Actor1Geo_ADM2Code",
          "Actor1Geo_Lat", "Actor1Geo_Long", "Actor1Geo_FeatureID",
          "Actor2Geo_Type", "Actor2Geo_Fullname", "Actor2Geo_CountryCode", "Actor2Geo_ADM1Code", "Actor2Geo_ADM2Code",
          "Actor2Geo_Lat", "Actor2Geo_Long", "Actor2Geo_FeatureID",
          "ActionGeo_Type", "ActionGeo_Fullname", "ActionGeo_CountryCode", "ActionGeo_ADM1Code", "ActionGeo_ADM2Code",
          "ActionGeo_Lat", "ActionGeo_Long", "ActionGeo_FeatureID", "DATEADDED", "SOURCEURL"]

s3 = boto3.client("s3")


def latest_stamp() -> str:
    """The newest file's timestamp, YYYYMMDDHHMMSS, from GDELT's pointer file."""
    with urllib.request.urlopen(BASE + "lastupdate.txt", timeout=10) as r:
        for line in r.read().decode().splitlines():
            if line.endswith(".export.CSV.zip"):
                return line.split("/")[-1].split(".")[0]
    raise RuntimeError("no export file in lastupdate.txt")


def fetch_rows(stamp: str) -> list[list[str]]:
    """The events of one quarter hour as rows of 61 fields; [] if that file does not exist."""
    try:
        with urllib.request.urlopen(f"{BASE}{stamp}.export.CSV.zip", timeout=30) as r:
            z = zipfile.ZipFile(io.BytesIO(r.read()))
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return []
        raise
    text = z.read(z.namelist()[0]).decode("utf-8", errors="replace")
    return [line.split("\t") for line in text.splitlines() if line]


def to_csv(rows: list[list[str]], header: bool) -> str:
    buf = io.StringIO()
    w = csv.writer(buf)
    if header:
        w.writerow(FIELDS)
    w.writerows(r[:len(FIELDS)] for r in rows)
    return buf.getvalue()


def append(day: str, rows: list[list[str]]) -> str:
    """Append rows to the day's CSV (read, add, write back, like the weather pull); return the key."""
    key = f"{PREFIX}/{day}.csv"
    try:
        existing = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read().decode()
    except s3.exceptions.NoSuchKey:
        existing = ""
    s3.put_object(Bucket=BUCKET, Key=key, Body=(existing + to_csv(rows, header=not existing)).encode(), ContentType="text/csv")
    return key


def lambda_handler(event, context):
    if event.get("day"):                        # backfill: the whole day in one write
        d = datetime.strptime(event["day"], "%Y-%m-%d")
        rows = []
        for q in range(96):
            rows += fetch_rows((d + timedelta(minutes=15 * q)).strftime("%Y%m%d%H%M%S"))
        key = f"{PREFIX}/{event['day']}.csv"
        s3.put_object(Bucket=BUCKET, Key=key, Body=to_csv(rows, header=True).encode(), ContentType="text/csv")
        out = {"wrote": key, "rows": len(rows), "backfill": True}
    else:
        stamp = latest_stamp()
        rows = fetch_rows(stamp)
        key = append(f"{stamp[:4]}-{stamp[4:6]}-{stamp[6:8]}", rows)
        out = {"wrote": key, "rows": len(rows), "stamp": stamp}
    print(json.dumps(out))
    return out
