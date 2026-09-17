"""Compaction, DuckDB: fold the day's GDELT CSVs into this engine's own Parquet file.

The twin of compact/handler.py (pandas), same trigger, same day files, same rule (one row per
event id), a different engine and a different output key. All of it runs inside DuckDB over
s3:// through the httpfs extension the layer carries; the engine streams and spills to /tmp,
so the same 45 MB day files meet the memory setting differently. No boto3.
"""
import json
import os
from datetime import datetime, timedelta, timezone

import duckdb

BUCKET = os.environ["BUCKET"]
PREFIX = os.environ.get("PREFIX", "gdelt/events")
PARQUET_KEY = os.environ.get("PARQUET_KEY_DUCKDB", "gdelt/events-duckdb.parquet")
HTTPFS = os.environ.get("HTTPFS_PATH", "/opt/duckdb_ext/httpfs.duckdb_extension")
REGION = os.environ.get("AWS_REGION", "ca-central-1")
MEMORY_LIMIT = os.environ.get("DUCKDB_MEMORY", "600MB")
CASTS = {"GlobalEventID": "BIGINT", "Day": "BIGINT", "IsRootEvent": "BIGINT", "QuadClass": "BIGINT",
         "GoldsteinScale": "DOUBLE", "NumMentions": "BIGINT", "NumSources": "BIGINT", "NumArticles": "BIGINT",
         "AvgTone": "DOUBLE", "Actor1Geo_Lat": "DOUBLE", "Actor1Geo_Long": "DOUBLE",
         "Actor2Geo_Lat": "DOUBLE", "Actor2Geo_Long": "DOUBLE", "ActionGeo_Lat": "DOUBLE", "ActionGeo_Long": "DOUBLE"}
CON = None


def connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    if os.path.exists(HTTPFS):
        con.execute(f"LOAD '{HTTPFS}'")
    else:
        con.execute("INSTALL httpfs; LOAD httpfs")
    q = lambda name: os.environ.get(name, "").replace("'", "''")
    con.execute(f"CREATE SECRET s3 (TYPE s3, PROVIDER config, KEY_ID '{q('AWS_ACCESS_KEY_ID')}', "
                f"SECRET '{q('AWS_SECRET_ACCESS_KEY')}', SESSION_TOKEN '{q('AWS_SESSION_TOKEN')}', REGION '{REGION}', ENDPOINT 's3.{REGION}.amazonaws.com')")
    os.makedirs("/tmp/duck", exist_ok=True)
    con.execute(f"SET memory_limit='{MEMORY_LIMIT}'; SET temp_directory='/tmp/duck'; SET preserve_insertion_order=false")
    return con


def lambda_handler(event, context):
    global CON
    if CON is None:
        CON = connect()
    con = CON
    key = ((event or {}).get("detail") or {}).get("object", {}).get("key")     # the S3 event names the day file
    if key:
        files = [f"s3://{BUCKET}/{key}"]
    else:                                                                       # by hand: today and yesterday
        days = [(datetime.now(timezone.utc) - timedelta(days=d)).strftime("%Y-%m-%d") for d in (1, 0)]
        present = {os.path.basename(f) for (f,) in con.execute("SELECT file FROM glob($g)", {"g": f"s3://{BUCKET}/{PREFIX}/*.csv"}).fetchall()}
        files = [f"s3://{BUCKET}/{PREFIX}/{d}.csv" for d in days if f"{d}.csv" in present]
    parquet = f"s3://{BUCKET}/{PARQUET_KEY}"
    # a glob without a wildcard is returned unchecked, so list the folder and look for the key
    folder = {f for (f,) in con.execute("SELECT file FROM glob($g)", {"g": f"s3://{BUCKET}/{os.path.dirname(PARQUET_KEY)}/*.parquet"}).fetchall()}
    have_parquet = parquet in folder
    cast = ", ".join(f"TRY_CAST({c} AS {t}) AS {c}" for c, t in CASTS.items())
    new = f"SELECT * REPLACE ({cast}) FROM read_csv({json.dumps(files)}, header=true, all_varchar=true, union_by_name=true)"
    if have_parquet:                          # keep the old rows, add the ids not seen before
        body = f"SELECT * FROM read_parquet('{parquet}') UNION ALL BY NAME SELECT * FROM ({new}) n WHERE n.GlobalEventID NOT IN (SELECT GlobalEventID FROM read_parquet('{parquet}'))"
    else:
        body = f"SELECT * FROM ({new}) QUALIFY row_number() OVER (PARTITION BY GlobalEventID) = 1"
    after = con.execute(f"COPY ({body}) TO '{parquet}' (FORMAT PARQUET)").fetchone()[0]
    out = {"rows_after": after, "key": PARQUET_KEY, "files": len(files)}
    print(json.dumps(out))
    return out
