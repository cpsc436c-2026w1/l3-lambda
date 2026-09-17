"""Compaction, DuckDB: fold the day's CSV pulls into this engine's own Parquet file.

The twin of compact/handler.py (pandas), same trigger, same CSVs, same rule (one row per city
and reading time), a different engine and a different output key. Everything here is DuckDB:
the httpfs extension reads the CSVs and the old Parquet over s3:// and COPY writes the new file
back, with the function's own credentials from the environment. No boto3. Needs our DuckDB
layer, which carries the extension at /opt/duckdb_ext/httpfs.duckdb_extension.
"""
import json
import os
from datetime import datetime, timedelta, timezone

import duckdb

BUCKET = os.environ["BUCKET"]
PREFIX = os.environ.get("PREFIX", "weather/cities")
PARQUET_KEY = os.environ.get("PARQUET_KEY_DUCKDB", "weather/cities-duckdb.parquet")
HTTPFS = os.environ.get("HTTPFS_PATH", "/opt/duckdb_ext/httpfs.duckdb_extension")
REGION = os.environ.get("AWS_REGION", "ca-central-1")


def connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    if os.path.exists(HTTPFS):
        con.execute(f"LOAD '{HTTPFS}'")
    else:                                   # a laptop: DuckDB fetches the extension itself
        con.execute("INSTALL httpfs; LOAD httpfs")
    q = lambda name: os.environ.get(name, "").replace("'", "''")   # the function's own credentials, from its environment
    con.execute(f"CREATE SECRET s3 (TYPE s3, PROVIDER config, KEY_ID '{q('AWS_ACCESS_KEY_ID')}', "
                f"SECRET '{q('AWS_SECRET_ACCESS_KEY')}', SESSION_TOKEN '{q('AWS_SESSION_TOKEN')}', REGION '{REGION}', ENDPOINT 's3.{REGION}.amazonaws.com')")
    return con


CON = None                                   # one connection per container: warm calls skip the extension load


def lambda_handler(event, context):
    global CON
    if CON is None:
        CON = connect()
    con = CON
    days = [(datetime.now(timezone.utc) - timedelta(days=d)).strftime("%Y-%m-%d") for d in (1, 0)]
    present = {os.path.basename(f) for (f,) in con.execute("SELECT file FROM glob($g)", {"g": f"s3://{BUCKET}/{PREFIX}/*.csv"}).fetchall()}
    sources = [f"SELECT * FROM read_csv('s3://{BUCKET}/{PREFIX}/{d}.csv', header=true, timestampformat='%Y-%m-%dT%H:%M')"
               for d in days if f"{d}.csv" in present]
    parquet = f"s3://{BUCKET}/{PARQUET_KEY}"
    # a glob without a wildcard is returned unchecked, so list the folder and look for the key
    folder = {f for (f,) in con.execute("SELECT file FROM glob($g)", {"g": f"s3://{BUCKET}/{os.path.dirname(PARQUET_KEY)}/*.parquet"}).fetchall()}
    if parquet in folder:
        sources.append(f"SELECT * FROM read_parquet('{parquet}')")
    after = con.execute(f"""
        COPY (
          SELECT city, CAST(time AS TIMESTAMP) AS time, CAST(temperature_c AS DOUBLE) AS temperature_c,
                 CAST(humidity_pct AS DOUBLE) AS humidity_pct, CAST(precipitation_mm AS DOUBLE) AS precipitation_mm,
                 CAST(wind_kmh AS DOUBLE) AS wind_kmh, CAST(weather_code AS BIGINT) AS weather_code
          FROM ({" UNION ALL BY NAME ".join(sources)})
          QUALIFY row_number() OVER (PARTITION BY city, time) = 1
          ORDER BY time, city
        ) TO '{parquet}' (FORMAT PARQUET)
    """).fetchone()[0]                      # COPY returns the rows written
    out = {"rows_after": after, "key": PARQUET_KEY}
    print(json.dumps(out))
    return out
