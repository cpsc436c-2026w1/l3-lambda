"""Chart function, deployed twice: once over the CSV day files, once over the Parquet.

SOURCE=csv      read_csv over the last DAYS day files (about 45 MB each) on s3://
SOURCE=parquet  read_parquet over gdelt/events-duckdb.parquet on s3://

Same engine, same SQL, same picture: events per hour for the last days, optionally one
country (?country=CA, GDELT's ActionGeo_CountryCode, FIPS codes). Both read S3 through DuckDB's
httpfs extension, so the Parquet side fetches only the three columns the query touches and the
CSV side must read every byte of every file. The response says how long the query took.
"""
import json
import os
import time
from datetime import datetime, timedelta, timezone

import duckdb

BUCKET = os.environ["BUCKET"]
SOURCE = os.environ.get("SOURCE", "parquet")
PREFIX = os.environ.get("PREFIX", "gdelt/events")
PARQUET_KEY = os.environ.get("PARQUET_KEY_DUCKDB", "gdelt/events-duckdb.parquet")
DAILY_PREFIX = os.environ.get("DAILY_PREFIX", "gdelt/events-daily")
DAYS = int(os.environ.get("DAYS", "7"))
HTTPFS = os.environ.get("HTTPFS_PATH", "/opt/duckdb_ext/httpfs.duckdb_extension")
REGION = os.environ.get("AWS_REGION", "ca-central-1")
CON = None
CALLS = 0


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
    con.execute("SET memory_limit='600MB'; SET temp_directory='/tmp/duck'")
    return con


def source_sql(con) -> str:
    if SOURCE == "parquet":
        return f"read_parquet('s3://{BUCKET}/{PARQUET_KEY}')"
    if SOURCE == "daily":                       # one Parquet per day; the WHERE on day skips whole files
        since = (datetime.now(timezone.utc) - timedelta(days=DAYS)).strftime("%Y-%m-%d")
        return f"(SELECT * FROM read_parquet('s3://{BUCKET}/{DAILY_PREFIX}/*/*.parquet', hive_partitioning=true) WHERE day >= '{since}')"
    present = {os.path.basename(f) for (f,) in con.execute("SELECT file FROM glob($g)", {"g": f"s3://{BUCKET}/{PREFIX}/*.csv"}).fetchall()}
    files = [f"s3://{BUCKET}/{PREFIX}/{d}.csv" for d in ((datetime.now(timezone.utc) - timedelta(days=k)).strftime("%Y-%m-%d") for k in range(DAYS)) if f"{d}.csv" in present]
    if not files:
        return "(SELECT NULL AS DATEADDED, NULL AS ActionGeo_CountryCode, NULL AS AvgTone WHERE false)"
    return f"read_csv({json.dumps(files)}, header=true, all_varchar=true, union_by_name=true)"


def per_hour(con, country: str | None) -> list[tuple]:
    where = f"AND ActionGeo_CountryCode = '{country[:2].upper()}'" if country else ""
    return con.execute(f"""
        SELECT date_trunc('hour', strptime(CAST(DATEADDED AS VARCHAR), '%Y%m%d%H%M%S')) AS h, count(*) AS n, avg(TRY_CAST(AvgTone AS DOUBLE)) AS tone
        FROM {source_sql(con)}
        WHERE strptime(CAST(DATEADDED AS VARCHAR), '%Y%m%d%H%M%S') >= now() - INTERVAL {DAYS} DAY {where}
        GROUP BY 1 ORDER BY 1""").fetchall()


def svg(rows: list[tuple], country: str | None, ms: float, width: int = 900, height: int = 340) -> str:
    title_who = f"{country.upper()} events" if country else "all events"
    if not rows:
        return f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}"><text x="20" y="40" font-family="Helvetica" font-size="18">No data yet ({SOURCE})</text></svg>'
    L, R, T, B = 60, 20, 50, 50
    n = len(rows)
    xs = [L + (width - L - R) * (i / max(n - 1, 1)) for i in range(n)]
    counts = [r[1] for r in rows]
    top = max(counts) * 1.05
    ty = lambda v: T + (height - T - B) * (1 - v / top)
    out = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" font-family="Helvetica, Arial, sans-serif">',
           f'<rect width="{width}" height="{height}" fill="#fbf9f4"/>']
    seen = set()
    for x, r in zip(xs, rows):
        day = r[0].strftime("%m-%d")
        if day in seen:
            continue
        seen.add(day)
        out.append(f'<line x1="{x:.1f}" y1="{T}" x2="{x:.1f}" y2="{height - B}" stroke="#ccc" stroke-dasharray="3 3"/>')
        out.append(f'<text x="{x + 3:.1f}" y="{height - B + 18}" font-size="12" fill="#444">{day}</text>')
    out.append('<polyline points="' + " ".join(f"{x:.1f},{ty(c):.1f}" for x, c in zip(xs, counts)) + '" fill="none" stroke="#002145" stroke-width="2"/>')
    for v in range(0, int(top) + 1, max(1, int(top / 5))):
        out.append(f'<text x="{L - 8}" y="{ty(v) + 4:.1f}" font-size="12" text-anchor="end" fill="#444">{v}</text>')
    total = sum(counts)
    out.append(f'<text x="{L}" y="28" font-size="18" font-weight="700" fill="#002145">GDELT, {title_who} per hour, last {DAYS} days from {SOURCE}: {total:,} events, query {ms / 1000:.1f} s</text>')
    out.append(f'<text x="{width - R}" y="{height - 8}" font-size="11" text-anchor="end" fill="#888">one GDELT file every 15 minutes; drawn from the {SOURCE} files by DuckDB over S3; add ?country=CA for one country</text>')
    out.append("</svg>")
    return "\n".join(out)


def lambda_handler(event, context):
    global CON, CALLS
    CALLS += 1
    if CON is None:
        CON = connect()
    country = (event.get("queryStringParameters") or {}).get("country")
    t0 = time.perf_counter()
    rows = per_hour(CON, country)
    ms = (time.perf_counter() - t0) * 1000
    print(json.dumps({"source": SOURCE, "query_ms": round(ms), "hours": len(rows), "events": sum(r[1] for r in rows)}))
    return {"statusCode": 200,
            "headers": {"Content-Type": "image/svg+xml", "Cache-Control": "no-store", "X-Source": SOURCE,
                        "X-Query-Ms": str(round(ms)), "X-Cold-Start": "1" if CALLS == 1 else "0"},
            "body": svg(rows, country, ms)}
