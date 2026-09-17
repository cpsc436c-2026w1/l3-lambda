"""DuckDB alternative: the same growing Parquet file queried with SQL, on request.

Routes (HTTP API):
  /daily?city=   one JSON row per city and day (all cities unless ?city=): mean, min, max, rain, readings
  /chart?city=   the daily series for one city as SVG (mean line, min-to-max bar, rain)
  /sql?q=        a read-only SELECT over the view `weather` (the room writes the query)

The Parquet file is downloaded to /tmp once per execution environment and re-downloaded only
when its ETag changes; the DuckDB connection is kept too. A warm call is one HEAD and one query.
"""
import json
import os

import boto3
import duckdb

BUCKET = os.environ["BUCKET"]
PARQUET_KEY = os.environ.get("PARQUET_KEY_DUCKDB", "weather/cities-duckdb.parquet")   # the DuckDB pipeline's file
LOCAL = "/tmp/weather.parquet"

s3 = boto3.client("s3")
CALLS = 0                                       # per execution environment; 1 on a cold start
CACHE = {"etag": None, "con": None}             # kept across warm calls

DAILY = """
SELECT city, CAST(time AS DATE) AS day,
       round(avg(temperature_c), 1) AS mean_c, round(min(temperature_c), 1) AS min_c,
       round(max(temperature_c), 1) AS max_c, round(sum(precipitation_mm), 1) AS rain_mm,
       count(*) AS readings
FROM weather {where} GROUP BY city, day ORDER BY city, day
"""


def connect():
    etag = s3.head_object(Bucket=BUCKET, Key=PARQUET_KEY)["ETag"]
    if etag != CACHE["etag"]:
        s3.download_file(BUCKET, PARQUET_KEY, LOCAL)
        con = duckdb.connect()
        con.execute(f"CREATE VIEW weather AS SELECT * FROM read_parquet('{LOCAL}')")
        CACHE["con"], CACHE["etag"] = con, etag
    return CACHE["con"]


def rows(con, sql, params=()):
    cur = con.execute(sql, params)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def svg_daily(days: list[dict], city: str, width=900, height=340) -> str:
    if not days:
        return f'<svg xmlns="http://www.w3.org/2000/svg" width="900" height="340"><text x="20" y="40" font-family="Helvetica" font-size="18">No data yet for {city}</text></svg>'
    L, R, T, B = 60, 20, 50, 50
    n = len(days)
    xs = [L + (width - L - R) * ((i + 0.5) / n) for i in range(n)]
    lo = min(d["min_c"] for d in days) - 1; hi = max(d["max_c"] for d in days) + 1
    rmax = max(max(d["rain_mm"] for d in days), 1.0)
    ty = lambda t: T + (height - T - B) * (1 - (t - lo) / (hi - lo))
    ry = lambda mm: (height - B) - (height - T - B) * 0.5 * (mm / rmax)
    bw = (width - L - R) / n * 0.7
    out = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" font-family="Helvetica, Arial, sans-serif">',
           f'<rect width="{width}" height="{height}" fill="#fbf9f4"/>']
    for x, d in zip(xs, days):
        if d["rain_mm"] > 0:
            out.append(f'<rect x="{x - bw/2:.1f}" y="{ry(d["rain_mm"]):.1f}" width="{bw:.1f}" height="{(height - B) - ry(d["rain_mm"]):.1f}" fill="#f2b134" opacity="0.85"/>')
        out.append(f'<line x1="{x:.1f}" y1="{ty(d["min_c"]):.1f}" x2="{x:.1f}" y2="{ty(d["max_c"]):.1f}" stroke="#9a4a17" stroke-width="3"/>')
        out.append(f'<text x="{x:.1f}" y="{height - B + 18}" font-size="12" text-anchor="middle" fill="#444">{str(d["day"])[5:]}</text>')
    pts = " ".join(f"{x:.1f},{ty(d['mean_c']):.1f}" for x, d in zip(xs, days))
    out.append(f'<polyline points="{pts}" fill="none" stroke="#002145" stroke-width="2.5"/>')
    for t in range(int(lo) + 1, int(hi) + 1, max(1, int((hi - lo) / 5))):
        out.append(f'<text x="{L - 8}" y="{ty(t) + 4:.1f}" font-size="12" text-anchor="end" fill="#444">{t}°C</text>')
    total = sum(d["readings"] for d in days)
    out.append(f'<text x="{L}" y="28" font-size="18" font-weight="700" fill="#002145">{city} by day: mean line, min to max bar, rain in amber; {n} days, {total} readings, via DuckDB</text>')
    out.append("</svg>")
    return "\n".join(out)


def lambda_handler(event, context):
    global CALLS
    CALLS += 1
    path = (event.get("rawPath") or "/").rstrip("/") or "/daily"
    qs = event.get("queryStringParameters") or {}
    city = qs.get("city")
    headers = {"X-Cold-Start": "1" if CALLS == 1 else "0", "X-Env-Calls": str(CALLS), "Cache-Control": "no-store"}
    con = connect()
    where, params = ("WHERE city = ?", (city,)) if city else ("", ())
    if path == "/chart":
        body, ctype = svg_daily(rows(con, DAILY.format(where="WHERE city = ?"), (city or "Vancouver",)), city or "Vancouver"), "image/svg+xml"
    elif path == "/sql":
        q = (qs.get("q") or "SELECT city, count(*) AS readings FROM weather GROUP BY city ORDER BY city").strip()
        if not q.lower().startswith("select") or ";" in q:
            return {"statusCode": 400, "headers": headers, "body": "one SELECT, no semicolons"}
        body, ctype = json.dumps(rows(con, q + " LIMIT 500"), default=str), "application/json"
    else:
        body, ctype = json.dumps(rows(con, DAILY.format(where=where), params), default=str), "application/json"
    headers["Content-Type"] = ctype
    return {"statusCode": 200, "headers": headers, "body": body}
