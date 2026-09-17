"""Chart function, deployed twice: once reading the raw CSV day files, once reading the Parquet.

SOURCE=csv      reads today's and yesterday's `weather/cities/YYYY-MM-DD.csv` (and the days before,
                up to DAYS) with DuckDB's read_csv
SOURCE=parquet  reads `weather/cities-duckdb.parquet` with read_parquet

Same engine, same SQL, same picture; only the source differs, so the difference in duration is
the file format. Two triggers: an HTTP API returns the SVG;
an hourly EventBridge rule with input {"prerender": true} writes charts/all-cities-<source>.svg.
No ?city= gives all ten cities on one chart; ?city=Bergen gives one city with its rain. Files are fetched once per execution environment and re-fetched when their ETag changes.
"""
import json
import os
from datetime import datetime, timedelta, timezone

import boto3
import duckdb

BUCKET = os.environ["BUCKET"]
SOURCE = os.environ.get("SOURCE", "parquet")
PREFIX = os.environ.get("PREFIX", "weather/cities")
PARQUET_KEY = os.environ.get("PARQUET_KEY_DUCKDB", "weather/cities-duckdb.parquet")
DAYS = int(os.environ.get("DAYS", "7"))

s3 = boto3.client("s3")
CALLS = 0
ETAGS: dict[str, str] = {}                      # key -> etag of the copy in /tmp


def local(key: str) -> str | None:
    """Fetch the object to /tmp if it changed since the last call; None if it does not exist."""
    path = "/tmp/" + key.replace("/", "_")
    try:
        etag = s3.head_object(Bucket=BUCKET, Key=key)["ETag"]
    except s3.exceptions.ClientError:
        return None
    if ETAGS.get(key) != etag:
        s3.download_file(BUCKET, key, path)
        ETAGS[key] = etag
    return path


def source_sql() -> str:
    if SOURCE == "parquet":
        p = local(PARQUET_KEY)
        return f"read_parquet('{p}')" if p else "(SELECT NULL AS city, NULL AS time, NULL AS temperature_c, NULL AS precipitation_mm WHERE false)"
    paths = [local(f"{PREFIX}/{(datetime.now(timezone.utc) - timedelta(days=d)):%Y-%m-%d}.csv") for d in range(DAYS)]
    paths = [p for p in paths if p]
    if not paths:
        return "(SELECT NULL AS city, NULL AS time, NULL AS temperature_c, NULL AS precipitation_mm WHERE false)"
    return f"read_csv({json.dumps(paths)}, header=true, timestampformat='%Y-%m-%dT%H:%M', union_by_name=true)"


def readings(city: str) -> list[tuple]:
    con = duckdb.connect()
    return con.execute(f"""
        SELECT strftime(CAST(time AS TIMESTAMP), '%Y-%m-%dT%H:%M'), temperature_c, precipitation_mm
        FROM {source_sql()}
        WHERE city = ? AND CAST(time AS TIMESTAMP) >= now() - INTERVAL {DAYS} DAY
        QUALIFY row_number() OVER (PARTITION BY time) = 1
        ORDER BY time""", [city]).fetchall()


def readings_all() -> dict[str, list[tuple]]:
    """city -> [(time, temperature)] for every city, last DAYS days."""
    con = duckdb.connect()
    rows = con.execute(f"""
        SELECT city, strftime(CAST(time AS TIMESTAMP), '%Y-%m-%dT%H:%M'), temperature_c
        FROM {source_sql()}
        WHERE CAST(time AS TIMESTAMP) >= now() - INTERVAL {DAYS} DAY
        QUALIFY row_number() OVER (PARTITION BY city, time) = 1
        ORDER BY city, time""").fetchall()
    out: dict[str, list[tuple]] = {}
    for city, t, temp in rows:
        out.setdefault(city, []).append((t, temp))
    return out


PALETTE = ["#002145", "#f2b134", "#2b7a3c", "#9a4a17", "#6a4c93", "#c0392b", "#1f77b4", "#7f7f7f", "#e377c2", "#17becf"]


def svg_all(series: dict[str, list[tuple]], width: int = 900, height: int = 380) -> str:
    """Every city's temperature as a line on one time axis, legend on the right. Pure SVG."""
    if not series:
        return f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}"><text x="20" y="40" font-family="Helvetica" font-size="18">No data yet</text></svg>'
    L, R, T, B = 60, 150, 50, 50
    times = sorted({t for pts in series.values() for t, _ in pts})
    tx = {t: L + (width - L - R) * (i / max(len(times) - 1, 1)) for i, t in enumerate(times)}
    temps = [v for pts in series.values() for _, v in pts]
    lo, hi = min(temps) - 1, max(temps) + 1
    ty = lambda v: T + (height - T - B) * (1 - (v - lo) / (hi - lo))
    out = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" font-family="Helvetica, Arial, sans-serif">',
           f'<rect width="{width}" height="{height}" fill="#fbf9f4"/>']
    seen = set()
    for t in times:
        day = t[:10]
        if day in seen:
            continue
        seen.add(day)
        out.append(f'<line x1="{tx[t]:.1f}" y1="{T}" x2="{tx[t]:.1f}" y2="{height - B}" stroke="#ccc" stroke-dasharray="3 3"/>')
        out.append(f'<text x="{tx[t] + 3:.1f}" y="{height - B + 18}" font-size="12" fill="#444">{day[5:]}</text>')
    for k, (city, pts) in enumerate(sorted(series.items())):
        colour = PALETTE[k % len(PALETTE)]
        out.append('<polyline points="' + " ".join(f"{tx[t]:.1f},{ty(v):.1f}" for t, v in pts) + f'" fill="none" stroke="{colour}" stroke-width="2"/>')
        out.append(f'<rect x="{width - R + 10}" y="{T + 18 * k - 9}" width="12" height="12" fill="{colour}"/>')
        out.append(f'<text x="{width - R + 28}" y="{T + 18 * k + 2}" font-size="13" fill="#222">{city} {pts[-1][1]:.0f}°</text>')
    for v in range(int(lo) + 1, int(hi) + 1, max(1, int((hi - lo) / 6))):
        out.append(f'<text x="{L - 8}" y="{ty(v) + 4:.1f}" font-size="12" text-anchor="end" fill="#444">{v}°C</text>')
    n = sum(len(p) for p in series.values())
    out.append(f'<text x="{L}" y="28" font-size="18" font-weight="700" fill="#002145">Ten cities, last {DAYS} days, from {SOURCE}: {n} readings, latest {times[-1].replace("T", " ")} UTC</text>')
    out.append(f'<text x="{L}" y="{height - 8}" font-size="11" fill="#888">temperature per city; Open-Meteo every 15 minutes; drawn from the {SOURCE} files by DuckDB; add ?city=Bergen for one city with rain</text>')
    out.append("</svg>")
    return "\n".join(out)


def svg_chart(rows: list[tuple], city: str, width: int = 900, height: int = 340) -> str:
    if not rows:
        return f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}"><text x="20" y="40" font-family="Helvetica" font-size="18">No data yet for {city}</text></svg>'
    L, R, T, B = 60, 20, 50, 50
    n = len(rows)
    xs = [L + (width - L - R) * (i / max(n - 1, 1)) for i in range(n)]
    temps = [float(r[1]) for r in rows]
    rain = [float(r[2]) for r in rows]
    tmin, tmax = min(temps) - 1, max(temps) + 1
    rmax = max(max(rain), 1.0)
    def ty(t): return T + (height - T - B) * (1 - (t - tmin) / (tmax - tmin))
    def ry(mm): return (height - B) - (height - T - B) * 0.5 * (mm / rmax)
    out = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" font-family="Helvetica, Arial, sans-serif">',
           f'<rect width="{width}" height="{height}" fill="#fbf9f4"/>']
    bw = max((width - L - R) / n * 0.8, 1)
    for x, mm in zip(xs, rain):
        if mm > 0:
            out.append(f'<rect x="{x - bw/2:.1f}" y="{ry(mm):.1f}" width="{bw:.1f}" height="{(height - B) - ry(mm):.1f}" fill="#f2b134" opacity="0.85"/>')
    out.append('<polyline points="' + " ".join(f"{x:.1f},{ty(t):.1f}" for x, t in zip(xs, temps)) + '" fill="none" stroke="#002145" stroke-width="2.5"/>')
    seen = set()
    for x, r in zip(xs, rows):
        day = r[0][:10]
        if day in seen:
            continue
        seen.add(day)
        out.append(f'<line x1="{x:.1f}" y1="{T}" x2="{x:.1f}" y2="{height - B}" stroke="#ccc" stroke-dasharray="3 3"/>')
        out.append(f'<text x="{x + 3:.1f}" y="{height - B + 18}" font-size="12" fill="#444">{day[5:]}</text>')
    for t in range(int(tmin) + 1, int(tmax) + 1, max(1, int((tmax - tmin) / 5))):
        out.append(f'<text x="{L - 8}" y="{ty(t) + 4:.1f}" font-size="12" text-anchor="end" fill="#444">{t}°C</text>')
    last = rows[-1]
    out.append(f'<text x="{L}" y="28" font-size="18" font-weight="700" fill="#002145">{city}, last {DAYS} days from {SOURCE}: {last[1]} °C at {last[0].replace("T", " ")} UTC, {n} readings, {sum(rain):.1f} mm of rain</text>')
    out.append(f'<text x="{width - R}" y="{height - 8}" font-size="11" text-anchor="end" fill="#888">temperature line, precipitation bars (mm per reading); Open-Meteo every 15 minutes; drawn from the {SOURCE} files by DuckDB</text>')
    out.append("</svg>")
    return "\n".join(out)


def lambda_handler(event, context):
    global CALLS
    CALLS += 1
    if event.get("prerender"):                  # the hourly rule: all cities
        key = f"charts/all-cities-{SOURCE}.svg"
        s3.put_object(Bucket=BUCKET, Key=key, Body=svg_all(readings_all()).encode(), ContentType="image/svg+xml")
        print(json.dumps({"rendered": key}))
        return {"rendered": key}
    city = (event.get("queryStringParameters") or {}).get("city")
    body = svg_chart(readings(city), city) if city else svg_all(readings_all())
    return {"statusCode": 200,
            "headers": {"Content-Type": "image/svg+xml", "Cache-Control": "no-store",
                        "X-Cold-Start": "1" if CALLS == 1 else "0", "X-Env-Calls": str(CALLS), "X-Source": SOURCE},
            "body": body}
