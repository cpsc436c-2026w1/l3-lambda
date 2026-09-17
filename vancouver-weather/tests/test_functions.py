"""Tests that run without AWS: the parser, the merge and the charts are pure functions."""
import importlib.util
import os
from pathlib import Path

os.environ.setdefault("BUCKET", "test-bucket")
ROOT = Path(__file__).resolve().parents[1]


def load(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / name / "handler.py")
    mod = importlib.util.module_from_spec(spec); spec.loader.exec_module(mod); return mod


os.environ.setdefault("SOURCE", "parquet")
pull, compact, chart = load("pull"), load("compact"), load("charts")

CUR = {"time": "2026-09-16T19:30", "interval": 900, "temperature_2m": 18.0, "relative_humidity_2m": 60,
       "precipitation": 0.0, "wind_speed_10m": 7.8, "weather_code": 0}
SAMPLE = [{"latitude": lat, "longitude": lon, "current": dict(CUR, temperature_2m=18.0 + i)} for i, (_, lat, lon) in enumerate(pull.CITIES)]


def test_rows_from_open_meteo_one_per_city():
    rows = pull.rows_from(SAMPLE)
    assert [r["city"] for r in rows] == [c[0] for c in pull.CITIES]
    assert list(rows[0]) == pull.FIELDS and rows[-1]["temperature_c"] == 27.0
    assert "Bergen" in [r["city"] for r in rows] and "Reykjavik" in [r["city"] for r in rows]


def test_merge_keeps_one_row_per_city_and_time():
    import pandas as pd
    base = {"humidity_pct": 60, "precipitation_mm": 0.0, "wind_kmh": 5.0, "weather_code": 0}
    old = pd.DataFrame([dict(base, city="Vancouver", time="2026-09-16T19:15", temperature_c=17.0)])
    new = pd.DataFrame([dict(base, city="Vancouver", time="2026-09-16T19:15", temperature_c=17.0),   # repeat
                        dict(base, city="Bergen", time="2026-09-16T19:15", temperature_c=11.0),      # same time, other city
                        dict(base, city="Vancouver", time="2026-09-16T19:30", temperature_c=18.0)])
    m = compact.merge(old, new)
    assert len(m) == 3 and list(m.columns) == compact.COLS
    assert m["time"].is_monotonic_increasing


def test_chart_has_one_point_per_reading_and_a_bar_per_wet_reading():
    rows = [(f"2026-09-{15 + i // 24:02d}T{i % 24:02d}:00", 10 + (i % 7), 0.4 if i % 5 == 0 else 0.0) for i in range(48)]
    svg = chart.svg_chart(rows, "Bergen")
    assert svg.startswith("<svg") and svg.endswith("</svg>")
    assert svg.count('fill="#f2b134"') == sum(1 for r in rows if r[2] > 0)
    assert "Bergen, last" in svg and "48 readings" in svg


def test_chart_with_no_data_still_renders():
    assert "No data yet for Whistler" in chart.svg_chart([], "Whistler")


def test_all_cities_chart_has_a_line_and_a_legend_entry_per_city():
    series = {c: [(f"2026-09-16T{h:02d}:00", 10 + k + h % 3) for h in range(8)] for k, c in enumerate(["Bergen", "Calgary", "Vancouver"])}
    svg = chart.svg_all(series)
    assert svg.count("<polyline") == 3 and "Vancouver" in svg and "24 readings" in svg
