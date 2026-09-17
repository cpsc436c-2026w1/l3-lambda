-- charts/handler.py, no ?city=: every city's temperature for the last 7 days.
SELECT city, strftime(CAST(time AS TIMESTAMP), '%Y-%m-%dT%H:%M') AS time, temperature_c
FROM {source}   -- read_parquet('.../cities-duckdb.parquet') or read_csv([the day files])
WHERE CAST(time AS TIMESTAMP) >= now() - INTERVAL 7 DAY
QUALIFY row_number() OVER (PARTITION BY city, time) = 1
ORDER BY city, time;
