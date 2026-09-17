-- charts/handler.py, ?city=: one city's readings for the last DAYS days; {source} is read_parquet(...) or read_csv([...]).
SELECT strftime(CAST(time AS TIMESTAMP), '%Y-%m-%dT%H:%M') AS time, temperature_c, precipitation_mm
FROM {source}
WHERE city = ? AND CAST(time AS TIMESTAMP) >= now() - INTERVAL 7 DAY
QUALIFY row_number() OVER (PARTITION BY time) = 1
ORDER BY time;
