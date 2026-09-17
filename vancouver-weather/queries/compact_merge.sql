-- compact-duckdb/handler.py runs this on every CSV write, all over s3:// through httpfs.
-- {day_files} is the list of today's and yesterday's day files.
COPY (
  SELECT city, CAST(time AS TIMESTAMP) AS time,
         CAST(temperature_c AS DOUBLE) AS temperature_c,
         CAST(humidity_pct AS DOUBLE) AS humidity_pct,
         CAST(precipitation_mm AS DOUBLE) AS precipitation_mm,
         CAST(wind_kmh AS DOUBLE) AS wind_kmh,
         CAST(weather_code AS BIGINT) AS weather_code
  FROM (
    SELECT * FROM read_csv({day_files}, header=true)      -- the new rows, as text
    UNION ALL BY NAME                                     -- plus the whole history so far
    SELECT * FROM read_parquet('s3://436c-2026w1-weather/weather/cities-duckdb.parquet')
  )
  QUALIFY row_number() OVER (PARTITION BY city, time) = 1  -- one row per city and time
  ORDER BY time, city
) TO 's3://436c-2026w1-weather/weather/cities-duckdb.parquet' (FORMAT PARQUET);
