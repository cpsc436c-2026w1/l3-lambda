-- charts/handler.py: events per hour for the last 7 days, optionally one country. {source} is one of
--   read_csv([day files], header=true, all_varchar=true, union_by_name=true)                      -- SOURCE=csv, every byte read
--   read_parquet('s3://436c-2026w1-weather/gdelt/events-duckdb.parquet')                            -- SOURCE=parquet, three columns read
--   (SELECT * FROM read_parquet('s3://436c-2026w1-weather/gdelt/events-daily/*/*.parquet', hive_partitioning=true) WHERE day >= '2026-09-10')  -- SOURCE=daily
SELECT date_trunc('hour', strptime(CAST(DATEADDED AS VARCHAR), '%Y%m%d%H%M%S')) AS h, count(*) AS n, avg(TRY_CAST(AvgTone AS DOUBLE)) AS tone
FROM {source}
WHERE strptime(CAST(DATEADDED AS VARCHAR), '%Y%m%d%H%M%S') >= now() - INTERVAL 7 DAY
  AND ActionGeo_CountryCode = 'CA'
GROUP BY 1 ORDER BY 1;
