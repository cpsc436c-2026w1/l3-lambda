-- compact-duckdb/handler.py: fold the day file the event names into the whole-file Parquet (keeps the old rows,
-- adds the event ids not seen before). Rewrites the entire history every run: this is the layout that fails.
COPY (
  SELECT * FROM read_parquet('s3://436c-2026w1-weather/gdelt/events-duckdb.parquet')
  UNION ALL BY NAME
  SELECT * FROM (
    SELECT * REPLACE (TRY_CAST(GlobalEventID AS BIGINT) AS GlobalEventID, TRY_CAST(AvgTone AS DOUBLE) AS AvgTone /* ... the 15 numeric columns */)
    FROM read_csv(['s3://436c-2026w1-weather/gdelt/events/2026-09-16.csv'], header=true, all_varchar=true, union_by_name=true)
  ) n
  WHERE n.GlobalEventID NOT IN (SELECT GlobalEventID FROM read_parquet('s3://436c-2026w1-weather/gdelt/events-duckdb.parquet'))
) TO 's3://436c-2026w1-weather/gdelt/events-duckdb.parquet' (FORMAT PARQUET);
