-- compact-daily/handler.py, as one query: the day file the event names goes to its own folder.
-- The deployed function does the same in pandas: read one CSV, drop duplicate ids, write it.
COPY (
  SELECT * FROM read_csv('s3://436c-2026w1-weather/gdelt/events/2026-09-17.csv',
                         header=true, all_varchar=true)
  QUALIFY row_number() OVER (PARTITION BY GlobalEventID ORDER BY DATEADDED DESC) = 1
) TO 's3://436c-2026w1-weather/gdelt/events-daily/day=2026-09-17/part.parquet'
  (FORMAT PARQUET);
-- No other day is read, so memory is one day, whatever the history holds.
