# Two serverless data pipelines on AWS

Two pipelines with the same shape. Each calls a public feed on a schedule, appends what it gets
to one CSV file per day in an S3 bucket, turns those CSVs into Parquet when they are written, and
draws a chart on request. One works on kilobytes of weather readings, the other on tens of
megabytes of world event records.

## See them running

| what | link |
|---|---|
| Ten cities, drawn from Parquet | https://cbk4nglccf.execute-api.ca-central-1.amazonaws.com/ |
| Ten cities, drawn from the day CSVs | https://m8vzkaileg.execute-api.ca-central-1.amazonaws.com/ |
| One city | add `?city=Bergen` to either link |
| SQL over the weather Parquet | https://gzryvhi8xk.execute-api.ca-central-1.amazonaws.com/sql?q=SELECT%20count(*)%20FROM%20weather |
| World events by the hour, from one Parquet file | https://8miqcchy42.execute-api.ca-central-1.amazonaws.com/?country=CA |
| The same, from one Parquet file per day | https://ku5utxo679.execute-api.ca-central-1.amazonaws.com/?country=CA |
| The same, read from the raw CSVs | https://z6wvgn28c5.execute-api.ca-central-1.amazonaws.com/?country=CA |
| GDELT dashboard | https://436c-2026w1-public.s3.ca-central-1.amazonaws.com/dashboards/gdelt.html |

The three world-event links run the same query over the same rows in three layouts, so the time
each takes is the difference between them.

## `vancouver-weather/`

Ten cities, one row per city every 15 minutes.

| function | what it does |
|---|---|
| `pull` | A schedule fires it every 15 minutes. One call to Open-Meteo for all ten cities, and the rows are appended to today's CSV. |
| `compact` | Runs when a CSV is written. Reads the Parquet file and the day's CSVs with pandas, keeps one row per city and reading time, writes the Parquet file back. |
| `compact-duckdb` | The same job as one DuckDB query, reading and writing over `s3://` through the httpfs extension. |
| `chart-csv` | Answers an HTTP request by querying the day CSVs and returning an SVG. |
| `chart-parquet` | The same chart from the compacted Parquet file. |
| `duck` | A small SQL endpoint over the weather Parquet: `/daily`, `/chart?city=`, and `/sql?q=` for one read-only SELECT. |

## `gdelt-events/`

Every event GDELT publishes, 61 columns wide, about 45 MB a day.

| function | what it does |
|---|---|
| `pull` | A schedule fires it every 15 minutes. Fetches the newest GDELT export and appends its rows to today's CSV. |
| `compact` | Rewrites the whole history into one Parquet file with pandas on every CSV write. |
| `compact-duckdb` | The same whole-file job as one DuckDB query. |
| `compact-daily` | Writes one Parquet file per day under `day=YYYY-MM-DD` and reads no other day. |
| `chart-csv`, `chart-parquet`, `chart-daily` | The same query and the same picture, over the raw CSVs, the single Parquet file, and the day folder. |

Each folder also holds `queries/` with the SQL the functions run, `deploy.sh`, and `teardown.sh`.

## Where the data comes from

- Open-Meteo forecast API, no key needed: https://open-meteo.com/en/docs
- GDELT 2.0 event files, a new one every 15 minutes: https://www.gdeltproject.org/data.html
- The GDELT index the pull reads: http://data.gdeltproject.org/gdeltv2/lastupdate.txt
