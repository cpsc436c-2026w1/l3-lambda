# Ten cities' weather, a serverless pipeline that keeps running

Six AWS Lambda functions in ca-central-1, one private Amazon S3 bucket, deployed 2026-09-15, ten cities
since 2026-09-16 (Vancouver, Victoria, Kelowna, Prince George, Whistler, Toronto, Montreal, Calgary,
Bergen, Reykjavik). The dataset grows by ten rows every 15 minutes.

| Piece | Trigger | What it does |
|---|---|---|
| `436c-weather-pull` (256 MB, no layer) | timer, `rate(15 minutes)`, Open-Meteo's refresh cadence | one call for all ten cities (keyless, 96 calls a day against a 10,000 limit), appends ten rows to `weather/cities/YYYY-MM-DD.csv`, UTC |
| `436c-weather-compact` (1,024 MB, AWS SDK for pandas layer) | the CSV write, through EventBridge | pandas: today's and yesterday's CSVs into `weather/cities-pandas.parquet`, one row per city and reading time |
| `436c-weather-compact-duckdb` (1,024 MB, our DuckDB layer) | the same event | DuckDB: the same job into `weather/cities-duckdb.parquet`, reading and writing `s3://` itself through the httpfs extension the layer carries (no boto3, the function's own credentials go into a `CREATE SECRET`). Its first call in a fresh container pays about 5 s to load the 42 MB extension, warm calls about 0.45 s at 250 rows |
| `436c-weather-chart-csv` (1,024 MB, DuckDB layer) | HTTP API (`?city=Bergen`) and an hourly timer | draws one city's last seven days straight from the CSV day files. No `?city=` gives all ten cities on one chart, which the timer pre-renders to `charts/all-cities-csv.svg` |
| `436c-weather-chart-parquet` (same code, `SOURCE=parquet`) | HTTP API and an hourly timer | the same chart from `cities-duckdb.parquet`. Same engine, same SQL, only the file format differs |
| `436c-weather-duck` (1,024 MB, DuckDB layer) | HTTP API: `/daily?city=`, `/chart?city=`, `/sql?q=SELECT …` | a SQL playground over the DuckDB pipeline's file |

S3 refuses two Lambda notifications with overlapping filters, so the bucket publishes object events to
Amazon EventBridge and two rules match the same event, one per compaction.

- Bucket: `s3://436c-2026w1-weather/` (private). Raw CSVs under `weather/cities/`, two growing Parquet
  files, `weather/cities-pandas.parquet` and `weather/cities-duckdb.parquet`, one per engine. The pandas
  chart reads the pandas file, the DuckDB functions read the DuckDB file, the last pre-rendered chart sits
  under `charts/`, and `weather/vancouver*` is the first day's Vancouver-only data, kept.
- Chart from the CSV day files:
  <https://m8vzkaileg.execute-api.ca-central-1.amazonaws.com/?city=Vancouver>
- Chart from the Parquet file:
  <https://cbk4nglccf.execute-api.ca-central-1.amazonaws.com/?city=Vancouver>
  Both answer `image/svg+xml` with no caching. No `?city=` gives all ten cities.
- SQL playground: <https://gzryvhi8xk.execute-api.ca-central-1.amazonaws.com/daily>, also
  `/chart?city=Vancouver` and `/sql?q=SELECT ...`.
- The read functions keep their files in `/tmp` across warm calls and re-fetch only when an ETag changes,
  so a warm call is one HEAD request per file.
- Role: `436c-weather-demo-role`, basic execution plus get and put under `weather/*` and `charts/*` and
  list on the bucket. Nothing else.
- The compactions are triggered by writes under `weather/cities/` and write under `weather/`, never under
  the CSV prefix, so nothing invokes itself.

## The three trigger types, one example each

Schedule (the pull and the hourly pre-renders), event (the compactions on the CSV write), request (the
charts and the SQL playground through the HTTP API).

## Queries

`queries/` holds the SQL the functions run, as files that can be read and tried on a laptop without the
Python around them. `chart_one_city.sql` and `chart_all_cities.sql` are the two queries in
`charts/handler.py`, with `{source}` standing for the `read_parquet` or `read_csv` call the environment
variable chooses. `compact_merge.sql` is the `COPY` statement in `compact-duckdb/handler.py`.
`probe_threads.sql` holds the two CPU probes used to measure how DuckDB parallelises.

## What the numbers looked like

- The pull function has no layer and shows Init 0.46 s on its first call. The chart and compact functions
  carry pandas and pyarrow and show Init 2.17 s each (measured 2026-09-15, from the REPORT lines in
  CloudWatch Logs). Same account, same runtime, and the package is the difference.
- The DuckDB compaction shows the same rule inside one call. Its httpfs extension (42 MB) loads on the
  first call of a container, about 5 s, and never again while the container lives, so warm calls take
  0.45 s with one connection per container.
- Scale to zero: the invocation graph is a tick every 15 minutes and nothing between, while the bucket
  bills for a few kilobytes a day whether anyone looks or not.
- Freshness: the pre-rendered chart is at most an hour behind the CSV, and the CSV at most ten minutes
  behind the sky.

## Run locally

`uv run --with boto3 --with pytest --with pandas --with pyarrow python -m pytest -q tests` runs the tests.
The parser, the merge and the chart are pure functions, so no AWS account is needed.
`./deploy.sh` re-deploys code and configuration, and `./teardown.sh` removes everything but the bucket.

## Cost

About 4,300 pull calls a month, 720 compactions and a few hundred charts come to cents. S3 holds
kilobytes. The first million HTTP API calls a month are free for the first twelve months of an account.
