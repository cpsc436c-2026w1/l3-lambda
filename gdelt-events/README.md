# GDELT events: the weather pipeline's layout on 45 MB day files

The same pieces as [`../vancouver-weather/`](../vancouver-weather/), pointed at a source that publishes a
new file every 15 minutes at real scale. GDELT 2.0's event export carries about 1,200 events and 61
columns per quarter hour, tab-separated, zipped, and needs no key. Same bucket, same role, its own prefix.

| function | trigger | does |
|---|---|---|
| `436c-gdelt-pull` (512 MB, no layer) | timer, `rate(15 minutes)` | reads GDELT's pointer file, downloads the newest export, appends its rows to `gdelt/events/YYYY-MM-DD.csv` (UTC). A day file grows to about 45 MB. An event `{"day": "YYYY-MM-DD"}` backfills a whole past day in one call |
| `436c-gdelt-compact` (1,024 MB, pandas layer) | the CSV write, through EventBridge | pandas: the day file the event names and the existing Parquet, one row per event id, back to `gdelt/events-pandas.parquet` |
| `436c-gdelt-compact-duckdb` (1,024 MB, DuckDB layer) | the same event | DuckDB over `s3://` through httpfs: the same rule into `gdelt/events-duckdb.parquet`, 600 MB memory limit, spills to `/tmp` |
| `436c-gdelt-compact-daily` (1,024 MB, pandas layer) | the same event | pandas again, one Parquet per day: the day file the event names becomes `gdelt/events-daily/day=YYYY-MM-DD/part.parquet`. No history is read, so memory is bounded by one day |
| `436c-gdelt-chart-csv` (1,024 MB, DuckDB layer) | HTTP API (`?country=CA`) | events per hour, last seven days, `read_csv` over the day files on `s3://` |
| `436c-gdelt-chart-parquet` (same code, `SOURCE=parquet`) | HTTP API | the same query over the DuckDB Parquet file, which fetches only the three columns the query touches |
| `436c-gdelt-chart-daily` (same code, `SOURCE=daily`) | HTTP API | the same query over the day folder with hive partitioning, where the filter on `day` skips whole files |

All three compactions have reserved concurrency 1, because two runs of one engine must not race on the
same Parquet file. Events that arrive while one runs wait in Lambda's asynchronous invocation queue.

- Chart from the CSV day files:
  <https://z6wvgn28c5.execute-api.ca-central-1.amazonaws.com/?country=CA>
- Chart from the whole-history Parquet file:
  <https://8miqcchy42.execute-api.ca-central-1.amazonaws.com/?country=CA>
- Chart from the per-day Parquet folder:
  <https://ku5utxo679.execute-api.ca-central-1.amazonaws.com/?country=CA>
- The Quarto dashboard built from the day files:
  <https://436c-2026w1-public.s3.ca-central-1.amazonaws.com/dashboards/gdelt.html>

Every chart response carries an `X-Query-Ms` header with the time the query itself took, and an
`X-Cold-Start` header saying whether this was the first call in its execution environment.

## Why it exists

The weather pipeline's files are kilobytes, and at that size every engine and format costs the same few
hundred milliseconds. This pipeline keeps the layout and changes only the size of the rows, so what
changes is what the shape of the data does. A merge that happens wholly in memory now runs against the
memory setting, a query that must read every byte of a CSV file now differs from one that reads three
columns of a Parquet file, and a compaction that re-reads a 45 MB day file rewrites the whole Parquet
every quarter hour.

## Queries

`queries/` holds the SQL the functions run, as files that can be read on their own.
`chart_events_per_hour.sql` is the single query in `charts/handler.py`, with a comment showing the three
things `{source}` becomes for `SOURCE=csv`, `SOURCE=parquet` and `SOURCE=daily`. `compact_whole_file.sql`
is the `COPY` statement in `compact-duckdb/handler.py`, the one that rewrites the entire history on every
run. `probe_threads_our_data.sql` is the thread probe run against the whole-file Parquet, plus the query
that counts its row groups.

## Run

```
./deploy.sh                                   # functions, timer, rules, APIs
aws lambda invoke --function-name 436c-gdelt-pull --region ca-central-1 \
  --cli-binary-format raw-in-base64-out --payload '{"day":"2026-09-10"}' /dev/stdout   # backfill one day
./teardown.sh                                 # functions, rules, APIs, keeps the data
```

Watch it in the two compactions' REPORT lines in CloudWatch Logs (`/aws/lambda/436c-gdelt-compact` and
`-compact-duckdb`), where Duration and Max Memory Used can be read against the Parquet's row count. The
columns of the source are named in the GDELT 2.0 event codebook
(<http://data.gdeltproject.org/documentation/GDELT-Event_Codebook-V2.0.pdf>).

## Measured 2026-09-16, seven days backfilled, 702,618 rows

| | pandas, whole file, 1,024 MB | pandas, whole file, 3,008 MB | DuckDB, whole file, 1,024 MB | pandas, one file per day, 1,024 MB |
|---|---|---|---|---|
| memory at 295k rows | 993 MB, then OutOfMemory | 1,304 MB | about 600 MB | 500 to 675 MB at any row count |
| memory at 703k rows | dead since day three | 2,993 MB | 912 MB | 500 to 675 MB |
| duration at 703k rows | | 9.5 s | 11 to 12.6 s | 3 to 7 s, in proportion to the day |
| events per hour, 7 days | CSV, 276 MB: 5.0 s warm, 10.9 s cold | | one Parquet, 48 MB: 0.35 s warm, 4.4 s cold | eight day files: 0.8 s warm, 5.1 s cold |

Both whole-file compactions rewrite the entire Parquet every quarter hour, so both climb towards their
memory setting as the file grows, and the engine only moves the day they reach it. The layout that scales
is one Parquet per day, which rewrites today's file only and reads the folder.
draws the three curves from CloudWatch Logs.
