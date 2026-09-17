-- The on-our-data thread probe of 2026-09-16: a CPU-heavy regex over the whole-file Parquet (711k rows, 7 row groups).
-- 3,008 MB: 0.34 / 0.22 / 0.22 s at 1 / 2 / 4 threads. 1,769 MB: 0.34 / 0.34 / 0.40. 1,024 MB: 0.65 / 0.66 / 0.70.
SELECT count(*) FROM read_parquet('/tmp/gd.parquet') WHERE regexp_matches(SOURCEURL, '(news|press|times|post)');
-- How many row groups the file has, which is how many threads a scan can use.
SELECT count(DISTINCT row_group_id) FROM parquet_metadata('/tmp/gd.parquet');
