-- The CPU probes of 2026-09-16 (bench/out/cpu-probes-2026-09-16.tsv).
-- Single-threaded source: a generated range arrives as one stream, so threads cannot split it.
SELECT count(*) FROM (SELECT range % 7 AS k, sum(range) FROM range(60000000) GROUP BY 1);
-- A source with row groups: a materialised table scans in parallel (1.00 / 0.49 / 0.33 s at 1 / 2 / 4 threads on a laptop).
WITH t AS MATERIALIZED (SELECT range AS i FROM range(100000000)) SELECT sum((i * 2654435761) % 1000003) FROM t;
