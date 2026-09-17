# Keeping this dashboard static-ish

## What we built

`gdelt-dashboard.qmd` is a Quarto dashboard. At render time DuckDB reads the last seven
day partitions straight from S3 through httpfs, runs six aggregate queries and draws four
Plotly charts. The output is one self-contained HTML file with no server behind it.

## Static-ish, the recommended shape

Re-render on a schedule and publish the HTML. A GitHub Action on a cron (every hour, or
every morning before class) installs Quarto and the four packages, assumes a read-only
role through OIDC, runs `quarto render` and pushes the result to GitHub Pages. The page
is then never older than the last run, and the reader loads a plain file.

A Lambda can do the same work if the course prefers to keep everything on AWS. Quarto and
a Python runtime fit in a container image, the function writes the HTML to a public S3
bucket or to CloudFront, and EventBridge triggers it. This costs a few cents a month and
matches the pattern already used by the course chart functions.

## Alternatives

**Quarto plus Observable JS with DuckDB-Wasm.** The browser fetches Parquet byte ranges
over HTTP and runs the SQL itself, so the page is genuinely live and still a static file.
It needs the objects readable by anonymous browsers and a CORS rule that exposes range
requests, which is a real lesson in itself but also a real risk of a public bucket.

**Lambda that renders HTML on request.** Fresh at every click and already the course
pattern. Cold starts of several seconds, a cost per view, and a function to keep alive.

**Streamlit or Shiny for Python on an instance.** Best interactivity, real widgets and
callbacks. It is a server that runs all term and needs patching, and it is the thing the
course contrasts with serverless and static hosting.

**Evidence.** SQL in markdown, a build step, static output, and a good story about
analytics as code. It is one more toolchain to teach.

## Recommendation

For a lecture demo, render on a schedule with a GitHub Action and publish to Pages. It
keeps the artefact a single readable file, it reuses the toolchain the students already
have, and the numbers on the page prove the work happened over real data on S3. Show the
DuckDB-Wasm version afterwards as the contrast, because where the compute lives is the
point of the lecture.
