# The two Lambda layers

A Lambda layer is a zip file of libraries that AWS Lambda unpacks at `/opt` before the handler runs. The
handler imports from it as if the packages were in its own deployment package, and several functions can
share one layer. These pipelines use two layers. One comes ready-made from AWS and is attached by ARN, and
one is built here because no managed layer carries what it holds.

If you are taking this course, the assignment attaches the managed pandas layer by ARN and asks you to
build nothing. Read the second half only when a package you need has no managed layer.

## The pandas layer, attached by ARN

AWS publishes the AWS SDK for pandas as a managed layer in every region. It contains pandas, pyarrow and
numpy, which is everything the compaction functions and the pandas charts import, already compiled for
Lambda's runtime. Attaching it is one line of configuration and nothing is built or uploaded.
`vancouver-weather/deploy.sh` names the version it uses:

```
PANDAS_LAYER="arn:aws:lambda:ca-central-1:336392948345:layer:AWSSDKPandas-Python312:20"
```

The account number in that ARN is AWS's own, the region must match the region the function runs in, the
`Python312` part must match the function's runtime, and the trailing `20` is the layer version. Pin the
version rather than tracking the newest one, so a redeploy cannot change the library under the code.

## The DuckDB layer, built here

There is no managed layer for DuckDB, so `vancouver-weather/deploy.sh` builds one in the `DuckDB layer`
section of the script. The layer holds two things, a folder `python/` with the DuckDB Python package and a
folder `duckdb_ext/` with DuckDB's httpfs extension, which is the piece that lets DuckDB read and write
`s3://` paths itself.

### Install the package for Lambda's platform, not yours

Lambda runs on x86_64 Linux, so the wheel must be the Linux one whatever machine builds the layer. `uv pip
install` can be told which platform to resolve for:

```
uv pip install --python-version 3.12 --python-platform x86_64-manylinux2014 \
  --only-binary :all: --target python duckdb==1.2.2
```

`--only-binary :all:` refuses to fall back to building from source, which would silently produce a package
for the wrong platform. The `--target python` part matters as much as the rest. Lambda looks for Python
packages in `python/` at the root of the layer, so the folder name is part of the contract.

### Add the httpfs extension, from the right platform folder

DuckDB's Python wheel does not bundle httpfs. DuckDB would normally download the extension on first use,
but it writes it into the user's home directory, and a Lambda function cannot write there. So the
extension goes into the layer as a file:

```
curl -sfL http://extensions.duckdb.org/v1.2.2/linux_amd64_gcc4/httpfs.duckdb_extension.gz \
  | gunzip > duckdb_ext/httpfs.duckdb_extension
```

Use the version number of the DuckDB package you just installed, since an extension only loads into the
build it was made for. The platform folder is the part that catches people out. The manylinux wheel
identifies itself to DuckDB as the `linux_amd64_gcc4` platform, so an extension taken from the plain
`linux_amd64` folder is refused at load time even though the machine is the same.

### How the handler loads it

The DuckDB functions open one connection per execution environment and set it up in two statements. The
first loads the extension from the layer by path, and the second hands DuckDB the credentials Lambda
already put in the function's environment:

```python
con.execute("LOAD '/opt/duckdb_ext/httpfs.duckdb_extension'")
con.execute("CREATE SECRET s3 (TYPE s3, PROVIDER config, KEY_ID '...', SECRET '...', "
            "SESSION_TOKEN '...', REGION 'ca-central-1', ENDPOINT 's3.ca-central-1.amazonaws.com')")
```

The three values come from `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `AWS_SESSION_TOKEN`, which
Lambda sets from the function's execution role. Nothing is stored anywhere, and the function can reach
only what the role allows. The `ENDPOINT` is the regional S3 endpoint, `s3.<region>.amazonaws.com`, and
setting it keeps the request in the region instead of following a redirect.

### Zip it and publish it

The two folders go in at the root of the zip, so the archive contains `python/` and `duckdb_ext/` and no
wrapping folder:

```
zip -qr duck-layer.zip python duckdb_ext
aws s3 cp duck-layer.zip s3://YOUR-BUCKET/layers/duck-layer.zip
aws lambda publish-layer-version --layer-name 436c-duckdb \
  --content S3Bucket=YOUR-BUCKET,S3Key=layers/duck-layer.zip \
  --compatible-runtimes python3.12 --compatible-architectures x86_64
```

The zip goes through S3 rather than straight to Lambda because it is about 43 MB zipped and roughly 100 MB
unpacked, over the 50 MB limit for a direct upload. Publishing creates a new immutable version, and
functions attach one version by ARN.

### Sizes to keep an eye on

A function's own package plus all its layers may not exceed 250 MB unpacked, and a function may attach at
most five layers. This layer alone is about 100 MB of that budget, so there is room but not endless room.
The `deploy.sh` build excludes the type stubs and the ADBC driver from the wheel for that reason.
