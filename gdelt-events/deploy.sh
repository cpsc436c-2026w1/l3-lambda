#!/usr/bin/env bash
# Deploy the GDELT pipeline: the weather pipeline's layout on 45 MB day files. Re-runnable.
#   pull            timer, every 15 min          GDELT export file -> gdelt/events/YYYY-MM-DD.csv (append)
#   compact         CSV write (via EventBridge)   pandas:  day files -> gdelt/events-pandas.parquet
#   compact-duckdb  CSV write (via EventBridge)   DuckDB over s3://: day files -> gdelt/events-duckdb.parquet
#   chart-csv       HTTP API                     DuckDB over the CSV day files -> SVG (events per hour)
#   chart-parquet   HTTP API                     DuckDB over events-duckdb.parquet -> SVG
#   compact-daily   CSV write (via EventBridge)   pandas: the day file -> gdelt/events-daily/day=YYYY-MM-DD/part.parquet (no history read)
#   chart-daily     HTTP API                     DuckDB over the day folder -> SVG
set -euo pipefail
cd "$(dirname "$0")"

REGION=${REGION:-ca-central-1}
BUCKET=${BUCKET:-436c-2026w1-weather}
PREFIX=${PREFIX:-gdelt/events}
PARQUET_KEY_PANDAS=${PARQUET_KEY_PANDAS:-gdelt/events-pandas.parquet}
PARQUET_KEY_DUCKDB=${PARQUET_KEY_DUCKDB:-gdelt/events-duckdb.parquet}
ROLE_NAME=${ROLE_NAME:-436c-weather-demo-role}
RUNTIME=python3.12
PANDAS_LAYER="arn:aws:lambda:$REGION:336392948345:layer:AWSSDKPandas-Python312:20"
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
ENV="Variables={BUCKET=$BUCKET,PREFIX=$PREFIX,PARQUET_KEY_PANDAS=$PARQUET_KEY_PANDAS,PARQUET_KEY_DUCKDB=$PARQUET_KEY_DUCKDB}"
FN=436c-gdelt

echo "== role policy for the gdelt prefix (the weather role, a second inline policy)"
aws iam put-role-policy --role-name "$ROLE_NAME" --policy-name gdelt-bucket --policy-document "{\"Version\":\"2012-10-17\",\"Statement\":[
  {\"Effect\":\"Allow\",\"Action\":[\"s3:GetObject\",\"s3:PutObject\",\"s3:AbortMultipartUpload\"],\"Resource\":[\"arn:aws:s3:::$BUCKET/gdelt/*\"]},
  {\"Effect\":\"Allow\",\"Action\":\"s3:ListBucket\",\"Resource\":\"arn:aws:s3:::$BUCKET\"}]}"
ROLE_ARN="arn:aws:iam::$ACCOUNT:role/$ROLE_NAME"

echo "== DuckDB layer (the weather pipeline's, newest version, carries httpfs)"
DUCK_LAYER=$(aws lambda list-layer-versions --layer-name 436c-duckdb --region "$REGION" --query "LayerVersions[0].LayerVersionArn" --output text)
echo "   $DUCK_LAYER"

deploy_fn () {  # name dir timeout memory layer extra-env
  local name=$1 dir=$2 timeout=$3 memory=$4 layer=${5:-} extra=${6:-}
  local env="$ENV"; [ -n "$extra" ] && env="${ENV%\}},$extra}"
  (cd "$dir" && rm -f ../"$name".zip && zip -q ../"$name".zip handler.py)
  if aws lambda get-function --function-name "$name" --region "$REGION" >/dev/null 2>&1; then
    aws lambda update-function-code --function-name "$name" --zip-file fileb://"$name".zip --region "$REGION" >/dev/null
    aws lambda wait function-updated-v2 --function-name "$name" --region "$REGION"
    aws lambda update-function-configuration --function-name "$name" --timeout "$timeout" --memory-size "$memory" --environment "$env" ${layer:+--layers "$layer"} --region "$REGION" >/dev/null
  else
    aws lambda create-function --function-name "$name" --runtime $RUNTIME --handler handler.lambda_handler \
      --role "$ROLE_ARN" --zip-file fileb://"$name".zip --timeout "$timeout" --memory-size "$memory" --region "$REGION" \
      --environment "$env" ${layer:+--layers "$layer"} >/dev/null
  fi
  aws lambda wait function-active-v2 --function-name "$name" --region "$REGION"
  aws lambda wait function-updated-v2 --function-name "$name" --region "$REGION"
  echo "   $name deployed"
}
arn () { aws lambda get-function --function-name "$1" --region "$REGION" --query Configuration.FunctionArn --output text; }
allow () { aws lambda add-permission --function-name "$1" --statement-id "$2" --action lambda:InvokeFunction --principal "$3" --source-arn "$4" --region "$REGION" >/dev/null 2>&1 || true; }
api () {
  local id; id=$(aws apigatewayv2 get-apis --region "$REGION" --query "Items[?Name=='$1-api'].ApiId" --output text)
  if [ -z "$id" ]; then
    id=$(aws apigatewayv2 create-api --name "$1-api" --protocol-type HTTP --target "$(arn "$1")" --region "$REGION" --query ApiId --output text)
    allow "$1" apigw apigateway.amazonaws.com "arn:aws:execute-api:$REGION:$ACCOUNT:$id/*"
  fi
  echo "https://$id.execute-api.$REGION.amazonaws.com"
}

echo "== functions"
deploy_fn $FN-pull pull 600 512
deploy_fn $FN-compact compact 300 1024 "$PANDAS_LAYER"
deploy_fn $FN-compact-duckdb compact-duckdb 300 1024 "$DUCK_LAYER"
deploy_fn $FN-compact-daily compact-daily 300 1024 "$PANDAS_LAYER"
deploy_fn $FN-chart-csv charts 60 1024 "$DUCK_LAYER" "SOURCE=csv"
deploy_fn $FN-chart-parquet charts 60 1024 "$DUCK_LAYER" "SOURCE=parquet"
deploy_fn $FN-chart-daily charts 60 1024 "$DUCK_LAYER" "SOURCE=daily"
# one compaction at a time per engine: two runs must not race on the same Parquet file
for f in compact compact-duckdb compact-daily; do aws lambda put-function-concurrency --function-name $FN-$f --reserved-concurrent-executions 1 --region "$REGION" >/dev/null; done

echo "== timer: pull every 15 minutes"
aws events put-rule --name $FN-pull-15min --schedule-expression "rate(15 minutes)" --region "$REGION" >/dev/null
allow $FN-pull events-$FN-pull-15min events.amazonaws.com "arn:aws:events:$REGION:$ACCOUNT:rule/$FN-pull-15min"
aws events put-targets --rule $FN-pull-15min --targets "Id=1,Arn=$(arn $FN-pull)" --region "$REGION" >/dev/null

echo "== S3 events through EventBridge (the bucket already publishes them): a CSV write runs both compactions"
PATTERN="{\"source\":[\"aws.s3\"],\"detail-type\":[\"Object Created\"],\"detail\":{\"bucket\":{\"name\":[\"$BUCKET\"]},\"object\":{\"key\":[{\"prefix\":\"$PREFIX/\"}]}}}"
for f in compact compact-duckdb compact-daily; do
  aws events put-rule --name "$FN-$f-on-csv" --event-pattern "$PATTERN" --region "$REGION" >/dev/null
  allow $FN-$f "events-$FN-$f-on-csv" events.amazonaws.com "arn:aws:events:$REGION:$ACCOUNT:rule/$FN-$f-on-csv"
  aws events put-targets --rule "$FN-$f-on-csv" --targets "Id=1,Arn=$(arn $FN-$f)" --region "$REGION" >/dev/null
done

echo "== HTTP APIs"
echo "   chart from CSV:     $(api $FN-chart-csv)/?country=CA"
echo "   chart from Parquet: $(api $FN-chart-parquet)/?country=CA"
echo "   chart from day files: $(api $FN-chart-daily)/?country=CA"
echo "done. Backfill a day: aws lambda invoke --function-name $FN-pull --payload '{\"day\":\"2026-09-10\"}' ... ; remove with ./teardown.sh"
