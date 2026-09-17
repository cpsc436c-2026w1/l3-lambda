#!/usr/bin/env bash
# Deploy the pipeline. Re-runnable: updates code and configuration if the pieces exist.
#   pull            timer, every 15 min          Open-Meteo -> weather/cities/YYYY-MM-DD.csv (ten cities)
#   compact         CSV write (via EventBridge)   pandas:  CSVs -> weather/cities-pandas.parquet
#   compact-duckdb  CSV write (via EventBridge)   DuckDB:  CSVs -> weather/cities-duckdb.parquet
#   chart-csv       HTTP API + hourly timer      DuckDB over the CSV day files -> SVG
#   chart-parquet   HTTP API + hourly timer      DuckDB over cities-duckdb.parquet -> SVG
#   duck            HTTP API                     /daily, /chart, /sql over cities-duckdb.parquet
set -euo pipefail
cd "$(dirname "$0")"

REGION=${REGION:-ca-central-1}
BUCKET=${BUCKET:-436c-2026w1-weather}
PREFIX=${PREFIX:-weather/cities}
PARQUET_KEY_PANDAS=${PARQUET_KEY_PANDAS:-weather/cities-pandas.parquet}
PARQUET_KEY_DUCKDB=${PARQUET_KEY_DUCKDB:-weather/cities-duckdb.parquet}
ROLE_NAME=${ROLE_NAME:-436c-weather-demo-role}
RUNTIME=python3.12
PANDAS_LAYER="arn:aws:lambda:$REGION:336392948345:layer:AWSSDKPandas-Python312:20"
DUCK_LAYER_DIR=${DUCK_LAYER_DIR:-$PWD/layer}   # python/ holds the duckdb wheel for manylinux x86_64, duckdb_ext/ the httpfs build for linux_amd64_gcc4
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
ENV="Variables={BUCKET=$BUCKET,PREFIX=$PREFIX,PARQUET_KEY_PANDAS=$PARQUET_KEY_PANDAS,PARQUET_KEY_DUCKDB=$PARQUET_KEY_DUCKDB}"
FN=436c-weather

echo "== bucket $BUCKET"
if ! aws s3api head-bucket --bucket "$BUCKET" 2>/dev/null; then
  aws s3api create-bucket --bucket "$BUCKET" --region "$REGION" --create-bucket-configuration LocationConstraint="$REGION" >/dev/null
  aws s3api put-public-access-block --bucket "$BUCKET" --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
fi

echo "== role"
if ! aws iam get-role --role-name "$ROLE_NAME" >/dev/null 2>&1; then
  aws iam create-role --role-name "$ROLE_NAME" --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}' >/dev/null
  aws iam attach-role-policy --role-name "$ROLE_NAME" --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
  NEW_ROLE=1
fi
aws iam put-role-policy --role-name "$ROLE_NAME" --policy-name weather-bucket --policy-document "{\"Version\":\"2012-10-17\",\"Statement\":[
  {\"Effect\":\"Allow\",\"Action\":[\"s3:GetObject\",\"s3:PutObject\"],\"Resource\":[\"arn:aws:s3:::$BUCKET/weather/*\",\"arn:aws:s3:::$BUCKET/charts/*\"]},
  {\"Effect\":\"Allow\",\"Action\":\"s3:ListBucket\",\"Resource\":\"arn:aws:s3:::$BUCKET\"}]}"
[ "${NEW_ROLE:-}" = 1 ] && sleep 10
ROLE_ARN="arn:aws:iam::$ACCOUNT:role/$ROLE_NAME"

echo "== DuckDB layer"
DUCK_LAYER=$(aws lambda list-layer-versions --layer-name 436c-duckdb --region "$REGION" --query "LayerVersions[0].LayerVersionArn" --output text 2>/dev/null || true)
if [ -z "$DUCK_LAYER" ] || [ "$DUCK_LAYER" = "None" ]; then
  # the layer carries the httpfs extension so DuckDB reads and writes s3:// itself (no boto3 in the DuckDB functions).
  # The manylinux wheel is DuckDB's linux_amd64_gcc4 platform, so the extension must come from that folder.
  V=$(grep -m1 "^Version:" "$DUCK_LAYER_DIR"/python/duckdb-*.dist-info/METADATA | cut -d" " -f2)
  [ -f "$DUCK_LAYER_DIR/duckdb_ext/httpfs.duckdb_extension" ] || { mkdir -p "$DUCK_LAYER_DIR/duckdb_ext"; curl -sfL "http://extensions.duckdb.org/v$V/linux_amd64_gcc4/httpfs.duckdb_extension.gz" | gunzip > "$DUCK_LAYER_DIR/duckdb_ext/httpfs.duckdb_extension"; }
  (cd "$DUCK_LAYER_DIR" && rm -f duck-layer.zip && zip -qr duck-layer.zip python duckdb_ext -x "python/duckdb-stubs/*" -x "python/adbc_driver_duckdb/*")
  aws s3 cp "$DUCK_LAYER_DIR/duck-layer.zip" "s3://$BUCKET/layers/duck-layer.zip" --region "$REGION" >/dev/null
  DUCK_LAYER=$(aws lambda publish-layer-version --layer-name 436c-duckdb --content "S3Bucket=$BUCKET,S3Key=layers/duck-layer.zip" \
    --compatible-runtimes $RUNTIME --compatible-architectures x86_64 --region "$REGION" --query LayerVersionArn --output text)
fi
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
allow () {  # function statement-id principal source-arn
  aws lambda add-permission --function-name "$1" --statement-id "$2" --action lambda:InvokeFunction --principal "$3" --source-arn "$4" --region "$REGION" >/dev/null 2>&1 || true
}
schedule () {  # rule-name expression function [input-json]
  aws events put-rule --name "$1" --schedule-expression "$2" --region "$REGION" >/dev/null
  allow "$3" "events-$1" events.amazonaws.com "arn:aws:events:$REGION:$ACCOUNT:rule/$1"
  if [ -n "${4:-}" ]; then
    aws events put-targets --rule "$1" --targets "[{\"Id\":\"1\",\"Arn\":\"$(arn "$3")\",\"Input\":$(printf '%s' "$4" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))')}]" --region "$REGION" >/dev/null
  else
    aws events put-targets --rule "$1" --targets "Id=1,Arn=$(arn "$3")" --region "$REGION" >/dev/null
  fi
}
api () {  # function -> prints the endpoint
  local id; id=$(aws apigatewayv2 get-apis --region "$REGION" --query "Items[?Name=='$1-api'].ApiId" --output text)
  if [ -z "$id" ]; then
    id=$(aws apigatewayv2 create-api --name "$1-api" --protocol-type HTTP --target "$(arn "$1")" --region "$REGION" --query ApiId --output text)
    allow "$1" apigw apigateway.amazonaws.com "arn:aws:execute-api:$REGION:$ACCOUNT:$id/*"
  fi
  echo "https://$id.execute-api.$REGION.amazonaws.com"
}

echo "== functions"
deploy_fn $FN-pull pull 30 256
deploy_fn $FN-compact compact 60 1024 "$PANDAS_LAYER"
deploy_fn $FN-compact-duckdb compact-duckdb 60 1024 "$DUCK_LAYER"
deploy_fn $FN-chart-csv charts 30 1024 "$DUCK_LAYER" "SOURCE=csv"
deploy_fn $FN-chart-parquet charts 30 1024 "$DUCK_LAYER" "SOURCE=parquet"
deploy_fn $FN-duck duck 30 1024 "$DUCK_LAYER"

echo "== timers: pull every 15 minutes; both charts pre-render hourly"
schedule $FN-pull-15min "rate(15 minutes)" $FN-pull
schedule $FN-chart-csv-hourly "rate(1 hour)" $FN-chart-csv '{"prerender": true}'
schedule $FN-chart-parquet-hourly "rate(1 hour)" $FN-chart-parquet '{"prerender": true}'
for old in $FN-compact-hourly $FN-compact-duckdb-hourly $FN-pull-10min; do
  aws events remove-targets --rule "$old" --ids 1 --region "$REGION" >/dev/null 2>&1 && aws events delete-rule --name "$old" --region "$REGION" >/dev/null 2>&1 || true
done

echo "== S3 events through EventBridge: a CSV write runs both compactions"
# S3 refuses two Lambda notifications with overlapping filters, so the bucket publishes to EventBridge
# and two rules, one per engine, match the same object-created event.
aws s3api put-bucket-notification-configuration --bucket "$BUCKET" --notification-configuration '{"EventBridgeConfiguration":{}}'
PATTERN="{\"source\":[\"aws.s3\"],\"detail-type\":[\"Object Created\"],\"detail\":{\"bucket\":{\"name\":[\"$BUCKET\"]},\"object\":{\"key\":[{\"prefix\":\"$PREFIX/\"}]}}}"
for f in compact compact-duckdb; do
  aws events put-rule --name "$FN-$f-on-csv" --event-pattern "$PATTERN" --region "$REGION" >/dev/null
  allow $FN-$f "events-$FN-$f-on-csv" events.amazonaws.com "arn:aws:events:$REGION:$ACCOUNT:rule/$FN-$f-on-csv"
  aws events put-targets --rule "$FN-$f-on-csv" --targets "Id=1,Arn=$(arn $FN-$f)" --region "$REGION" >/dev/null
done

echo "== HTTP APIs"
echo "   chart from CSV:     $(api $FN-chart-csv)/?city=Vancouver"
echo "   chart from Parquet: $(api $FN-chart-parquet)/?city=Vancouver"
echo "   DuckDB SQL:         $(api $FN-duck)/daily  (/chart?city=, /sql?q=SELECT ...)"

echo "== retire the pandas chart function and its API, if present"
OLD=$(aws apigatewayv2 get-apis --region "$REGION" --query "Items[?Name=='$FN-chart-api'].ApiId" --output text)
[ -n "$OLD" ] && aws apigatewayv2 delete-api --api-id "$OLD" --region "$REGION" && echo "   old chart API removed"
aws lambda delete-function --function-name $FN-chart --region "$REGION" 2>/dev/null && echo "   old chart function removed" || true

echo "== one pull now (the CSV write triggers both compactions)"
aws lambda invoke --function-name $FN-pull --region "$REGION" --cli-binary-format raw-in-base64-out --payload '{}' /dev/stdout | head -c 200; echo
echo "done. Remove with: ./teardown.sh"
