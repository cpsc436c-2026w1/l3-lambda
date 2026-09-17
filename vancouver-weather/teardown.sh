#!/usr/bin/env bash
# Remove everything deploy.sh created except the bucket and its data (delete those by hand).
set -uo pipefail
REGION=${REGION:-ca-central-1}; BUCKET=${BUCKET:-436c-2026w1-weather}; ROLE_NAME=${ROLE_NAME:-436c-weather-demo-role}; FN=436c-weather
for f in chart-csv chart-parquet duck chart; do
  id=$(aws apigatewayv2 get-apis --region "$REGION" --query "Items[?Name=='$FN-$f-api'].ApiId" --output text)
  [ -n "$id" ] && aws apigatewayv2 delete-api --api-id "$id" --region "$REGION"
done
aws s3api put-bucket-notification-configuration --bucket "$BUCKET" --notification-configuration '{}'
for rule in $FN-compact-on-csv $FN-compact-duckdb-on-csv $FN-pull-15min $FN-chart-csv-hourly $FN-chart-parquet-hourly $FN-compact-hourly $FN-compact-duckdb-hourly $FN-pull-10min; do
  aws events remove-targets --rule "$rule" --ids 1 --region "$REGION" >/dev/null 2>&1 && aws events delete-rule --name "$rule" --region "$REGION"
done
for f in pull compact compact-duckdb chart-csv chart-parquet duck chart; do aws lambda delete-function --function-name $FN-$f --region "$REGION" 2>/dev/null; done
aws iam delete-role-policy --role-name "$ROLE_NAME" --policy-name weather-bucket
aws iam detach-role-policy --role-name "$ROLE_NAME" --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
aws iam delete-role --role-name "$ROLE_NAME"
echo "removed; bucket $BUCKET, its data and the 436c-duckdb layer versions kept"
