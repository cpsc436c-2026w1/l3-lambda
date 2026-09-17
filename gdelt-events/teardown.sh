#!/usr/bin/env bash
# Remove the GDELT pipeline's functions, rules and APIs. Keeps the bucket, the data and the layer.
set -uo pipefail
REGION=${REGION:-ca-central-1}; FN=436c-gdelt
for r in $FN-pull-15min $FN-compact-on-csv $FN-compact-duckdb-on-csv $FN-compact-daily-on-csv; do
  aws events remove-targets --rule $r --ids 1 --region $REGION >/dev/null 2>&1; aws events delete-rule --name $r --region $REGION >/dev/null 2>&1 && echo "rule $r removed"
done
for f in chart-csv chart-parquet chart-daily; do
  id=$(aws apigatewayv2 get-apis --region $REGION --query "Items[?Name=='$FN-$f-api'].ApiId" --output text); [ -n "$id" ] && aws apigatewayv2 delete-api --api-id $id --region $REGION && echo "api $FN-$f removed"
done
for f in pull compact compact-duckdb compact-daily chart-csv chart-parquet chart-daily; do aws lambda delete-function --function-name $FN-$f --region $REGION >/dev/null 2>&1 && echo "function $FN-$f removed"; done
aws iam delete-role-policy --role-name 436c-weather-demo-role --policy-name gdelt-bucket 2>/dev/null && echo "policy removed"
