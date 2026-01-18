#!/usr/bin/env bash
set -euo pipefail

if [ -f .env ]; then
  set -a
  source .env
  set +a
fi


ver_json="$(
  yc serverless function version create \
    --function-name "$FUNC_NAME" \
    --runtime python311 \
    --entrypoint index.handler \
    --memory 256m \
    --execution-timeout 5s \
    --service-account-id "$RUNTIME_SA_ID" \
    --source-path function.zip \
    --environment YDB_ENDPOINT="$YDB_ENDPOINT",YDB_DATABASE="$YDB_DATABASE",BACKEND_VERSION="$BACKEND_CANARY_VERSION",REPLICA_NAME=canary \
    --format json
)"

ver_id="$(echo "$ver_json" | jq -r '.id')"
if [ -z "$ver_id" ] || [ "$ver_id" = "null" ]; then
  echo "ERROR: failed to parse version id from yc output" >&2
  echo "$ver_json" >&2
  exit 1
fi

yc serverless function version set-tag --id "$ver_id" --tag canary >/dev/null

echo "OK: deployed $FUNC_NAME version=$ver_id tag=canary backend=$BACKEND_CANARY_VERSION replica=canary"
