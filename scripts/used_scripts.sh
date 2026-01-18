yc ydb database create --name guestbook-ydb --serverless


yc ydb database get guestbook-ydb


yc ydb database get --name guestbook-ydb --format json


yc iam service-account create --name guestbook-runtime-sa


yc iam service-account create --name guestbook-apigw-sa


yc config set folder-id b1g699ga1ofi6vanedo1
yc config get folder-id


yc iam service-account get --name guestbook-runtime-sa --format json | jq -r .id


yc iam service-account get --name guestbook-apigw-sa --format json | jq -r .id


yc resource-manager folder add-access-binding "$FOLDER_ID" \
  --role ydb.editor \
  --subject serviceAccount:"$RUNTIME_SA_ID"


yc serverless function create --name guestbook-fn


yc ydb database get --name guestbook-ydb


# shellcheck disable=SC2046
yc serverless function set-tag --name guestbook-fn --tag stable --version $(yc serverless function version list --function-name guestbook-fn --format json | jq -r '.[0].id')


yc serverless function version list --function-name guestbook-fn



# !also used scripts from deploy_canary.sh, deploy_stable.sh and update_gateway.sh!

