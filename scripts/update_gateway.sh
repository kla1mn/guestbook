#!/usr/bin/env bash

if [ -f .env ]; then
  set -a
  source .env
  set +a
fi

envsubst < apigw/spec.yaml > apigw/spec.rendered.yaml
yc serverless api-gateway update --name guestbook-gw --spec apigw/spec.rendered.yaml
