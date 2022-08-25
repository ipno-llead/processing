#!/usr/bin/env bash
set -Eeuo pipefail

docker build -t ghrunner gh_k8s_runner
TAG=$(docker images --format '{{.ID}}' ghrunner:latest)
docker tag ghrunner:latest gcr.io/$1/ghrunner:$TAG
docker push gcr.io/$1/ghrunner:$TAG
kustomize build gh_k8s_runner | sed "s/image: ghrunner/image: gcr.io\/$1\/ghrunner:$TAG/" | kubectl apply -f -
