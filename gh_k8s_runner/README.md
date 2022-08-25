# GitHub Self-hosted Kubernetes Runner

To ensure minimal reprocessing of data whenever the code change, this runner persists all data in a persistent volume.

## Setup steps

1. Install [ARC](https://github.com/actions-runner-controller/actions-runner-controller#installation) with Kubectl
2. Authenticate ARC using [Github App](https://github.com/actions-runner-controller/actions-runner-controller#deploying-using-github-app-authentication)
3. `docker build -t ghrunner gh_k8s_runner`
4. `docker tag ghrunner:latest gcr.io/{project_id}/ghrunner:$(docker images --format '{{.ID}}' ghrunner:latest)`
5. `docker push gcr.io/{project_id}/ghrunner:$(docker images --format '{{.ID}}' ghrunner:latest)`
6. `kustomize build gh_k8s_runner | sed "s/image: ghrunner/image: gcr.io\/{project_id}\/ghrunner:$(docker images --format '{{.ID}}' ghrunner:latest)/" | kubectl apply -f -`

## Using this runner

Set `runs-on: cdi-runner` on any job that should run on this runner.
