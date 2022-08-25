# GitHub Self-hosted Kubernetes Runner

To ensure minimal reprocessing of data whenever the code change, this runner persists all data in a persistent volume.

## Setup steps

1. Install [ARC](https://github.com/actions-runner-controller/actions-runner-controller#installation) with Kubectl
2. Authenticate ARC using [Github App](https://github.com/actions-runner-controller/actions-runner-controller#deploying-using-github-app-authentication)
3. Run `gh_k8s_runner/install.sh <project_id>`

## Using this runner

Set `runs-on: cdi-runner` on any job that should run on this runner.

## IMPORTANT

Rerun `gh_k8s_runner/install.sh <project_id>` whenever `requirements.txt` is updated, otherwise Github runner will not be able to pick up new dependencies.
