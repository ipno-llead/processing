#!/usr/bin/env bash

set -Eeuo pipefail
trap cleanup SIGINT SIGTERM ERR EXIT

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd -P)
cwd=$(pwd -P)
version=v0.3.11
declare -a executables=("gcloud" "gsutil" "kustomize" "docker" "curl" "kubectl")

usage() {
  cat <<EOF
Usage: $(basename "${BASH_SOURCE[0]}") [-h] [-v]
    [-s service_account] [-n namespace]
    [-S scripts_dir] [-k kustomize_dir]
    [-N node_selector:value]
    [-t toleration_key:toleration_value]
    project_id input_bucket output_bucket

Install an OCR jobqueue based on DocTR in your K8s cluster

Available options:
-h, --help              Print this help and exit
-v, --verbose           Print script debug info
-s, --service-account   Service account that will be created
                        to read and write to buckets, defaults
                        to 'k8s-ocr-jobqueue'
-n, --namespace         Kubernetes namespace to install this jobqueue,
                        defaults to 'k8s-ocr-jobqueue'
-S, --scripts-dir       Save enqueue and uninstall scripts to this folder,
                        defaults to 'scripts'
-k, --kustomize-dir     Save kustomization manifests to this folder,
                        defaults to 'k8s-ocr-jobqueue'
-t, --toleration        Add pod toleration (key and value separated by colon)
-N, --node-selector     Add pod nodeSelector (key and value separated by colon)
EOF
  exit
}

cleanup() {
  trap - SIGINT SIGTERM ERR EXIT
  [[ -n "$tmp_dir"  ]] && cd && rm -rf $tmp_dir
}

setup_colors() {
  if [[ -t 2 ]] && [[ -z "${NO_COLOR-}" ]] && [[ "${TERM-}" != "dumb" ]]; then
NOFORMAT='\033[0m' RED='\033[0;31m' GREEN='\033[0;32m' ORANGE='\033[0;33m' BLUE='\033[0;34m' PURPLE='\033[0;35m' CYAN='\033[0;36m' YELLOW='\033[1;33m'
  else
NOFORMAT='' RED='' GREEN='' ORANGE='' BLUE='' PURPLE='' CYAN='' YELLOW=''
  fi
}

msg() {
  echo >&2 -e "${1-}"
}

die() {
  local msg=$1
  local code=${2:-1} # default exit status 1
  msg "Error: $msg"
  exit "$code"
}

check_executables() {
  for cmd in "${executables[@]}"
  do
    command -v $cmd >/dev/null 2>&1 || die "$cmd could not be found"
  done
}

parse_params() {
  # default values of variables set from params
  project_id=''
  input_bucket=''
  output_bucket=''
  service_account='k8s-ocr-jobqueue'
  namespace='k8s-ocr-jobqueue'
  scripts_dir='scripts'
  kustomize_dir='k8s-ocr-jobqueue'
  toleration_key=''
  toleration_value=''
  nodeselector_key=''
  nodeselector_value=''

  while :; do
case "${1-}" in
-h | --help) usage ;;
-v | --verbose) set -x ;;
--no-color) NO_COLOR=1 ;;
-s | --service-account)
  service_account="${2-}"
  shift
  ;;
-n | --namespace)
  namespace="${2-}"
  shift
  ;;
-S | --scripts-dir)
  scripts_dir="${2-}"
  shift
  ;;
-k | --kustomize-dir)
  kustomize_dir="${2-}"
  shift
  ;;
-t | --toleration)
  IFS=: read -r toleration_key toleration_value <<< "${2-}"
  shift
  ;;
-N | --node-selector)
  IFS=: read -r nodeselector_key nodeselector_value <<< "${2-}"
  shift
  ;;
-?*) die "Unknown option: " ;;
*) break ;;
esac
shift
  done

  args=("$@")

  # check required params and arguments
  [[ ${#args[@]} -lt 3 ]] && die "Wrong number of script arguments: ${#args[@]} < 3"

  project_id="${args[0]}"
  input_bucket="${args[1]}"
  output_bucket="${args[2]}"

  return 0
}

download_assets() {
  echo "Downloading assets..."
  tmp_dir=$(mktemp -d -t ci-XXXXXXXXXX)
  cd $tmp_dir
  assets_dir=$tmp_dir/installation-assets
  local FILE=installation-assets.tar.gz
  local URL=https://github.com/pckhoi/k8s-ocr-jobqueue/releases/download/$version/$FILE
  echo "Downloading:" $URL
  curl -A "k8s-ocr-jobqueueu-installer" -fsL "$URL" > "$FILE"
  tar zxf "$FILE"
  cd $assets_dir
}

create_buckets() {
  echo "Creating buckets..."
  gsutil ls -b -p $project_id gs://$input_bucket || gsutil mb -p $project_id gs://$input_bucket
  gsutil ls -b -p $project_id gs://$output_bucket || gsutil mb -p $project_id gs://$output_bucket
  gsutil iam ch allUsers:objectViewer gs://$output_bucket
}

create_service_account() {
  echo "Creating service account..."
  key_file=key.json
  gcloud iam service-accounts describe $service_account@$project_id.iam.gserviceaccount.com \
    || gcloud iam service-accounts create $service_account \
    --description="Read/write OCR data to storage buckets" \
    --display-name="OCR docs admin" \
    --project $project_id
  gsutil iam ch \
    serviceAccount:$service_account@$project_id.iam.gserviceaccount.com:admin \
    gs://$input_bucket gs://$output_bucket
  gcloud iam service-accounts keys create $key_file \
    --iam-account=$service_account@$project_id.iam.gserviceaccount.com
}

push_image() {
  echo "Pushing image..."
  docker build -t doctr image
  img_id=$(docker images --format '{{.ID}}' doctr:latest)
  docker tag doctr:latest gcr.io/$project_id/doctr:$img_id
  docker push gcr.io/$project_id/doctr:$img_id
}

prepare_kustomize_dir() {
  echo 'Preparing kustomize manifests in directory "'$kustomize_dir'"...'
  kubectl create namespace $namespace --dry-run=client -o yaml | kubectl apply -f -
  kubectl delete secret service-account-key \
    -n $namespace  --ignore-not-found=true
  kubectl create secret generic service-account-key \
    -n $namespace \
    --from-file=key.json=$key_file
  mkdir -p $cwd/$kustomize_dir
  cd $cwd/$kustomize_dir
  cp $assets_dir/kustomization.yml .
  cp $assets_dir/job.yml .
  kustomize edit set image doctr=gcr.io/$project_id/doctr:$img_id
  kustomize edit add configmap doctr-config \
    --from-literal=SOURCE_BUCKET=$input_bucket \
    --from-literal=SINK_BUCKET=$output_bucket
  ( [[ ! -z "$toleration_key" ]] || [[ ! -z "$nodeselector_key" ]] ) && \
    cat <<EOT >> kustomization.yml

patchesStrategicMerge:
- |-
  apiVersion: batch/v1
  kind: Job
  metadata:
    name: doctr
  spec:
    template:
      spec:
EOT
  [[ ! -z "$nodeselector_key" ]] && \
    cat <<EOT >> kustomization.yml
        nodeSelector:
          $nodeselector_key: "$nodeselector_value"
EOT
  [[ ! -z "$toleration_key" ]] && \
    cat <<EOT >> kustomization.yml
        tolerations:
          - key: "$toleration_key"
            operator: "Equal"
            value: "$toleration_value"
            effect: "NoSchedule"
EOT
}

prepare_scripts_dir() {
  echo 'Preparing scripts in directory "'$scripts_dir'"'
  mkdir -p $cwd/$scripts_dir
  cat $assets_dir/uninstall.sh \
    | sed 's/project_id=%%/project_id='$project_id'/; s/input_bucket=%%/input_bucket='$input_bucket'/; s/output_bucket=%%/output_bucket='$output_bucket'/; s/service_account=%%/service_account='$service_account'/; s/namespace=%%/namespace='$namespace'/' \
    > $cwd/$scripts_dir/uninstall_k8s_ocr_jobqueue.sh
  cat $assets_dir/queue_pdf_for_ocr.py \
    | sed 's/SOURCE_BUCKET = ""/SOURCE_BUCKET = "'$input_bucket'"/; s/KUSTOMIZE_DIR = ""/KUSTOMIZE_DIR = "'$kustomize_dir'"/' \
    > $cwd/$scripts_dir/queue_pdf_for_ocr.py
  chmod +x $cwd/$scripts_dir/uninstall_k8s_ocr_jobqueue.sh
  chmod +x $cwd/$scripts_dir/queue_pdf_for_ocr.py
  echo "Installation finished!"
  echo "To start enqueuing PDF for OCR, run:"
  echo "    $scripts_dir/queue_pdf_for_ocr.py PDF_DIR [PDF_DIR...]"
  echo ""
  echo "To uninstall, run:"
  echo "    $scripts_dir/uninstall_k8s_ocr_jobqueue.sh"
}

parse_params "$@"
check_executables
setup_colors

download_assets
create_buckets
create_service_account
push_image
prepare_kustomize_dir
prepare_scripts_dir
