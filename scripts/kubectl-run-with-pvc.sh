#!/bin/bash

IMAGE="gcr.io/google-containers/ubuntu-slim:0.14"
COMMAND="/bin/bash"
NAMESPACE="default"
SUFFIX=$(date +%s | shasum | base64 | fold -w 10 | head -1 | tr '[:upper:]' '[:lower:]')

usage_exit() {
    echo "Usage: $0 [-c command] [-i image] [-n namespace] [-N nodeLabel=nodeValue] [-r resourceRequests] [-l resourceLimits] PVC ..." 1>&2
    echo "" 1>&2
    echo "Example:" 1>&2
    echo "    ./kubectl-run-with-pvc.sh \\" 1>&2
    echo "        -n cdi-runner \\" 1>&2
    echo "        -i \"gcr.io/excellent-zoo-300106/ghrunner:70791b9580a3\" \\" 1>&2
    echo "        -r \"cpu=0.5\" \\" 1>&2
    echo "        -r \"memory=4Gi\" \\" 1>&2
    echo "        -l \"cpu=1.0\" \\" 1>&2
    echo "        -l \"memory=8Gi\" \\" 1>&2
    echo "        -N \"part-of=actions-runner\" \\" 1>&2
    echo "        cdi-runner-data" 1>&2
    exit 1
}

REQUESTS=""
LIMITS=""

while getopts c:i:n:r:l:N:h OPT
do
    case $OPT in
        i)  IMAGE=$OPTARG
            ;;
        c)  COMMAND=$OPTARG
            ;;
        n)  NAMESPACE=$OPTARG
            ;;
        N)  
            IFS='=' read -ra ADDR <<< "$OPTARG"
            NODEPROPS=", \"nodeSelector\": {\"${ADDR[0]}\": \"${ADDR[1]}\"}, \"tolerations\": [{\"key\": \"${ADDR[0]}\", \"operator\": \"Equal\", \"value\": \"${ADDR[1]}\", \"effect\": \"NoSchedule\"}]"
            ;;
        r)  
            IFS='=' read -ra ADDR <<< "$OPTARG"
            if [ ${#REQUESTS} -ne 0 ]; then
                REQUESTS+=", "
            fi
            REQUESTS+="\"${ADDR[0]}\": \"${ADDR[1]}\""
            ;;
        l)  
            IFS='=' read -ra ADDR <<< "$OPTARG"
            if [ ${#LIMITS} -ne 0 ]; then
                LIMITS+=", "
            fi
            LIMITS+="\"${ADDR[0]}\": \"${ADDR[1]}\""
            ;;
        h)  usage_exit
            ;;
        \?) usage_exit
            ;;
    esac
done
shift $(($OPTIND - 1))

VOL_MOUNTS=""
VOLS=""
COMMA=""

for i in $@
do
  VOL_MOUNTS="${VOL_MOUNTS}${COMMA}{\"name\": \"${i}\",\"mountPath\": \"/pvcs/${i}\"}"
  VOLS="${VOLS}${COMMA}{\"name\": \"${i}\",\"persistentVolumeClaim\": {\"claimName\": \"${i}\"}}"
  COMMA=","
done

kubectl run --namespace=${NAMESPACE} -it --rm --pod-running-timeout=1h --restart=Never --image=${IMAGE} pvc-mounter-${SUFFIX} --overrides "
{
  \"spec\": {
    \"hostNetwork\": true,
    \"containers\":[
      {
        \"args\": [\"${COMMAND}\"],
        \"stdin\": true,
        \"tty\": true,
        \"name\": \"pvc\",
        \"image\": \"${IMAGE}\",
        \"volumeMounts\": [
          ${VOL_MOUNTS}
        ],
        \"resources\": {
          \"requests\": {$REQUESTS},
          \"limits\": {$LIMITS}
        }
      }
    ],
    \"volumes\": [
      ${VOLS}
    ]
    ${NODEPROPS}
  }
}
" -- ${COMMAND}
