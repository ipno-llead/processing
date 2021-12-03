#!/usr/bin/env bash
set -ex

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." >/dev/null 2>&1 && pwd )"
SCRIPT=$1
shift 1
cd $DIR
export PYTHONPATH=$DIR:$PYTHONPATH

python $SCRIPT $@
