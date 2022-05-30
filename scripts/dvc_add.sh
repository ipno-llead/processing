#!/usr/bin/env bash
set -ex

for folder in data/raw
do
    mkdir -p dvc/$folder || true
    find $folder -type d -mindepth 1 -maxdepth 1 | xargs -I{} dvc add --file dvc/{}.dvc {}
done
