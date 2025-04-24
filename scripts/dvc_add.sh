#!/usr/bin/env bash
set -ex


for folder in data/raw
do
   mkdir -p dvc/$folder || true
   # Find subdirectories and process each one
   find $folder -type d -mindepth 1 -maxdepth 1 | while read dir; do
       # Extract just the directory name without the path
       dir_name=$(basename "$dir")
       # Run dvc add from the directory where we want the .dvc file
       (cd dvc/$folder/ && dvc add -f "../../../$dir")
       # Rename the .dvc file to match the directory name
       mv dvc/$folder/$(basename $dir).dvc dvc/$folder/$dir_name.dvc
   done
done


# Add the raw_minutes directory
(cd . && dvc add -f data/raw_minutes)
mv data/raw_minutes.dvc raw_minutes.dvc
