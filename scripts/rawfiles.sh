#!/usr/bin/env bash

set -e

if [ -z "$1" ]; then
  echo "usage: scripts/rawfiles.sh SUB_DIR"
  echo ""
  echo "Prints raw files under SUB_DIR of build/raw folder."
  echo ""
  echo "positional arguments:"
  echo "  SUB_DIR               sub-directory under build/raw folder. These folders are generated from raw_datasets.json therefore SUB_DIR must be one of the keys found in that file."
  exit 2
fi

# ensure build/raw/*/*.link files are updated
make download_links

# prints valid options for SUB_DIR if incorrect ones are given.
if [ ! -d "build/raw/$1" ]; then
    echo "invalid SUB_DIR. Valid options are:"
    for name in $(find build/raw -maxdepth 1 -mindepth 1 | sed -r 's/^build\/raw\///')
    do
        echo "  $name"
    done
    exit 1
fi

# download raw files
make $(find build/raw/$1 -name '*.link' | sed -r 's/build\/(.+)\.link/data\/\1/' | tr '\n' ' ')

# print raw files under $1
echo "$(find build/raw/$1 -name '*.link' | sed -r 's/build\/(.+)\.link/\1/')"
