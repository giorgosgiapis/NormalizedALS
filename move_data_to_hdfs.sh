#!/bin/bash
if [ $# -lt 1 ]; then
  echo 1>&2 "$0: missing dataset type argument (small or large)"
  exit 2
fi
if [ "$1" != "small" ] && [ "$1" != "large" ]; then
  echo 1>&2 "$0: invalid dataset type argument (must be small or large)"
  exit 2
fi
if [ ! -d "data_""$1" ]; then
  ./download_data.sh "$1"
fi
hdfs dfs -mkdir data_"$1"
hdfs dfs -put data_"$1"/* data_"$1"/
