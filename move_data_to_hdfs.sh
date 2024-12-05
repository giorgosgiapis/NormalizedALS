#!/bin/bash
if [ ! -d "data" ]; then
  ./download_data.sh
fi
hdfs dfs -put data/* data/
