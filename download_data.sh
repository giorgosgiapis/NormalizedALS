#!/bin/bash
if [ $# -lt 1 ]; then
  echo 1>&2 "$0: missing dataset type argument (small or large)"
  exit 2
fi
if [ "$1" != "small" ] && [ "$1" != "large" ]; then
  echo 1>&2 "$0: invalid dataset type argument (must be small or large)"
  exit 2
fi
if [ "$1" == "small" ]; then
  if [[ "$OSTYPE" == "darwin"* ]]; then
    curl -O https://files.grouplens.org/datasets/movielens/ml-latest-small.zip
  else
    wget https://files.grouplens.org/datasets/movielens/ml-latest-small.zip
  fi
  unzip -o ml-latest-small.zip -d .
  rm -rf data_small
  mv ml-latest-small data_small
  rm -f ml-latest-small.zip
else
  if [[ "$OSTYPE" == "darwin"* ]]; then
    curl -O https://files.grouplens.org/datasets/movielens/ml-latest.zip
  else
    wget https://files.grouplens.org/datasets/movielens/ml-latest.zip
  fi
  unzip -o ml-latest.zip -d .
  rm -rf data_large
  mv ml-latest data_large
  rm -f ml-latest.zip
fi
