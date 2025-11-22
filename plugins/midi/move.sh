#!/usr/bin/env bash
set -e  # exit immediately on any error

# Require existing folders
if [ ! -d "/home/miles/Documents/abstract-data-pipeline/plugins/midi/data/json" ]; then
  echo "ERROR: ssssdata/csv does not exist."
  exit 1
fi

if [ ! -d "/home/miles/Documents/abstract-data-pipeline/plugins/midi/data/wav" ]; then
  echo "ERROR: data/wav does not exist."
  exit 1
fi

# Ensure there are CSV files
if ! find . -type f -name "*.csv" | grep -q .; then
  echo "ERROR: No .csv files found."
  exit 1
fi

# Ensure there are WAV files
if ! find . -type f -name "*.wav" | grep -q .; then
  echo "ERROR: No .wav files found."
  exit 1
fi

# Move the files
find . -type f -name "*.csv" -exec mv {} data/csv/ \;
find . -type f -name "*.wav" -exec mv {} data/wav/ \;

echo "Done: moved all .csv → data/csv and all .wav → data/wav"
