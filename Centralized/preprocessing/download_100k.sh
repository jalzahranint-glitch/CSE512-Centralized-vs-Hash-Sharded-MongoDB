#!/bin/bash
set -e

mkdir -p data/raw

OUTPUT="data/raw/nyc_speed_100k_candidate.csv"

# Avoid downloading again if the file exists
if [ -f "$OUTPUT" ]; then
    echo "100K dataset already exists."
    exit 0
fi

curl -fLG --show-error --progress-bar \
  --retry 3 \
  --connect-timeout 30 \
  --max-time 600 \
  'https://data.cityofnewyork.us/resource/nc67-uf89.csv' \
  --data-urlencode "\$where=violation = 'PHTO SCHOOL ZN SPEED VIOLATION'" \
  --data-urlencode '$limit=100000' \
  -o "$OUTPUT"

echo "Download completed."
