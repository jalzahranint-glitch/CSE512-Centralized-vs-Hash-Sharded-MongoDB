#!/bin/bash
set -e

mkdir -p data/raw

curl -fLG --show-error --progress-bar \
  --retry 3 \
  --connect-timeout 30 \
  --max-time 1200 \
  'https://data.cityofnewyork.us/resource/nc67-uf89.csv' \
  --data-urlencode "\$where=violation = 'PHTO SCHOOL ZN SPEED VIOLATION'" \
  --data-urlencode '$limit=500000' \
  -o data/raw/nyc_speed_500k_candidate.csv
