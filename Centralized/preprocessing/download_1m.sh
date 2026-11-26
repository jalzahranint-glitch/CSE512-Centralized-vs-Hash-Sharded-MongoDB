#!/bin/bash
set -euo pipefail

mkdir -p data/raw

OUTPUT="data/raw/nyc_speed_1m.csv"
TEMP="data/raw/nyc_speed_1m.part.csv"

if [ -f "$OUTPUT" ]; then
    echo "1M file already exists."
    exit 0
fi

echo "Downloading 1,000,000 NYC speed violations..."

curl -fLG --show-error --progress-bar \
  --retry 2 \
  --connect-timeout 30 \
  --max-time 1800 \
  'https://data.cityofnewyork.us/resource/nc67-uf89.csv' \
  --data-urlencode "\$where=violation = 'PHTO SCHOOL ZN SPEED VIOLATION'" \
  --data-urlencode '$limit=1000000' \
  -o "$TEMP"

echo "Validating downloaded data..."

python3 - "$TEMP" "$OUTPUT" <<'PY'
import csv
import os
import sys

temp, output = sys.argv[1:]
expected_type = "PHTO SCHOOL ZN SPEED VIOLATION"

ids = set()
total = 0
duplicates = 0
wrong_types = 0
missing_ids = 0

with open(temp, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    columns = reader.fieldnames or []

    if "summons_number" not in columns or "violation" not in columns:
        raise SystemExit("ERROR: Required columns missing")

    for row in reader:
        total += 1
        sid = row["summons_number"]

        if not sid:
            missing_ids += 1
        elif sid in ids:
            duplicates += 1
        else:
            ids.add(sid)

        if row["violation"] != expected_type:
            wrong_types += 1

print("Records:", total)
print("Columns:", len(columns))
print("Duplicate IDs:", duplicates)
print("Missing IDs:", missing_ids)
print("Wrong violation types:", wrong_types)

if (
    total == 1000000
    and len(columns) == 19
    and duplicates == 0
    and missing_ids == 0
    and wrong_types == 0
):
    os.replace(temp, output)
    print("RESULT: PASS")
    print("Saved:", output)
else:
    print("RESULT: NEEDS REVIEW")
    raise SystemExit(1)
PY
