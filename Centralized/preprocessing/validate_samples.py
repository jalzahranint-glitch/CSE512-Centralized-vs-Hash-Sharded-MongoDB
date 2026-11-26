import csv
import json
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
DATA = BASE / "data/processed"
RESULTS = BASE / "results"

FIELDS = ["vehicle_hash", "issue_date", "zone", "violation"]

def load_sample(filename, expected):
    records = {}
    duplicates = 0
    total = 0

    with (DATA / filename).open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            total += 1
            sid = row["summons_number"]

            if sid in records:
                duplicates += 1

            records[sid] = tuple(row[field] for field in FIELDS)

    print(f"{filename}: {total:,} records")
    assert total == expected, "Incorrect record count"
    assert duplicates == 0, "Duplicate IDs found"
    return records

print("Validating samples...")

sample100 = load_sample("nyc_speed_100k_clean.csv", 100000)
sample500 = load_sample("nyc_speed_500k_clean.csv", 500000)

# Check 100K against 500K
assert all(
    sample500.get(sid) == values
    for sid, values in sample100.items()
), "100K and 500K do not match"

print("100K vs 500K: PASS")

# Check 500K against 1M
matched = set()
total = 0

with (DATA / "nyc_speed_1m_clean.csv").open(
    newline="", encoding="utf-8"
) as f:
    for row in csv.DictReader(f):
        total += 1
        sid = row["summons_number"]

        if sid in sample500:
            values = tuple(row[field] for field in FIELDS)
            assert sample500[sid] == values, "Record mismatch"
            matched.add(sid)

assert total == 1000000, "Incorrect 1M count"
assert len(matched) == 500000, "Missing records from 1M"

print("500K vs 1M: PASS")

RESULTS.mkdir(exist_ok=True)

summary = {
    "sample_100k": 100000,
    "sample_500k": 500000,
    "sample_1m": 1000000,
    "nested_samples": True,
    "vehicle_hash_consistency": "PASS"
}

(RESULTS / "sample_validation.json").write_text(
    json.dumps(summary, indent=2)
)

print("FINAL RESULT: PASS")
print("Saved validation summary.")
