import csv
import random
from pathlib import Path

base = Path(__file__).resolve().parent.parent
source = base / "data/raw/nyc_speed_1m.csv"

if not source.exists():
    raise SystemExit("ERROR: 1M file not found")

print("Checking 1M dataset...", flush=True)

with source.open(newline="", encoding="utf-8") as f:
    total = sum(1 for _ in csv.DictReader(f))

if total != 1_000_000:
    raise SystemExit(f"Expected 1M, found {total}")

rng = random.Random(42)
selected = rng.sample(range(total), 500_000)

sample_100k = set(selected[:100_000])
sample_500k = set(selected)

with source.open(newline="", encoding="utf-8") as src, \
     (base / "data/raw/nyc_speed_100k_final.csv").open(
         "w", newline="", encoding="utf-8") as f100, \
     (base / "data/raw/nyc_speed_500k_final.csv").open(
         "w", newline="", encoding="utf-8") as f500:

    reader = csv.DictReader(src)
    writer100 = csv.DictWriter(f100, fieldnames=reader.fieldnames)
    writer500 = csv.DictWriter(f500, fieldnames=reader.fieldnames)

    writer100.writeheader()
    writer500.writeheader()

    for i, row in enumerate(reader):
        if i in sample_100k:
            writer100.writerow(row)

        if i in sample_500k:
            writer500.writerow(row)

print("100K sample: Created")
print("500K sample: Created")
print("1M dataset: Ready")
print("All samples use the same source.")
