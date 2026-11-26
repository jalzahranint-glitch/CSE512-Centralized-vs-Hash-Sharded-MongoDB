import csv
import json
import time
from pathlib import Path
import requests

BASE = Path(__file__).resolve().parent.parent
OUTPUT = BASE / "data/raw/nyc_speed_100k_download.csv"
OUTPUT.parent.mkdir(parents=True, exist_ok=True)

URL = "https://data.cityofnewyork.us/resource/nc67-uf89.json"
TARGET = 100000
BATCH_SIZE = 2500

columns = [
    "plate", "state", "license_type", "summons_number",
    "issue_date", "violation_time", "violation",
    "judgment_entry_date", "fine_amount", "penalty_amount",
    "interest_amount", "reduction_amount", "payment_amount",
    "amount_due", "precinct", "county", "issuing_agency",
    "violation_status", "summons_image"
]

count = 0
last_id = 0

# Resume an interrupted download
if OUTPUT.exists():
    with OUTPUT.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            count += 1
            last_id = int(row["summons_number"])

print(f"Starting from {count:,} records", flush=True)

session = requests.Session()

with OUTPUT.open("a", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=columns)

    if count == 0:
        writer.writeheader()

    while count < TARGET:
        params = {
            "$where": (
                "violation = 'PHTO SCHOOL ZN SPEED VIOLATION' "
                f"AND summons_number > {last_id}"
            ),
            "$order": "summons_number ASC",
            "$limit": min(BATCH_SIZE, TARGET - count)
        }

        for attempt in range(3):
            try:
                response = session.get(
                    URL, params=params, timeout=(15, 120)
                )
                response.raise_for_status()
                records = response.json()
                break
            except requests.RequestException:
                if attempt == 2:
                    raise
                time.sleep(5)

        if not records:
            print("No more matching records.")
            break

        for record in records:
            row = {
                col: record.get(col, "") for col in columns
            }

            if isinstance(row["summons_image"], dict):
                row["summons_image"] = json.dumps(
                    row["summons_image"]
                )

            writer.writerow(row)
            last_id = int(record["summons_number"])
            count += 1

        f.flush()
        print(f"Downloaded: {count:,}/{TARGET:,}", flush=True)

print("Finished. Total records:", count)
print("Saved to:", OUTPUT)
