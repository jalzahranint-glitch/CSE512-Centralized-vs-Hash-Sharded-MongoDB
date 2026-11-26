import requests
import csv
import json
from pathlib import Path

url = "https://data.cityofnewyork.us/resource/nc67-uf89.json"

params = {
    "$where": "violation = 'PHTO SCHOOL ZN SPEED VIOLATION'",
    "$limit": 1000
}

columns = [
    "plate",
    "state",
    "license_type",
    "summons_number",
    "issue_date",
    "violation_time",
    "violation",
    "judgment_entry_date",
    "fine_amount",
    "penalty_amount",
    "interest_amount",
    "reduction_amount",
    "payment_amount",
    "amount_due",
    "precinct",
    "county",
    "issuing_agency",
    "violation_status",
    "summons_image"
]

print("Connecting to NYC Open Data...", flush=True)

response = requests.get(url, params=params, timeout=120)
response.raise_for_status()

records = response.json()

output = (
    Path(__file__).resolve().parent.parent
    / "data/raw/nyc_speed_test_1000_full.csv"
)

output.parent.mkdir(parents=True, exist_ok=True)

with open(output, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=columns)
    writer.writeheader()

    for record in records:
        row = {col: record.get(col, "") for col in columns}

        if isinstance(row["summons_image"], dict):
            row["summons_image"] = json.dumps(row["summons_image"])

        writer.writerow(row)

print("Downloaded records:", len(records))
print("Total columns:", len(columns))
print("Violation types:", set(r.get("violation") for r in records))
print("Saved to:", output)
