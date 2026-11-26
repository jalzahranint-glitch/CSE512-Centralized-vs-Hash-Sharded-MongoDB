import csv
import hmac
import hashlib
import json
import secrets
from pathlib import Path
from datetime import datetime

# Set the base directory for the project.
BASE = Path(__file__).resolve().parent.parent

# Specify the input CSV file.
SOURCE = BASE / "data/raw/nyc_speed_100k_final.csv"
OUTPUT = BASE / "data/processed/nyc_speed_100k_clean.csv"
# Specify the location of the private HMAC key.
KEY_FILE = BASE / "data/private/vehicle_hmac.key"

OUTPUT.parent.mkdir(parents=True, exist_ok=True)
KEY_FILE.parent.mkdir(parents=True, exist_ok=True)

# Create a private key only once
if not KEY_FILE.exists():
    KEY_FILE.write_bytes(secrets.token_bytes(32))
    KEY_FILE.chmod(0o600)

# Read the key used to generate vehicle hashes.
secret_key = KEY_FILE.read_bytes()

# Map county codes to borough names.
borough_map = {
    "QN": "Queens",
    "BK": "Brooklyn",
    "BX": "Bronx",
    "ST": "Staten Island",
    "MN": "Manhattan"
}

total = 0
missing_vehicle = 0
invalid_dates = 0
unknown_boroughs = 0

with SOURCE.open(newline="", encoding="utf-8") as src, \
     OUTPUT.open("w", newline="", encoding="utf-8") as dst:

    reader = csv.DictReader(src)

    columns = [
        "summons_number",
        "vehicle_hash",
        "issue_date",
        "zone",
        "violation"
    ]

    writer = csv.DictWriter(dst, fieldnames=columns)
    writer.writeheader()

    # Process each record in the input CSV.
    for row in reader:
        total += 1

        # Normalize vehicle identity fields.
        vehicle = [
            (row.get(c) or "").strip().upper()
            for c in ["plate", "state", "license_type"]
        ]

        # Check that all vehicle identity fields are present.
        if all(vehicle):
            identity = json.dumps(
                vehicle, separators=(",", ":")
            ).encode("utf-8")

            # Generate the vehicle hash using HMAC-SHA256.
            vehicle_hash = hmac.new(
                secret_key, identity, hashlib.sha256
            ).hexdigest()
        else:
            missing_vehicle += 1
            vehicle_hash = ""

        try:
            # Convert dates to YYYY-MM-DD format.
            date = datetime.strptime(
                row["issue_date"], "%m/%d/%Y"
            ).strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            invalid_dates += 1
            date = ""

        county = (row.get("county") or "").strip().upper()
        # Convert county codes to borough names.
        zone = borough_map.get(county, "UNKNOWN")

        if zone == "UNKNOWN":
            unknown_boroughs += 1

        # Write the cleaned record to the output CSV.
        writer.writerow({
            "summons_number": row["summons_number"],
            "vehicle_hash": vehicle_hash,
            "issue_date": date,
            "zone": zone,
            "violation": row["violation"]
        })

# Display the data cleaning summary.
print("Cleaned records:", total)
print("Output columns:", len(columns))
print("Missing vehicle identities:", missing_vehicle)
print("Invalid dates:", invalid_dates)
print("Unknown boroughs:", unknown_boroughs)
print("Saved:", OUTPUT)
