
import csv
import sys
from datetime import datetime, timezone
from pathlib import Path

from pymongo import MongoClient, ReplaceOne

# Distributed MongoDB connection (mongos)
MONGO_URI = "mongodb://127.0.0.1:27019"

DB_NAME = "nyc_traffic"
BATCH_SIZE = 1000

DATA_DIR = Path(__file__).resolve().parents[2] / "Centralized" / "data" / "processed"

DATASETS = {
    "100k": ("nyc_speed_100k_clean.csv", "violations_100k", 100000),
    "500k": ("nyc_speed_500k_clean.csv", "violations_500k", 500000),
    "1m": ("nyc_speed_1m_clean.csv", "violations_1m", 1000000),
}


def main():
    if len(sys.argv) != 2 or sys.argv[1] not in DATASETS:
        print("Usage: python3 scripts/load_dataset.py 100k|500k|1m")
        sys.exit(1)

    size = sys.argv[1]
    filename, collection_name, expected = DATASETS[size]
    csv_file = DATA_DIR / filename

    if not csv_file.is_file():
        raise FileNotFoundError(csv_file)

    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=15000,
    )

    try:
        hello = client.admin.command("hello")
        if hello.get("msg") != "isdbgrid":
            raise RuntimeError("Connection is not to mongos!")

        collection = client[DB_NAME][collection_name]

        print("Connected to mongos: YES", flush=True)
        print("Database:", DB_NAME, flush=True)
        print("Collection:", collection_name, flush=True)
        print("CSV:", csv_file, flush=True)

        batch = []
        total = 0

        with csv_file.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)

            for row in reader:
                if not row["vehicle_hash"] or not row["issue_date"]:
                    raise ValueError("Missing vehicle hash or date")

                document = {
                    "_id": row["summons_number"],
                    "vehicle_hash": row["vehicle_hash"],
                    "issue_date": datetime.strptime(
                        row["issue_date"], "%Y-%m-%d"
                    ).replace(tzinfo=timezone.utc),
                    "zone": row["zone"],
                    "violation": row["violation"],
                }

                batch.append(
                    ReplaceOne(
                        {
                            "_id": document["_id"],
                            "vehicle_hash": document["vehicle_hash"],
                        },
                        document,
                        upsert=True,
                    )
                )

                if len(batch) == BATCH_SIZE:
                    collection.bulk_write(batch, ordered=False)
                    total += len(batch)
                    batch.clear()
                    print(f"Processed: {total:,}", flush=True)

            if batch:
                collection.bulk_write(batch, ordered=False)
                total += len(batch)

        stored = collection.count_documents({})

        print("Input records:", total)
        print("MongoDB records:", stored)
        print("Expected records:", expected)

        if total != expected or stored != expected:
            raise RuntimeError("FAIL: Record count mismatch")

        sample = collection.find_one({})
        print("Date type:", type(sample["issue_date"]).__name__)
        print("Validation: PASS")

    finally:
        client.close()


if __name__ == "__main__":
    main()
