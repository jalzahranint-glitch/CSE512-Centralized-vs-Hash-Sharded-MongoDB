import csv
from pathlib import Path
from datetime import datetime, timezone
from pymongo import MongoClient, ReplaceOne

BASE = Path(__file__).resolve().parent.parent
# Set the path to the cleaned 100K dataset.
FILE = BASE / "data/processed/nyc_speed_100k_clean.csv"

# Connect to the local MongoDB server.
client = MongoClient(
    "mongodb://127.0.0.1:27017/",
    serverSelectionTimeoutMS=10000
)

# Check that MongoDB is available.
client.admin.command("ping")

db = client["nyc_traffic"]
# Select the collection for the 100K dataset.
collection = db["violations_100k"]

batch = []
total = 0

# Read the cleaned CSV file.
with FILE.open(newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)

    for row in reader:
        # Reject records with missing vehicle hashes or dates.
        if not row["vehicle_hash"] or not row["issue_date"]:
            raise ValueError("Missing vehicle hash or date")

        # Prepare a MongoDB document from the CSV record.
        document = {
            # Use the summons number as the document ID.
            "_id": row["summons_number"],
            "vehicle_hash": row["vehicle_hash"],
            # Convert the date string to a UTC datetime.
            "issue_date": datetime.strptime(
                row["issue_date"], "%Y-%m-%d"
            ).replace(tzinfo=timezone.utc),
            "zone": row["zone"],
            "violation": row["violation"]
        }

        # Add a replace-or-insert operation to the batch.
        batch.append(
            ReplaceOne(
                {"_id": document["_id"]},
                document,
                upsert=True
            )
        )

        # Write each batch of 1000 records to MongoDB.
        if len(batch) == 1000:
            collection.bulk_write(batch, ordered=False)
            total += len(batch)
            batch.clear()
            print(f"Processed: {total:,}", flush=True)

# Write any remaining records after the loop.
if batch:
    collection.bulk_write(batch, ordered=False)
    total += len(batch)

# Display the import summary.
print("Input records:", total)
print("MongoDB records:", collection.count_documents({}))
print("Database:", db.name)
print("Collection:", collection.name)

# Close the MongoDB connection.
client.close()
