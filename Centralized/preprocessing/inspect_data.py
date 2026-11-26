import pandas as pd

df = pd.read_csv(
    "data/raw/nyc_speed_test_1000.csv",
    dtype=str
)

print("Total records:", len(df))
print("Total columns:", len(df.columns))
print("Duplicate IDs:", df["summons_number"].duplicated().sum())

print("\nMissing values:")
print(df.isna().sum())

print("\nViolation types:")
print(df["violation"].value_counts())
vehicle_cols = ["plate", "state", "license_type"]

vehicles = (
    df[vehicle_cols]
    .fillna("")
    .apply(lambda col: col.str.strip().str.upper())
)

vehicle_counts = vehicles.value_counts()

print("\nUnique vehicles:", len(vehicle_counts))
print("Vehicles with repeated violations:",
      (vehicle_counts > 1).sum())

dates = pd.to_datetime(
    df["issue_date"],
    format="%m/%d/%Y",
    errors="coerce"
)

print("Earliest date:", dates.min())
print("Latest date:", dates.max())