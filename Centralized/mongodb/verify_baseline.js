const database = db.getSiblingDB("nyc_traffic");

const samples = [
  ["violations_100k", 100000],
  ["violations_500k", 500000],
  ["violations_1m", 1000000]
];

for (const [name, expected] of samples) {
  const actual = database.getCollection(name).countDocuments({});
  const status = actual === expected ? "PASS" : "FAIL";

  print(`${name}: ${actual} records - ${status}`);

  if (status === "FAIL") {
    throw new Error(`Validation failed for ${name}`);
  }
}

print("BASELINE VALIDATION: PASS");
