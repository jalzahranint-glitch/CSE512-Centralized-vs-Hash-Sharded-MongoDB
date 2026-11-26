const database = db.getSiblingDB("nyc_traffic");
const sizes = ["100k", "500k", "1m"];

// Define the aggregation pipeline for the top 10 vehicles.
const pipeline = [
  {
    // Group records by vehicle hash and count violations.
    $group: {
      _id: "$vehicle_hash",
      violation_count: { $sum: 1 }
    }
  },
  {
    // Sort vehicles by violation count in descending order.
    $sort: {
      violation_count: -1,
      _id: 1
    }
  },
  // Return the 10 vehicles with the highest violation counts.
  { $limit: 10 }
];

// Run the query on each dataset size.
for (const size of sizes) {
  const collection =
    database.getCollection("violations_" + size);

  const times = [];
  let expected = null;

  // Perform five warm-up runs and 30 measured runs.
  for (let run = 1; run <= 35; run++) {
    const start = Date.now();

    // Execute the aggregation pipeline and collect the results.
    const results = collection.aggregate(
      pipeline,
      { allowDiskUse: true }
    ).toArray();

    const elapsed = Date.now() - start;

    // Verify that the query returns exactly 10 records.
    if (results.length !== 10) {
      throw new Error("Unexpected Q2 results: " + size);
    }

    // Check that the results remain consistent across runs.
    const signature = JSON.stringify(results);

    if (expected === null) {
      expected = signature;
    } else if (signature !== expected) {
      throw new Error("Q2 results changed: " + size);
    }

    // Exclude warm-up runs from the recorded measurements.
    if (run > 5) {
      times.push(elapsed);

      print(JSON.stringify({
        type: "measurement",
        query: "Q2",
        system: "centralized",
        dataset: size,
        run: run - 5,
        latencyMillis: elapsed,
        recordsReturned: results.length
      }));
    }
  }

  // Sort latency measurements to calculate median and P95.
  const sorted = [...times].sort((a, b) => a - b);

  print(JSON.stringify({
    type: "summary",
    query: "Q2",
    system: "centralized",
    dataset: size,
    warmupRuns: 5,
    measuredRuns: 30,
    medianMillis: (sorted[14] + sorted[15]) / 2,
    p95Millis: sorted[28],
    minMillis: sorted[0],
    maxMillis: sorted[29],
    recordsReturned: 10,
    resultsConsistent: true
  }));
}
