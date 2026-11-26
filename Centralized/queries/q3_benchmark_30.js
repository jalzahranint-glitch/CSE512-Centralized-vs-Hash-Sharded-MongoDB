const database = db.getSiblingDB("nyc_traffic");
const sizes = ["100k", "500k", "1m"];

// Define the aggregation pipeline for vehicles with violations in multiple zones.
const pipeline = [
  {
    $match: {
      // Keep records with non-empty string values for vehicle hash and zone.
      vehicle_hash: { $type: "string", $ne: "" },
      zone: { $type: "string", $ne: "" }
    }
  },
  {
    $group: {
      _id: "$vehicle_hash",
      // Collect distinct zones for each vehicle.
      boroughs: { $addToSet: "$zone" },
      // Count the total violations for each vehicle.
      total_violations: { $sum: 1 }
    }
  },
  {
    $project: {
      boroughs: 1,
      // Calculate the number of distinct zones.
      borough_count: { $size: "$boroughs" },
      total_violations: 1
    }
  },
  {
    $match: {
      // Keep vehicles with violations in at least two zones.
      borough_count: { $gte: 2 }
    }
  },
  {
    $sort: {
      // Rank vehicles by zone count and total violations.
      borough_count: -1,
      total_violations: -1,
      _id: 1
    }
  },
  // Return the top 10 vehicles based on the ranking.
  { $limit: 10 }
];

// Normalize query results to compare them across runs.
function normalized(results) {
  return JSON.stringify(results.map(r => ({
    id: r._id,
    // Sort zone values to ensure consistent comparisons.
    boroughs: [...r.boroughs].sort(),
    borough_count: r.borough_count,
    total_violations: r.total_violations
  })));
}

// Run the query on each dataset size.
for (const size of sizes) {
  const collection =
    database.getCollection("violations_" + size);

  const times = [];
  let expected = null;

  // Perform five warm-up runs and 30 measured runs.
  for (let run = 1; run <= 35; run++) {
    const start = Date.now();

    // Execute the aggregation pipeline.
    const results = collection.aggregate(
      pipeline,
      { allowDiskUse: true }
    ).toArray();

    const elapsed = Date.now() - start;

    if (results.length !== 10) {
      throw new Error("Unexpected Q3 result count: " + size);
    }

    // Verify that query results remain consistent across runs.
    const signature = normalized(results);

    if (expected === null) {
      expected = signature;
    } else if (signature !== expected) {
      throw new Error("Q3 results changed: " + size);
    }

    // Exclude warm-up runs from latency measurements.
    if (run > 5) {
      times.push(elapsed);

      print(JSON.stringify({
        type: "measurement",
        query: "Q3",
        system: "centralized",
        dataset: size,
        run: run - 5,
        latencyMillis: elapsed,
        recordsReturned: results.length
      }));
    }
  }

  // Sort latency values to calculate median and P95.
  const sorted = [...times].sort((a, b) => a - b);

  print(JSON.stringify({
    type: "summary",
    query: "Q3",
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
