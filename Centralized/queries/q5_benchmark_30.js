const database = db.getSiblingDB("nyc_traffic");
const sizes = ["100k", "500k", "1m"];

// Define the aggregation pipeline for monthly repeat offenders.
const pipeline = [
  {
    $match: {
      // Filter records with non-empty vehicle hashes and valid dates.
      vehicle_hash: { $type: "string", $ne: "" },
      issue_date: { $type: "date" }
    }
  },
  {
    $group: {
      _id: {
        vehicle: "$vehicle_hash",
        month: {
          $dateToString: {
            // Extract the year and month using UTC.
            format: "%Y-%m",
            date: "$issue_date",
            timezone: "UTC"
          }
        }
      },
      // Count violations for each vehicle within the same month.
      violation_count: { $sum: 1 }
    }
  },
  {
    $match: {
      // Keep vehicles with at least two violations in one month.
      violation_count: { $gte: 2 }
    }
  },
  {
    $group: {
      // Group the qualifying vehicle records by month.
      _id: "$_id.month",
      // Count the repeat-offender vehicles for each month.
      repeat_offender_vehicles: { $sum: 1 },
      // Calculate total violations from repeat offenders.
      repeat_violations: { $sum: "$violation_count" },
      max_violations_per_vehicle: {
        // Find the highest monthly violation count for one vehicle.
        $max: "$violation_count"
      }
    }
  },
  {
    $sort: {
      // Rank months by repeat-offender count.
      repeat_offender_vehicles: -1,
      _id: 1
    }
  },
  // Return the 10 months with the most repeat offenders.
  { $limit: 10 },
  {
    // Format the final monthly statistics.
    $project: {
      _id: 0,
      month: "$_id",
      repeat_offender_vehicles: 1,
      repeat_violations: 1,
      max_violations_per_vehicle: 1
    }
  }
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

    // Execute the aggregation pipeline.
    const results = collection.aggregate(
      pipeline,
      { allowDiskUse: true }
    ).toArray();

    const elapsed = Date.now() - start;

    if (results.length !== 10) {
      throw new Error("Unexpected Q5 result count: " + size);
    }

    // Verify that the query results remain consistent across runs.
    const signature = JSON.stringify(results);

    if (expected === null) {
      expected = signature;
    } else if (signature !== expected) {
      throw new Error("Q5 results changed: " + size);
    }

    // Exclude warm-up runs from latency measurements.
    if (run > 5) {
      times.push(elapsed);

      print(JSON.stringify({
        type: "measurement",
        query: "Q5",
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
    query: "Q5",
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
