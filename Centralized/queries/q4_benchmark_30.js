const database = db.getSiblingDB("nyc_traffic");
const sizes = ["100k", "500k", "1m"];

// Define the aggregation pipeline for monthly violations by borough.
const pipeline = [
  {
    // Filter records by date type and the five NYC boroughs.
    $match: {
      issue_date: { $type: "date" },
      zone: {
        $in: [
          "Bronx",
          "Brooklyn",
          "Manhattan",
          "Queens",
          "Staten Island"
        ]
      }
    }
  },
  {
    // Group violations by borough and month using UTC dates.
    $group: {
      _id: {
        borough: "$zone",
        month: {
          $dateToString: {
            format: "%Y-%m",
            date: "$issue_date",
            timezone: "UTC"
          }
        }
      },
      // Count the total violations in each borough and month.
      total_violations: { $sum: 1 }
    }
  },
  {
    // Sort by violation count, using borough and month to break ties.
    $sort: {
      total_violations: -1,
      "_id.borough": 1,
      "_id.month": 1
    }
  },
  // Return the 10 borough-month groups with the most violations.
  { $limit: 10 },
  {
    // Format the output with borough, month, and violation count.
    $project: {
      _id: 0,
      borough: "$_id.borough",
      month: "$_id.month",
      total_violations: 1
    }
  }
];


// Run the query on each dataset size.
for (const size of sizes) {
  const collection = database.getCollection("violations_" + size);
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
    // Measure query latency in milliseconds.
    const elapsed = Date.now() - start;

    // Verify that the query returns exactly 10 results.
    if (results.length !== 10) {
      throw new Error("Unexpected Q4 result count: " + size);
    }

    // Compare results to ensure consistency across runs.
    const signature = JSON.stringify(results);
    if (expected === null) {
      expected = signature;
    } else if (signature !== expected) {
      throw new Error("Q4 results changed: " + size);
    }

    // Exclude warm-up runs from latency measurements.
    if (run > 5) {
      times.push(elapsed);
      print(JSON.stringify({
        type: "measurement",
        query: "Q4",
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
    query: "Q4",
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
