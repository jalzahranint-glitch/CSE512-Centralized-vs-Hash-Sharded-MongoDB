const database = db.getSiblingDB("nyc_traffic");
const sizes = ["100k", "500k", "1m"];
const vehicle = "e1b3dc8fd32093207a7a965292593e4601fe2825d72d02e58f37d0d438e84dd2";

const modes = [
  { name: "Collection Scan", hint: { $natural: 1 } },
  { name: "Hashed Index", hint: { vehicle_hash: "hashed" } }
];

function percentile(values, p) {
  return values[Math.ceil(values.length * p) - 1];
}

for (const size of sizes) {
  const collection = database.getCollection("violations_" + size);

  if (!collection.getIndexes().some(
    i => i.key.vehicle_hash === "hashed"
  )) {
    throw new Error("Missing hashed index: " + size);
  }

  // Store latency measurements for each query mode.
  const measurements = modes.map(() => []);

  // Run five warm-up iterations and 30 measured iterations.
  for (let run = 1; run <= 35; run++) {
    for (let m = 0; m < modes.length; m++) {
      const mode = modes[m];
      const start = Date.now();

      // Search for the selected vehicle using the current query mode.
      const docs = collection.find(
        { vehicle_hash: vehicle },
        {
          _id: 0,
          vehicle_hash: 1,
          issue_date: 1,
          zone: 1,
          violation: 1
        }
      ).hint(mode.hint).limit(10).toArray();

      // Measure query latency in milliseconds.
      const elapsed = Date.now() - start;

      if (docs.length === 0) {
        throw new Error("No vehicle found: " + size);
      }

      // Exclude warm-up runs from the recorded measurements.
      if (run > 5) {
        measurements[m].push(elapsed);

        print(JSON.stringify({
          type: "measurement",
          query: "Q1",
          dataset: size,
          mode: mode.name,
          run: run - 5,
          latencyMillis: elapsed,
          recordsReturned: docs.length
        }));
      }
    }
  }

  for (let m = 0; m < modes.length; m++) {
    // Sort the recorded times to calculate summary statistics.
    const times = [...measurements[m]].sort((a, b) => a - b);

    print(JSON.stringify({
      type: "summary",
      query: "Q1",
      dataset: size,
      mode: modes[m].name,
      warmupRuns: 5,
      measuredRuns: times.length,
      medianMillis: (times[14] + times[15]) / 2,
      p95Millis: percentile(times, 0.95),
      minMillis: times[0],
      maxMillis: times[29]
    }));
  }
}
