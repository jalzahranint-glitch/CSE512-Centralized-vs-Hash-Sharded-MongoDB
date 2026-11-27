var q5db = db.getSiblingDB("nyc_traffic");

// Define the three dataset sizes used in the benchmark.
var q5sizes = [
  { size: "100K", name: "violations_100k" },
  { size: "500K", name: "violations_500k" },
  { size: "1M", name: "violations_1m" }
];

// Define the aggregation pipeline for monthly repeat-offender analysis.
var q5pipeline = [
  {
    $match: {
      // Keep records with non-empty vehicle hashes and valid dates.
      vehicle_hash: { $type: "string", $ne: "" },
      issue_date: { $type: "date" }
    }
  },
  {
    $group: {
      _id: {
        // Group violations by vehicle and month.
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
      // Group qualifying vehicle records by month.
      _id: "$_id.month",
      // Count repeat-offender vehicles for each month.
      repeat_offender_vehicles: { $sum: 1 },
      // Calculate the total violations from repeat offenders.
      repeat_violations: { $sum: "$violation_count" },
      max_violations_per_vehicle: {
        // Find the highest monthly violation count for one vehicle.
        $max: "$violation_count"
      }
    }
  },
  {
    $sort: {
      // Rank months by the number of repeat offenders.
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

// Set five warm-up runs and 30 measured runs.
var Q5_WARMUP = 5;
var Q5_RUNS = 30;

// Verify that the database connection uses mongos.
if (db.adminCommand({ hello: 1 }).msg !== "isdbgrid") {
  throw new Error("Not connected to mongos");
}

// Execute the aggregation pipeline on the selected dataset.
function runQ5(name) {
  return q5db.getCollection(name)
    .aggregate(q5pipeline, { allowDiskUse: true })
    .toArray();
}

// Calculate median and P95 latency.
function q5Timing(values) {
  var sorted = values.slice().sort((a, b) => a - b);
  var mid = Math.floor(sorted.length / 2);

  return {
    median: sorted.length % 2 === 0
      ? (sorted[mid - 1] + sorted[mid]) / 2
      : sorted[mid],
    p95: sorted[Math.ceil(0.95 * sorted.length) - 1]
  };
}

// Find execution statistics within the explain output.
function q5FindStats(node) {
  if (!node || typeof node !== "object") return null;

  if (
    node.executionStats &&
    node.executionStats.totalDocsExamined != null
  ) {
    return node.executionStats;
  }

  if (Array.isArray(node)) {
    for (var item of node) {
      var found = q5FindStats(item);
      if (found) return found;
    }
    return null;
  }

  for (var key of Object.keys(node)) {
    var found = q5FindStats(node[key]);
    if (found) return found;
  }

  return null;
}

// Analyze shard involvement and execution statistics.
function q5Analyze(explain) {
  var shardStats = [];
  var shardNames = [];
  var stages = new Set();

  // Recursively collect query execution stages.
  function scan(node) {
    if (!node || typeof node !== "object") return;

    if (Array.isArray(node)) {
      node.forEach(scan);
      return;
    }

    if (typeof node.stage === "string") {
      stages.add(node.stage);
    }

    Object.keys(node).forEach(function(key) {
      scan(node[key]);
    });
  }

  scan(explain);

  // Read available execution statistics for each shard.
  if (explain.shards && typeof explain.shards === "object") {
    var entries = Array.isArray(explain.shards)
      ? explain.shards.map(x => [x.shardName, x])
      : Object.entries(explain.shards);

    for (var entry of entries) {
      var name = entry[0];
      if (!name) continue;

      shardNames.push(name);

      var stats = q5FindStats(entry[1]);

      if (stats) {
        shardStats.push({
          shard: name,
          docs: Number(stats.totalDocsExamined),
          keys: Number(stats.totalKeysExamined || 0),
          explainMillis: stats.executionTimeMillis ?? null
        });
      }
    }
  }

  var root = explain.executionStats;

  var rootDocs = root?.totalDocsExamined;
  var rootKeys = root?.totalKeysExamined;

  // Check whether all identified shards have execution statistics.
  var complete =
    shardNames.length > 0 &&
    shardStats.length === shardNames.length;

  // Use root document counts or complete per-shard totals.
  var docs = rootDocs != null
    ? Number(rootDocs)
    : complete
      ? shardStats.reduce((sum, s) => sum + s.docs, 0)
      : null;

  var keys = rootKeys != null
    ? Number(rootKeys)
    : complete
      ? shardStats.reduce((sum, s) => sum + s.keys, 0)
      : null;

  return {
    shards: shardNames.sort(),
    stages: [...stages].sort(),
    docs: docs,
    keys: keys,
    explainMillis: root?.executionTimeMillis ?? null,
    perShardStats: shardStats
  };
}

print(" Q5 DISTRIBUTED MONTHLY REPEAT OFFENDERS ");
print("Warmup runs: " + Q5_WARMUP);
print("Measured runs: " + Q5_RUNS);

// Run Q5 for each dataset size.
for (var dataset of q5sizes) {
  print("\n " + dataset.size + " ");

  var results = runQ5(dataset.name);

  // Perform warm-up runs before collecting measurements.
  for (var w = 0; w < Q5_WARMUP; w++) {
    runQ5(dataset.name);
  }

  var latencies = [];

  // Measure client-side query latency over 30 runs.
  for (var r = 0; r < Q5_RUNS; r++) {
    var start = Date.now();
    runQ5(dataset.name);
    latencies.push(Date.now() - start);
  }

  // Calculate median and P95 from the measured latencies.
  var timing = q5Timing(latencies);

  // Collect query execution statistics using explain.
  var explain = q5db.getCollection(dataset.name)
    .explain("executionStats")
    .aggregate(q5pipeline, { allowDiskUse: true });

  // Extract shard information and execution plan details.
  var plan = q5Analyze(explain);

  print(" Q5 Top 10 Results ");
  printjson(results);

  print(" Q5 Performance ");
  printjson({
    query: "Q5 - Monthly Repeat-Offender Analysis",
    dataset: dataset.size,
    mode: "Distributed - Hash Sharding",
    collection: dataset.name,

    warmupRuns: Q5_WARMUP,
    measuredRuns: Q5_RUNS,

    clientLatencyMs: latencies,
    medianLatencyMs: timing.median,
    p95LatencyMs: timing.p95,

    explainExecutionTimeMillis: plan.explainMillis,
    // Set server execution time to null because it is not measured here.
    serverExecutionTimeMillis: null,

    totalDocsExamined: plan.docs,
    totalKeysExamined: plan.keys,
    recordsReturned: results.length,

    shardsInvolved: plan.shards,
    shardCount: plan.shards.length,
    queryPlanStages: plan.stages,
    perShardExplainStats: plan.perShardStats
  });
}

print("\n Q5 ALL SIZES COMPLETED ");
