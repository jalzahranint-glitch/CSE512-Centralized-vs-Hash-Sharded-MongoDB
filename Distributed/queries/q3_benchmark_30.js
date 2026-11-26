var q3db = db.getSiblingDB("nyc_traffic");

// Define the three dataset sizes used in the benchmark.
var q3sizes = [
  { size: "100K", name: "violations_100k" },
  { size: "500K", name: "violations_500k" },
  { size: "1M", name: "violations_1m" }
];

// Define the aggregation pipeline for cross-zone vehicle detection.
var q3pipeline = [
  {
    $match: {
      // Keep records with non-empty vehicle hashes and zone values.
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

// Set five warm-up runs and 30 measured runs.
var Q3_WARMUP = 5;
var Q3_RUNS = 30;

// Verify that the connection is through mongos.
if (db.adminCommand({ hello: 1 }).msg !== "isdbgrid") {
  throw new Error("Not connected to mongos");
}

// Execute the aggregation pipeline on the selected dataset.
function runQ3(name) {
  return q3db.getCollection(name)
    .aggregate(q3pipeline, { allowDiskUse: true })
    .toArray();
}

// Calculate median and P95 latency.
function q3Timing(values) {
  var a = values.slice().sort((x, y) => x - y);
  var middle = Math.floor(a.length / 2);

  return {
    median: a.length % 2 === 0
      ? (a[middle - 1] + a[middle]) / 2
      : a[middle],
    p95: a[Math.ceil(0.95 * a.length) - 1]
  };
}

// Find available execution statistics in the explain output.
function q3FindStats(node) {
  if (!node || typeof node !== "object") {
    return null;
  }

  if (
    node.executionStats &&
    node.executionStats.totalDocsExamined != null
  ) {
    return node.executionStats;
  }

  if (Array.isArray(node)) {
    for (var item of node) {
      var result = q3FindStats(item);
      if (result) return result;
    }
    return null;
  }

  for (var key of Object.keys(node)) {
    var result = q3FindStats(node[key]);
    if (result) return result;
  }

  return null;
}

// Analyze the execution plan and shard statistics.
function q3Analyze(explain) {
  var shardStats = [];
  var stages = new Set();
  var shardNames = [];

  // Recursively collect query execution stages.
  function findStages(node) {
    if (!node || typeof node !== "object") return;

    if (Array.isArray(node)) {
      node.forEach(findStages);
      return;
    }

    if (typeof node.stage === "string") {
      stages.add(node.stage);
    }

    Object.keys(node).forEach(function(key) {
      findStages(node[key]);
    });
  }

  findStages(explain);

  // Read the available execution details for each shard.
  var sections = explain.shards;

  if (sections && typeof sections === "object") {
    var entries = Array.isArray(sections)
      ? sections.map(x => [x.shardName, x])
      : Object.entries(sections);

    for (var entry of entries) {
      var name = entry[0];

      if (!name) continue;

      shardNames.push(name);

      var stats = q3FindStats(entry[1]);

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

  // Check whether all identified shards have statistics.
  var allShardsMeasured =
    shardNames.length > 0 &&
    shardStats.length === shardNames.length;

  // Use root document counts or complete per-shard totals.
  var docs = rootDocs != null
    ? Number(rootDocs)
    : allShardsMeasured
      ? shardStats.reduce((sum, s) => sum + s.docs, 0)
      : null;

  var keys = rootKeys != null
    ? Number(rootKeys)
    : allShardsMeasured
      ? shardStats.reduce((sum, s) => sum + s.keys, 0)
      : null;

  return {
    shards: shardNames.sort(),
    stages: [...stages].sort(),
    docs: docs,
    keys: keys,
    perShardStats: shardStats,
    explainMillis: root?.executionTimeMillis ?? null
  };
}

print("===== Q3 DISTRIBUTED CROSS-BOROUGH =====");
print("Minimum distinct boroughs: 2");
print("Warmup runs: " + Q3_WARMUP);
print("Measured runs: " + Q3_RUNS);

// Run the query for each dataset size.
for (var dataset of q3sizes) {
  print("\n===== " + dataset.size + " =====");

  var results = runQ3(dataset.name);

  // Perform warm-up runs before collecting measurements.
  for (var w = 0; w < Q3_WARMUP; w++) {
    runQ3(dataset.name);
  }

  var latencies = [];

  // Measure client-side query latency over 30 runs.
  for (var r = 0; r < Q3_RUNS; r++) {
    var start = Date.now();
    runQ3(dataset.name);
    latencies.push(Date.now() - start);
  }

  var timing = q3Timing(latencies);

  // Collect query execution statistics using explain.
  var explain = q3db.getCollection(dataset.name)
    .explain("executionStats")
    .aggregate(q3pipeline, { allowDiskUse: true });

  // Extract shard information and execution statistics.
  var plan = q3Analyze(explain);

  print("===== Q3 Top 10 Results =====");
  printjson(results);

  print("===== Q3 Performance =====");

  printjson({
    query: "Q3 - Cross-Borough Vehicle Detection",
    dataset: dataset.size,
    mode: "Distributed - Hash Sharding",
    collection: dataset.name,

    warmupRuns: Q3_WARMUP,
    measuredRuns: Q3_RUNS,
    clientLatencyMs: latencies,
    medianLatencyMs: timing.median,
    p95LatencyMs: timing.p95,

    explainExecutionTimeMillis: plan.explainMillis,
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

print("\n===== Q3 ALL SIZES COMPLETED =====");
