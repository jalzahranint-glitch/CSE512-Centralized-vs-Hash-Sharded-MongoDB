var q2db = db.getSiblingDB("nyc_traffic");

// Define the three dataset sizes for the benchmark.
var q2sizes = [
  {size: "100K", name: "violations_100k"},
  {size: "500K", name: "violations_500k"},
  {size: "1M", name: "violations_1m"}
];

// Define the aggregation pipeline for the top 10 vehicles.
var q2pipeline = [
  {
    $group: {
      // Group violations by vehicle hash.
      _id: "$vehicle_hash",
      // Count the total violations for each vehicle.
      violation_count: {$sum: 1}
    }
  },
  {
    $sort: {
      // Sort vehicles by violation count in descending order.
      violation_count: -1,
      _id: 1
    }
  },
  // Return the 10 vehicles with the most violations.
  {$limit: 10}
];

// Set five warm-up runs and 30 measured runs.
var WARMUP = 5;
var RUNS = 30;

// Verify that the database connection uses mongos.
if (db.adminCommand({hello: 1}).msg !== "isdbgrid") {
  throw new Error("Not connected to mongos");
}

// Execute the aggregation pipeline on the selected dataset.
function runQ2(name) {
  return q2db.getCollection(name)
    .aggregate(q2pipeline, {allowDiskUse: true})
    .toArray();
}

// Calculate median and P95 latency from the measurements.
function latencySummary(values) {
  var a = values.slice().sort((x, y) => x - y);
  var mid = Math.floor(a.length / 2);

  return {
    median: a.length % 2 === 0
      ? (a[mid - 1] + a[mid]) / 2
      : a[mid],
    p95: a[Math.ceil(0.95 * a.length) - 1]
  };
}

// Find execution statistics within the explain output.
function findStats(node) {
  if (!node || typeof node !== "object") return null;

  if (node.executionStats &&
      node.executionStats.totalDocsExamined != null) {
    return node.executionStats;
  }

  if (Array.isArray(node)) {
    for (var item of node) {
      var found = findStats(item);
      if (found) return found;
    }
    return null;
  }

  for (var key of Object.keys(node)) {
    var found = findStats(node[key]);
    if (found) return found;
  }

  return null;
}

// Analyze shard involvement and query execution statistics.
function analyzeExplain(explain) {
  var names = new Set();
  var stages = new Set();

  // Inspect nested plan sections to identify shards and stages.
  function walk(node) {
    if (!node || typeof node !== "object") return;

    if (Array.isArray(node)) {
      node.forEach(walk);
      return;
    }

    if (typeof node.shardName === "string") {
      names.add(node.shardName);
    }

    if (typeof node.stage === "string") {
      stages.add(node.stage);
    }

    if (node.shards && !Array.isArray(node.shards)) {
      Object.keys(node.shards).forEach(function(name) {
        if (/^shard[1-4]RS$/.test(name)) names.add(name);
      });
    }

    Object.keys(node).forEach(function(key) {
      walk(node[key]);
    });
  }

  walk(explain);

  // Collect available execution statistics for each shard.
  var shardStats = [];
  var sections = explain.shards;

  if (sections && typeof sections === "object") {
    var entries = Array.isArray(sections)
      ? sections.map(x => [x.shardName, x])
      : Object.entries(sections);

    for (var entry of entries) {
      var s = findStats(entry[1]);
      if (s) {
        shardStats.push({
          shard: entry[0],
          docs: Number(s.totalDocsExamined),
          keys: Number(s.totalKeysExamined || 0),
          millis: s.executionTimeMillis ?? null
        });
      }
    }
  }

  var root = explain.executionStats;
  var hasRoot = root && root.totalDocsExamined != null;

  // Check whether statistics are available for every identified shard.
  var completeShards =
    names.size > 0 && shardStats.length === names.size;

  // Use root document counts or complete per-shard totals.
  var docs = hasRoot
    ? Number(root.totalDocsExamined)
    : completeShards
      ? shardStats.reduce((a, s) => a + s.docs, 0)
      : null;

  var keys = hasRoot
    ? Number(root.totalKeysExamined)
    : completeShards
      ? shardStats.reduce((a, s) => a + s.keys, 0)
      : null;

  return {
    shards: [...names].sort(),
    stages: [...stages].sort(),
    docs: docs,
    keys: keys,
    explainMillis: root?.executionTimeMillis ?? null,
    perShardStats: shardStats,
    explainSections: Object.keys(explain)
  };
}

print(" Q2 DISTRIBUTED REPEAT OFFENDERS ");
print("Warmup runs: " + WARMUP);
print("Measured runs: " + RUNS);

// Run Q2 for each dataset size.
for (var dataset of q2sizes) {
  print("\n " + dataset.size + " ");

  // Retrieve the top 10 vehicles from the selected collection.
  var results = runQ2(dataset.name);

  // Execute warm-up queries before measuring latency.
  for (var w = 0; w < WARMUP; w++) {
    runQ2(dataset.name);
  }

  var latencies = [];

  // Measure client-side latency over 30 runs.
  for (var r = 0; r < RUNS; r++) {
    var start = Date.now();
    runQ2(dataset.name);
    latencies.push(Date.now() - start);
  }

  // Calculate the median and P95 query latency.
  var timing = latencySummary(latencies);

  // Collect query execution statistics using explain.
  var explain = q2db.getCollection(dataset.name)
    .explain("executionStats")
    .aggregate(q2pipeline, {allowDiskUse: true});

  // Extract shard information and execution plan details.
  var plan = analyzeExplain(explain);

  print(" Top 10 Repeat Offenders ");
  printjson(results);

  print(" Q2 Performance ");

  // Display query results, latency, and execution statistics.
  printjson({
    query: "Q2 - Repeat Offender Detection",
    dataset: dataset.size,
    mode: "Distributed - Hash Sharding",
    collection: dataset.name,

    warmupRuns: WARMUP,
    measuredRuns: RUNS,
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
    perShardExplainStats: plan.perShardStats,
    explainSections: plan.explainSections
  });
}

print("\n Q2 ALL SIZES COMPLETED ");
