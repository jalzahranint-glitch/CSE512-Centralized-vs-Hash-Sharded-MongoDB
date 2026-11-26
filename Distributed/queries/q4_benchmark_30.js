var q4db = db.getSiblingDB("nyc_traffic");

// Define the three dataset sizes used in the benchmark.
var q4sizes = [
  { size: "100K", name: "violations_100k" },
  { size: "500K", name: "violations_500k" },
  { size: "1M", name: "violations_1m" }
];

// Define the aggregation pipeline for monthly violations by borough.
var q4pipeline = [
  {
    // Keep records with valid dates and one of the five NYC boroughs.
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
    // Group violations by borough and month.
    $group: {
      _id: {
        borough: "$zone",
        month: {
          $dateToString: {
            // Extract the year and month using UTC.
            format: "%Y-%m",
            date: "$issue_date",
            timezone: "UTC"
          }
        }
      },
      // Count violations for each borough and month.
      total_violations: { $sum: 1 }
    }
  },
  {
    // Rank groups by violation count, with borough and month as tie breakers.
    $sort: {
      total_violations: -1,
      "_id.borough": 1,
      "_id.month": 1
    }
  },
  // Return the 10 borough-month groups with the most violations.
  { $limit: 10 },
  {
    // Format the final output fields.
    $project: {
      _id: 0,
      borough: "$_id.borough",
      month: "$_id.month",
      total_violations: 1
    }
  }
];

// Set five warm-up runs and 30 measured runs.
var Q4_WARMUP = 5;
var Q4_RUNS = 30;

// Verify that the database connection uses mongos.
if (db.adminCommand({ hello: 1 }).msg !== "isdbgrid") {
  throw new Error("Not connected to mongos");
}

// Execute the aggregation pipeline on the selected dataset.
function runQ4(name) {
  return q4db.getCollection(name)
    .aggregate(q4pipeline, { allowDiskUse: true })
    .toArray();
}

// Calculate median and P95 latency.
function q4Timing(values) {
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
function q4FindStats(node) {
  if (!node || typeof node !== "object") return null;

  if (
    node.executionStats &&
    node.executionStats.totalDocsExamined != null
  ) {
    return node.executionStats;
  }

  if (Array.isArray(node)) {
    for (var item of node) {
      var found = q4FindStats(item);
      if (found) return found;
    }
    return null;
  }

  for (var key of Object.keys(node)) {
    var found = q4FindStats(node[key]);
    if (found) return found;
  }

  return null;
}

// Analyze shard involvement and query execution statistics.
function q4Analyze(explain) {
  var shardStats = [];
  var stages = new Set();
  var shardNames = [];

  // Collect execution stages from nested plan sections.
  function walk(node) {
    if (!node || typeof node !== "object") return;

    if (Array.isArray(node)) {
      node.forEach(walk);
      return;
    }

    if (typeof node.stage === "string") {
      stages.add(node.stage);
    }

    Object.keys(node).forEach(function(key) {
      walk(node[key]);
    });
  }

  walk(explain);

  // Read available execution statistics for each shard.
  var sections = explain.shards;

  if (sections && typeof sections === "object") {
    var entries = Array.isArray(sections)
      ? sections.map(x => [x.shardName, x])
      : Object.entries(sections);

    for (var entry of entries) {
      var name = entry[0];
      if (!name) continue;

      shardNames.push(name);

      var stats = q4FindStats(entry[1]);

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
  var allShardsMeasured =
    shardNames.length > 0 &&
    shardStats.length === shardNames.length;

  return {
    shards: shardNames.sort(),
    stages: [...stages].sort(),
    docs: rootDocs != null
      ? Number(rootDocs)
      : allShardsMeasured
        ? shardStats.reduce((sum, s) => sum + s.docs, 0)
        : null,
    keys: rootKeys != null
      ? Number(rootKeys)
      : allShardsMeasured
        ? shardStats.reduce((sum, s) => sum + s.keys, 0)
        : null,
    explainMillis: root?.executionTimeMillis ?? null,
    perShardStats: shardStats
  };
}

print("===== Q4 DISTRIBUTED BOROUGH MONTHLY ANALYSIS =====");
print("Warmup runs: " + Q4_WARMUP);
print("Measured runs: " + Q4_RUNS);

// Run Q4 for each dataset size.
for (var dataset of q4sizes) {
  print("\n===== " + dataset.size + " =====");

  var results = runQ4(dataset.name);

  // Perform warm-up runs before collecting measurements.
  for (var w = 0; w < Q4_WARMUP; w++) {
    runQ4(dataset.name);
  }

  var latencies = [];

  // Measure client-side query latency over 30 runs.
  for (var r = 0; r < Q4_RUNS; r++) {
    var start = Date.now();
    runQ4(dataset.name);
    latencies.push(Date.now() - start);
  }

  var timing = q4Timing(latencies);

  // Collect query execution statistics using explain.
  var explain = q4db.getCollection(dataset.name)
    .explain("executionStats")
    .aggregate(q4pipeline, { allowDiskUse: true });

  // Extract shard information and execution plan details.
  var plan = q4Analyze(explain);

  print("===== Q4 Top 10 Results =====");
  printjson(results);

  print("===== Q4 Performance =====");

  // Display latency, query results summary, and shard statistics.
  printjson({
    query: "Q4 - Borough and Monthly Analysis",
    dataset: dataset.size,
    mode: "Distributed - Hash Sharding",
    collection: dataset.name,

    warmupRuns: Q4_WARMUP,
    measuredRuns: Q4_RUNS,
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

print("\n===== Q4 ALL SIZES COMPLETED =====");
