var q5db = db.getSiblingDB("nyc_traffic");

var q5sizes = [
  { size: "100K", name: "violations_100k" },
  { size: "500K", name: "violations_500k" },
  { size: "1M", name: "violations_1m" }
];

var q5pipeline = [
  {
    $match: {
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
            format: "%Y-%m",
            date: "$issue_date",
            timezone: "UTC"
          }
        }
      },
      violation_count: { $sum: 1 }
    }
  },
  {
    $match: {
      violation_count: { $gte: 2 }
    }
  },
  {
    $group: {
      _id: "$_id.month",
      repeat_offender_vehicles: { $sum: 1 },
      repeat_violations: { $sum: "$violation_count" },
      max_violations_per_vehicle: {
        $max: "$violation_count"
      }
    }
  },
  {
    $sort: {
      repeat_offender_vehicles: -1,
      _id: 1
    }
  },
  { $limit: 10 },
  {
    $project: {
      _id: 0,
      month: "$_id",
      repeat_offender_vehicles: 1,
      repeat_violations: 1,
      max_violations_per_vehicle: 1
    }
  }
];

var Q5_WARMUP = 1;
var Q5_RUNS = 5;

if (db.adminCommand({ hello: 1 }).msg !== "isdbgrid") {
  throw new Error("Not connected to mongos");
}

function runQ5(name) {
  return q5db.getCollection(name)
    .aggregate(q5pipeline, { allowDiskUse: true })
    .toArray();
}

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

function q5Analyze(explain) {
  var shardStats = [];
  var shardNames = [];
  var stages = new Set();

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

  var complete =
    shardNames.length > 0 &&
    shardStats.length === shardNames.length;

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

print("===== Q5 DISTRIBUTED MONTHLY REPEAT OFFENDERS =====");
print("Warmup runs: " + Q5_WARMUP);
print("Measured runs: " + Q5_RUNS);

for (var dataset of q5sizes) {
  print("\n===== " + dataset.size + " =====");

  var results = runQ5(dataset.name);

  for (var w = 0; w < Q5_WARMUP; w++) {
    runQ5(dataset.name);
  }

  var latencies = [];

  for (var r = 0; r < Q5_RUNS; r++) {
    var start = Date.now();
    runQ5(dataset.name);
    latencies.push(Date.now() - start);
  }

  var timing = q5Timing(latencies);

  var explain = q5db.getCollection(dataset.name)
    .explain("executionStats")
    .aggregate(q5pipeline, { allowDiskUse: true });

  var plan = q5Analyze(explain);

  print("===== Q5 Top 10 Results =====");
  printjson(results);

  print("===== Q5 Performance =====");
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

print("\n===== Q5 ALL SIZES COMPLETED =====");
