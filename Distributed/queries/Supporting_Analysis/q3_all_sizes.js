var q3db = db.getSiblingDB("nyc_traffic");

var q3sizes = [
  { size: "100K", name: "violations_100k" },
  { size: "500K", name: "violations_500k" },
  { size: "1M", name: "violations_1m" }
];

var q3pipeline = [
  {
    $match: {
      vehicle_hash: { $type: "string", $ne: "" },
      zone: { $type: "string", $ne: "" }
    }
  },
  {
    $group: {
      _id: "$vehicle_hash",
      boroughs: { $addToSet: "$zone" },
      total_violations: { $sum: 1 }
    }
  },
  {
    $project: {
      boroughs: 1,
      borough_count: { $size: "$boroughs" },
      total_violations: 1
    }
  },
  {
    $match: {
      borough_count: { $gte: 2 }
    }
  },
  {
    $sort: {
      borough_count: -1,
      total_violations: -1,
      _id: 1
    }
  },
  { $limit: 10 }
];

var Q3_WARMUP = 1;
var Q3_RUNS = 5;

if (db.adminCommand({ hello: 1 }).msg !== "isdbgrid") {
  throw new Error("Not connected to mongos");
}

function runQ3(name) {
  return q3db.getCollection(name)
    .aggregate(q3pipeline, { allowDiskUse: true })
    .toArray();
}

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

function q3Analyze(explain) {
  var shardStats = [];
  var stages = new Set();
  var shardNames = [];

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

  var allShardsMeasured =
    shardNames.length > 0 &&
    shardStats.length === shardNames.length;

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

print(" Q3 DISTRIBUTED CROSS-BOROUGH ");
print("Minimum distinct boroughs: 2");
print("Warmup runs: " + Q3_WARMUP);
print("Measured runs: " + Q3_RUNS);

for (var dataset of q3sizes) {
  print("\n " + dataset.size + " ");

  var results = runQ3(dataset.name);

  for (var w = 0; w < Q3_WARMUP; w++) {
    runQ3(dataset.name);
  }

  var latencies = [];

  for (var r = 0; r < Q3_RUNS; r++) {
    var start = Date.now();
    runQ3(dataset.name);
    latencies.push(Date.now() - start);
  }

  var timing = q3Timing(latencies);

  var explain = q3db.getCollection(dataset.name)
    .explain("executionStats")
    .aggregate(q3pipeline, { allowDiskUse: true });

  var plan = q3Analyze(explain);

  print(" Q3 Top 10 Results ");
  printjson(results);

  print(" Q3 Performance ");

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

print("\n Q3 ALL SIZES COMPLETED ");
