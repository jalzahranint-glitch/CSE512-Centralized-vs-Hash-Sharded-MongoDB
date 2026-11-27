var q4db = db.getSiblingDB("nyc_traffic");

var q4sizes = [
  { size: "100K", name: "violations_100k" },
  { size: "500K", name: "violations_500k" },
  { size: "1M", name: "violations_1m" }
];

var q4pipeline = [
  {
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
      total_violations: { $sum: 1 }
    }
  },
  {
    $sort: {
      total_violations: -1,
      "_id.borough": 1,
      "_id.month": 1
    }
  },
  { $limit: 10 },
  {
    $project: {
      _id: 0,
      borough: "$_id.borough",
      month: "$_id.month",
      total_violations: 1
    }
  }
];

var Q4_WARMUP = 1;
var Q4_RUNS = 5;

if (db.adminCommand({ hello: 1 }).msg !== "isdbgrid") {
  throw new Error("Not connected to mongos");
}

function runQ4(name) {
  return q4db.getCollection(name)
    .aggregate(q4pipeline, { allowDiskUse: true })
    .toArray();
}

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

function q4Analyze(explain) {
  var shardStats = [];
  var stages = new Set();
  var shardNames = [];

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

print(" Q4 DISTRIBUTED BOROUGH MONTHLY ANALYSIS ");
print("Warmup runs: " + Q4_WARMUP);
print("Measured runs: " + Q4_RUNS);

for (var dataset of q4sizes) {
  print("\n " + dataset.size + " ");

  var results = runQ4(dataset.name);

  for (var w = 0; w < Q4_WARMUP; w++) {
    runQ4(dataset.name);
  }

  var latencies = [];

  for (var r = 0; r < Q4_RUNS; r++) {
    var start = Date.now();
    runQ4(dataset.name);
    latencies.push(Date.now() - start);
  }

  var timing = q4Timing(latencies);

  var explain = q4db.getCollection(dataset.name)
    .explain("executionStats")
    .aggregate(q4pipeline, { allowDiskUse: true });

  var plan = q4Analyze(explain);

  print(" Q4 Top 10 Results ");
  printjson(results);

  print(" Q4 Performance ");

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

print("\n Q4 ALL SIZES COMPLETED ");
