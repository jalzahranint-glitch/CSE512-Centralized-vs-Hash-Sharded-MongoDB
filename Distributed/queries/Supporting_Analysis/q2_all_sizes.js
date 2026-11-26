var q2db = db.getSiblingDB("nyc_traffic");

var q2sizes = [
  {size: "100K", name: "violations_100k"},
  {size: "500K", name: "violations_500k"},
  {size: "1M", name: "violations_1m"}
];

var q2pipeline = [
  {
    $group: {
      _id: "$vehicle_hash",
      violation_count: {$sum: 1}
    }
  },
  {
    $sort: {
      violation_count: -1,
      _id: 1
    }
  },
  {$limit: 10}
];

var WARMUP = 1;
var RUNS = 5;

if (db.adminCommand({hello: 1}).msg !== "isdbgrid") {
  throw new Error("Not connected to mongos");
}

function runQ2(name) {
  return q2db.getCollection(name)
    .aggregate(q2pipeline, {allowDiskUse: true})
    .toArray();
}

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

function analyzeExplain(explain) {
  var names = new Set();
  var stages = new Set();

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

  var completeShards =
    names.size > 0 && shardStats.length === names.size;

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

print("===== Q2 DISTRIBUTED REPEAT OFFENDERS =====");
print("Warmup runs: " + WARMUP);
print("Measured runs: " + RUNS);

for (var dataset of q2sizes) {
  print("\n===== " + dataset.size + " =====");

  var results = runQ2(dataset.name);

  for (var w = 0; w < WARMUP; w++) {
    runQ2(dataset.name);
  }

  var latencies = [];

  for (var r = 0; r < RUNS; r++) {
    var start = Date.now();
    runQ2(dataset.name);
    latencies.push(Date.now() - start);
  }

  var timing = latencySummary(latencies);

  var explain = q2db.getCollection(dataset.name)
    .explain("executionStats")
    .aggregate(q2pipeline, {allowDiskUse: true});

  var plan = analyzeExplain(explain);

  print("===== Top 10 Repeat Offenders =====");
  printjson(results);

  print("===== Q2 Performance =====");

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

print("\n===== Q2 ALL SIZES COMPLETED =====");
