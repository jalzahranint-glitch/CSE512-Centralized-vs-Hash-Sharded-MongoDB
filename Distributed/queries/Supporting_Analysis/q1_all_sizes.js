var database = db.getSiblingDB("nyc_traffic");

var datasets = [
  { size: "100K", name: "violations_100k" },
  { size: "500K", name: "violations_500k" },
  { size: "1M", name: "violations_1m" }
];

var WARMUP = 2;
var RUNS = 20;

function lookup(name, vehicle) {
  return db.getSiblingDB("nyc_traffic")
    .getCollection(name)
    .find(
      { vehicle_hash: vehicle },
      {
        _id: 0,
        vehicle_hash: 1,
        issue_date: 1,
        zone: 1,
        violation: 1
      }
    )
    .limit(10)
    .toArray();
}

function metrics(values) {
  var sorted = values.slice().sort((a, b) => a - b);
  var mid = Math.floor(sorted.length / 2);

  return {
    median: sorted.length % 2 === 0
      ? (sorted[mid - 1] + sorted[mid]) / 2
      : sorted[mid],
    p95: sorted[Math.ceil(sorted.length * 0.95) - 1]
  };
}

function getPlanInfo(explain) {
  var shards = new Set();
  var stages = new Set();

  function visit(node) {
    if (!node || typeof node !== "object") return;

    if (Array.isArray(node)) {
      node.forEach(visit);
      return;
    }

    if (typeof node.shardName === "string") {
      shards.add(node.shardName);
    }

    if (typeof node.stage === "string") {
      stages.add(node.stage);
    }

    Object.keys(node).forEach(function(key) {
      visit(node[key]);
    });
  }

  visit(explain.queryPlanner.winningPlan);
  visit(explain.executionStats.executionStages);

  return {
    shards: Array.from(shards).sort(),
    stages: Array.from(stages).sort()
  };
}

if (db.adminCommand({ hello: 1 }).msg !== "isdbgrid") {
  throw new Error("Not connected to mongos");
}

var sample = database.violations_100k.findOne();

if (!sample) {
  throw new Error("100K dataset is empty");
}

var vehicle = sample.vehicle_hash;

print(" Q1 DISTRIBUTED VEHICLE LOOKUP ");
print("Vehicle hash: " + vehicle);

for (var i = 0; i < datasets.length; i++) {
  var dataset = datasets[i];
  var filter = { vehicle_hash: vehicle };

  print("\n " + dataset.size + " ");

  var results = lookup(dataset.name, vehicle);

  for (var w = 0; w < WARMUP; w++) {
    lookup(dataset.name, vehicle);
  }

  var latencies = [];

  for (var r = 0; r < RUNS; r++) {
    var start = Date.now();
    lookup(dataset.name, vehicle);
    latencies.push(Date.now() - start);
  }

  var summary = metrics(latencies);

  var explain = database.getCollection(dataset.name)
    .find(filter)
    .explain("executionStats");

  var stats = explain.executionStats;
  var plan = getPlanInfo(explain);

  printjson({
    query: "Q1 - Vehicle Lookup",
    dataset: dataset.size,
    collection: dataset.name,
    vehicleHash: vehicle,
    warmupRuns: WARMUP,
    measuredRuns: RUNS,
    clientLatencyMs: latencies,
    medianLatencyMs: summary.median,
    p95LatencyMs: summary.p95,
    explainExecutionTimeMillis: stats.executionTimeMillis,
    totalDocsExamined: stats.totalDocsExamined,
    totalKeysExamined: stats.totalKeysExamined,
    explainNReturned: stats.nReturned,
    recordsReturned: results.length,
    shardsInvolved: plan.shards,
    shardCount: plan.shards.length || null,
    queryPlanStages: plan.stages
  });
}

print("\n Q1 ALL SIZES COMPLETED ");
