var database = db.getSiblingDB("nyc_traffic");

// Define the three dataset sizes used in the benchmark.
var datasets = [
  { size: "100K", name: "violations_100k" },
  { size: "500K", name: "violations_500k" },
  { size: "1M", name: "violations_1m" }
];

// Set the number of warm-up and measured runs.
var WARMUP = 5;
var RUNS = 30;

// Find violations for a vehicle using its hashed identifier.
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

// Calculate median and P95 latency from measured runs.
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

// Extract shard names and query stages from the execution plan.
function getPlanInfo(explain) {
  var shards = new Set();
  var stages = new Set();

  // Recursively inspect the execution plan.
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

// Verify that the connection is through mongos.
if (db.adminCommand({ hello: 1 }).msg !== "isdbgrid") {
  throw new Error("Not connected to mongos");
}

// Check that the 100K dataset contains records.
var sample = database.violations_100k.findOne();

if (!sample) {
  throw new Error("100K dataset is empty");
}

var vehicle = "e1b3dc8fd32093207a7a965292593e4601fe2825d72d02e58f37d0d438e84dd2";

print("===== Q1 DISTRIBUTED VEHICLE LOOKUP =====");
print("Vehicle hash: " + vehicle);

// Run the vehicle lookup for each dataset size.
for (var i = 0; i < datasets.length; i++) {
  var dataset = datasets[i];
  var filter = { vehicle_hash: vehicle };

  print("\n===== " + dataset.size + " =====");

  // Retrieve matching records for the selected vehicle.
  var results = lookup(dataset.name, vehicle);

  // Run warm-up queries before collecting measurements.
  for (var w = 0; w < WARMUP; w++) {
    lookup(dataset.name, vehicle);
  }

  var latencies = [];

  // Measure client-side latency over 30 runs.
  for (var r = 0; r < RUNS; r++) {
    var start = Date.now();
    lookup(dataset.name, vehicle);
    latencies.push(Date.now() - start);
  }

  // Calculate the median and P95 latency.
  var summary = metrics(latencies);

  // Inspect the execution plan for the same vehicle filter.
  var explain = database.getCollection(dataset.name)
    .find(filter)
    .explain("executionStats");

  var stats = explain.executionStats;
  // Identify the shards involved and query plan stages.
  var plan = getPlanInfo(explain);

  // Display latency, execution statistics, and shard information.
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

print("\n===== Q1 ALL SIZES COMPLETED =====");
