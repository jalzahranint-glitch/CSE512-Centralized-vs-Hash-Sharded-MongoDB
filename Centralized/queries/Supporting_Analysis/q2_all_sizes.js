// CSE 512 - Traffic Monitoring
// Q2: Repeat Offender Detection
// Centralized Baseline - 100K, 500K, 1M
// No Additional Index
// Metrics: Latency + Server Execution Time

const database = db.getSiblingDB("nyc_traffic");
const sizes = ["100k", "500k", "1m"];

const pipeline = [
  {
    $group: {
      _id: "$vehicle_hash",
      violation_count: { $sum: 1 }
    }
  },
  {
    $sort: {
      violation_count: -1,
      _id: 1
    }
  },
  { $limit: 10 }
];

const previous = database.getProfilingStatus();

try {
  database.setProfilingLevel(1, {
    slowms: 0,
    sampleRate: 1,
    filter: "unset"
  });

  for (const size of sizes) {
    const collection = database.getCollection(
      "violations_" + size
    );

    const tag = "Q2-" + size + "-" + new ObjectId();

    const start = Date.now();

    const results = collection.aggregate(
      pipeline,
      {
        allowDiskUse: true,
        comment: tag
      }
    ).toArray();

    const latency = Date.now() - start;

    const entries = database.system.profile.find({
      op: { $in: ["command", "getmore"] },
      $or: [
        { "command.comment": tag },
        { "originatingCommand.comment": tag }
      ]
    }).toArray();

    if (entries.length === 0) {
      throw new Error(
        "Profiler data missing for Q2 " + size
      );
    }

    const executionTime = entries.reduce(
      (sum, e) => sum + (e.millis ?? 0), 0
    );

    const docsExamined = entries.reduce(
      (sum, e) => sum + (e.docsExamined ?? 0), 0
    );

    const keysExamined = entries.reduce(
      (sum, e) => sum + (e.keysExamined ?? 0), 0
    );

    print("===== Q2 Results: " + size + " =====");
    printjson(results);

    print("===== Q2 Performance: " + size + " =====");
    printjson({
      query: "Q2 - Repeat Offender Detection",
      dataset: size,
      mode: "Centralized - No Additional Index",
      latencyMillis: latency,
      serverExecutionTimeMillis: executionTime,
      totalDocsExamined: docsExamined,
      totalKeysExamined: keysExamined,
      recordsReturned: results.length
    });
  }
} finally {
  database.setProfilingLevel(previous.was, {
    slowms: previous.slowms,
    sampleRate: previous.sampleRate,
    filter: previous.filter ?? "unset"
  });
}
