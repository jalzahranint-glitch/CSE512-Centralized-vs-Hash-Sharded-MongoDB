// CSE 512 
// Q3: Cross-Borough Vehicle Detection
// Centralized Baseline - All Sizes
// No Additional Index
// Metrics: Latency + Server Execution Time

const database = db.getSiblingDB("nyc_traffic");
const sizes = ["100k", "500k", "1m"];

const pipeline = [
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

// Save original profiler settings
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

    const tag = "Q3-" + size + "-" + new ObjectId();

    // Measure query latency
    const start = Date.now();

    const results = collection.aggregate(
      pipeline,
      {
        allowDiskUse: true,
        comment: tag
      }
    ).toArray();

    const latency = Date.now() - start;

    // Read server execution time
    const entries = database.system.profile.find({
      op: { $in: ["command", "getmore"] },
      $or: [
        { "command.comment": tag },
        { "originatingCommand.comment": tag }
      ]
    }).toArray();

    const executionTime = entries.length
      ? entries.reduce((sum, e) => sum + e.millis, 0)
      : null;

    const docsExamined = entries.some(
      e => e.docsExamined !== undefined
    )
      ? entries.reduce(
          (sum, e) => sum + (e.docsExamined ?? 0), 0
        )
      : null;

    const keysExamined = entries.some(
      e => e.keysExamined !== undefined
    )
      ? entries.reduce(
          (sum, e) => sum + (e.keysExamined ?? 0), 0
        )
      : null;

    print("===== Q3 Results: " + size + " =====");
    printjson(results);

    print("===== Q3 Performance: " + size + " =====");
    printjson({
      query: "Q3 - Cross-Borough Vehicle Detection",
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
  // Restore previous profiler settings
  database.setProfilingLevel(previous.was, {
    slowms: previous.slowms,
    sampleRate: previous.sampleRate,
    filter: previous.filter ?? "unset"
  });
}
