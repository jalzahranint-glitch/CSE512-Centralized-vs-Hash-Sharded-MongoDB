// CSE 512 - Traffic Monitoring
// Q4: Borough and Monthly Violation Analysis
// Centralized Baseline - All Sizes
// No Additional Index
// Latency + Server Execution Time

const database = db.getSiblingDB("nyc_traffic");
const sizes = ["100k", "500k", "1m"];

const pipeline = [
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

// Save previous profiler settings
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

    const tag = "Q4-" + size + "-" + new ObjectId();

    // Query Latency
    const start = Date.now();

    const results = collection.aggregate(
      pipeline,
      {
        allowDiskUse: true,
        comment: tag
      }
    ).toArray();

    const latency = Date.now() - start;

    // Server Execution Time
    const entries = database.system.profile.find({
      "command.comment": tag,
      op: { $in: ["command", "getmore"] }
    }).toArray();

    const executionTime = entries.length
      ? entries.reduce(
          (sum, e) => sum + (e.millis ?? 0), 0
        )
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

    print("===== Q4 Results: " + size + " =====");
    printjson(results);

    print("===== Q4 Performance: " + size + " =====");
    printjson({
      query: "Q4 - Borough and Monthly Analysis",
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
  // Restore profiler settings
  database.setProfilingLevel(previous.was, {
    slowms: previous.slowms,
    sampleRate: previous.sampleRate,
    filter: previous.filter ?? "unset"
  });
}
