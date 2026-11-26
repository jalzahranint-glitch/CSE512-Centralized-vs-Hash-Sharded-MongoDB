// CSE 512 - Traffic Monitoring
// Q5: Monthly Repeat-Offender Analysis
// Centralized Baseline - All Sizes
// No Additional Index
// Latency + Server Execution Time

const database = db.getSiblingDB("nyc_traffic");
const sizes = ["100k", "500k", "1m"];

const pipeline = [
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

    const tag = "Q5-" + size + "-" + new ObjectId();

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
        "Profiler data missing for Q5 " + size
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

    print("===== Q5 Results: " + size + " =====");
    printjson(results);

    print("===== Q5 Performance: " + size + " =====");
    printjson({
      query: "Q5 - Monthly Repeat-Offender Analysis",
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
