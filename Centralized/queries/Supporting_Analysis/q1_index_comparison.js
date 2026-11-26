// CSE 512 - Traffic Monitoring
// Q1: Vehicle Lookup - Index Comparison
// Centralized MongoDB: 100K, 500K, 1M
// Collection Scan vs Hashed Index

const database = db.getSiblingDB("nyc_traffic");
const sizes = ["100k", "500k", "1m"];

const sample = database.violations_100k.findOne();

if (!sample || !sample.vehicle_hash) {
  throw new Error("No valid sample vehicle");
}

const vehicle = sample.vehicle_hash;

const modes = [
  {
    name: "Collection Scan",
    hint: { $natural: 1 }
  },
  {
    name: "Hashed Index",
    hint: { vehicle_hash: "hashed" }
  }
];

// Create or verify the hashed index
for (const size of sizes) {
  const collection = database.getCollection(
    "violations_" + size
  );

  collection.createIndex({
    vehicle_hash: "hashed"
  });

  print("Index ready: " + size);
}

// Save profiler settings
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

    for (const mode of modes) {
      const tag = "Q1-" + size + "-" +
        new ObjectId().toHexString();

      const start = Date.now();

      const results = collection.find(
        { vehicle_hash: vehicle },
        {
          _id: 0,
          vehicle_hash: 1,
          issue_date: 1,
          zone: 1,
          violation: 1
        }
      )
      .hint(mode.hint)
      .comment(tag)
      .limit(10)
      .toArray();

      const latency = Date.now() - start;

      const entries = database.system.profile.find({
        op: { $in: ["query", "command", "getmore"] },
        $or: [
          { "command.comment": tag },
          { "originatingCommand.comment": tag }
        ]
      }).toArray();

      if (entries.length === 0) {
        throw new Error(
          "Profiler data missing: " + size
        );
      }

      const sum = field => entries.reduce(
        (total, entry) =>
          total + (entry[field] ?? 0), 0
      );

      print(
        "===== Q1 Performance: " +
        size + " | " + mode.name + " ====="
      );

      printjson({
        query: "Q1 - Vehicle Lookup",
        dataset: size,
        mode: mode.name,
        latencyMillis: latency,
        serverExecutionTimeMillis: sum("millis"),
        totalDocsExamined: sum("docsExamined"),
        totalKeysExamined: sum("keysExamined"),
        recordsReturned: results.length
      });
    }
  }

} finally {
  database.setProfilingLevel(previous.was, {
    slowms: previous.slowms,
    sampleRate: previous.sampleRate,
    filter: previous.filter ?? "unset"
  });
}
