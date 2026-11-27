// CSE 512 - Traffic Monitoring
// Q2-Q5 After Hashed Index
// Centralized MongoDB
// Datasets: 100K, 500K, 1M

const database = db.getSiblingDB("nyc_traffic");
const sizes = ["100k", "500k", "1m"];

const queries = [
  {
    id: "Q2",
    name: "Repeat Offenders",
    pipeline: [
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
    ]
  },
  {
    id: "Q3",
    name: "Cross-Borough Detection",
    pipeline: [
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
    ]
  },
  {
    id: "Q4",
    name: "Borough and Monthly Analysis",
    pipeline: [
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
    ]
  },
  {
    id: "Q5",
    name: "Monthly Repeat-Offender Analysis",
    pipeline: [
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
    ]
  }
];

for (const size of sizes) {
  const collection = database.getCollection(
    "violations_" + size
  );

  const hasIndex = collection.getIndexes().some(
    index => index.key.vehicle_hash === "hashed"
  );

  if (!hasIndex) {
    throw new Error("Hashed index missing: " + size);
  }
}

const previous = database.getProfilingStatus();

try {
  database.setProfilingLevel(1, {
    slowms: 0,
    sampleRate: 1,
    filter: "unset"
  });

  for (const query of queries) {
    for (const size of sizes) {
      const collection = database.getCollection(
        "violations_" + size
      );

      const tag = query.id + "-" + size + "-" +
        new ObjectId().toHexString();

      const start = Date.now();

      const results = collection.aggregate(
        query.pipeline,
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
        throw new Error("Profiler missing: " + tag);
      }

      const sum = field => entries.reduce(
        (total, entry) =>
          total + (entry[field] ?? 0), 0
      );

      const plans = [
        ...new Set(
          entries.map(e => e.planSummary).filter(Boolean)
        )
      ];

      print(
        " Performance: " +
        query.id + " | " + size + " "
      );

      printjson({
        query: query.name,
        dataset: size,
        mode: "Centralized - Hashed Index Present",
        latencyMillis: latency,
        serverExecutionTimeMillis: sum("millis"),
        totalDocsExamined: sum("docsExamined"),
        totalKeysExamined: sum("keysExamined"),
        recordsReturned: results.length,
        queryPlan: plans,
        results: results
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
