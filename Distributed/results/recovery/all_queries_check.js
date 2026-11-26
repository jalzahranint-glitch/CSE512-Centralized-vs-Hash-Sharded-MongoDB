
const d = db.getSiblingDB("nyc_traffic");
const vehicle = "e1b3dc8fd32093207a7a965292593e4601fe2825d72d02e58f37d0d438e84dd2";
const pipelines = {};

pipelines[2] = [
  {
    $group: {
      // Group violations by vehicle hash.
      _id: "$vehicle_hash",
      // Count the total violations for each vehicle.
      violation_count: {$sum: 1}
    }
  },
  {
    $sort: {
      // Sort vehicles by violation count in descending order.
      violation_count: -1,
      _id: 1
    }
  },
  // Return the 10 vehicles with the most violations.
  {$limit: 10}
];
pipelines[3] = [
  {
    $match: {
      // Keep records with non-empty vehicle hashes and zone values.
      vehicle_hash: { $type: "string", $ne: "" },
      zone: { $type: "string", $ne: "" }
    }
  },
  {
    $group: {
      _id: "$vehicle_hash",
      // Collect distinct zones for each vehicle.
      boroughs: { $addToSet: "$zone" },
      // Count the total violations for each vehicle.
      total_violations: { $sum: 1 }
    }
  },
  {
    $project: {
      boroughs: 1,
      // Calculate the number of distinct zones.
      borough_count: { $size: "$boroughs" },
      total_violations: 1
    }
  },
  {
    $match: {
      // Keep vehicles with violations in at least two zones.
      borough_count: { $gte: 2 }
    }
  },
  {
    $sort: {
      // Rank vehicles by zone count and total violations.
      borough_count: -1,
      total_violations: -1,
      _id: 1
    }
  },
  // Return the top 10 vehicles based on the ranking.
  { $limit: 10 }
];
pipelines[4] = [
  {
    // Keep records with valid dates and one of the five NYC boroughs.
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
    // Group violations by borough and month.
    $group: {
      _id: {
        borough: "$zone",
        month: {
          $dateToString: {
            // Extract the year and month using UTC.
            format: "%Y-%m",
            date: "$issue_date",
            timezone: "UTC"
          }
        }
      },
      // Count violations for each borough and month.
      total_violations: { $sum: 1 }
    }
  },
  {
    // Rank groups by violation count, with borough and month as tie breakers.
    $sort: {
      total_violations: -1,
      "_id.borough": 1,
      "_id.month": 1
    }
  },
  // Return the 10 borough-month groups with the most violations.
  { $limit: 10 },
  {
    // Format the final output fields.
    $project: {
      _id: 0,
      borough: "$_id.borough",
      month: "$_id.month",
      total_violations: 1
    }
  }
];
pipelines[5] = [
  {
    $match: {
      // Keep records with non-empty vehicle hashes and valid dates.
      vehicle_hash: { $type: "string", $ne: "" },
      issue_date: { $type: "date" }
    }
  },
  {
    $group: {
      _id: {
        // Group violations by vehicle and month.
        vehicle: "$vehicle_hash",
        month: {
          $dateToString: {
            // Extract the year and month using UTC.
            format: "%Y-%m",
            date: "$issue_date",
            timezone: "UTC"
          }
        }
      },
      // Count violations for each vehicle within the same month.
      violation_count: { $sum: 1 }
    }
  },
  {
    $match: {
      // Keep vehicles with at least two violations in one month.
      violation_count: { $gte: 2 }
    }
  },
  {
    $group: {
      // Group qualifying vehicle records by month.
      _id: "$_id.month",
      // Count repeat-offender vehicles for each month.
      repeat_offender_vehicles: { $sum: 1 },
      // Calculate the total violations from repeat offenders.
      repeat_violations: { $sum: "$violation_count" },
      max_violations_per_vehicle: {
        // Find the highest monthly violation count for one vehicle.
        $max: "$violation_count"
      }
    }
  },
  {
    $sort: {
      // Rank months by the number of repeat offenders.
      repeat_offender_vehicles: -1,
      _id: 1
    }
  },
  // Return the 10 months with the most repeat offenders.
  { $limit: 10 },
  {
    // Format the final monthly statistics.
    $project: {
      _id: 0,
      month: "$_id",
      repeat_offender_vehicles: 1,
      repeat_violations: 1,
      max_violations_per_vehicle: 1
    }
  }
];

let passed = 0;
let failed = 0;

print("=== BEFORE FAILURE: Q1-Q5 ===");

for (const size of ["100k", "500k", "1m"]) {
  const col = d.getCollection("violations_" + size);

  for (let q = 1; q <= 5; q++) {
    try {
      const results = q === 1
        ? col.find({vehicle_hash: vehicle})
            .maxTimeMS(60000).limit(10).toArray()
        : col.aggregate(
            pipelines[q],
            {allowDiskUse: true, maxTimeMS: 60000}
          ).toArray();

      const expected = q === 1
        ? (size === "1m" ? 2 : 1) : 10;

      const ok = results.length === expected;

      print(size + " Q" + q + ": " +
        (ok ? "PASS" : "FAIL") +
        " - " + results.length + " records");

      if (ok) passed++;
      else failed++;

    } catch (e) {
      print(size + " Q" + q + ": FAIL - " + e.message);
      failed++;
    }
  }
}

print("Passed: " + passed + "/15");
print("Failed: " + failed);

if (failed > 0) quit(2);
