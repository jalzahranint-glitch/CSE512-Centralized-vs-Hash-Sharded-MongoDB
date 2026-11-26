import csv
import json
import re
import subprocess
from pathlib import Path
from datetime import datetime

# Use the script folder as the project root.
root = Path(__file__).resolve().parent
out = root / "results/benchmarks/centralized/server_execution"
# Create the output directory if it does not exist.
out.mkdir(parents=True, exist_ok=True)

filename = out / (
    "Centralized_Q1_Q5_Server_" +
    datetime.now().strftime("%Y%m%d_%H%M%S") + ".csv"
)

container = "nyc-mongo-central"
vehicle = "e1b3dc8fd32093207a7a965292593e4601fe2825d72d02e58f37d0d438e84dd2"

# Run JavaScript commands in the centralized MongoDB container.
def mongo(js):
    p = subprocess.run(
        ["docker", "exec", container, "mongosh",
         "--quiet", "--eval", js],
        capture_output=True, text=True
    )
    if p.returncode:
        raise RuntimeError(p.stderr or p.stdout)
    return p.stdout

# Extract JSON records with the specified output prefix.
def extract(output, prefix):
    return [
        json.loads(line[len(prefix):])
        for line in output.splitlines()
        if line.startswith(prefix)
    ]

pipelines = {}

# Read the aggregation pipelines for Q2 through Q5.
for q in range(2, 6):
    path = root / f"queries/q{q}_benchmark_30.js"
    code = path.read_text()
    match = re.search(
        r'(?ms)^const pipeline\s*=\s*(\[.*?^\];)',
        code
    )
    if not match:
        raise RuntimeError(f"Q{q} pipeline missing")
    pipelines[q] = match.group(1)[:-1]

# Check the current MongoDB profiler settings.
status = extract(mongo("""
const s = db.getSiblingDB('nyc_traffic')
            .getProfilingStatus();
print('STATUS|' + JSON.stringify({
  level:s.was, slowms:s.slowms
}));
"""), "STATUS|")

if (len(status) != 1 or
    status[0]["level"] != 1 or
    status[0]["slowms"] != 0):
    raise RuntimeError("Profiler is not ready")

# Define the CSV columns for profiler and client measurements.
columns = [
    "query", "dataset", "run", "server_operation_ms",
    "operation", "docs_examined", "keys_examined",
    "records_returned", "client_with_profiler_ms",
    "status", "tag"
]

try:
    # Create a new CSV file without overwriting an existing file.
    with filename.open("x", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()

        # Collect measurements for Q1 through Q5.
        for q in range(1, 6):
            for size in ["100K", "500K", "1M"]:
                # Prepare the query settings and a unique run marker.
                cfg = json.dumps({
                    "q": q,
                    "size": size,
                    "vehicle": vehicle,
                    "marker": datetime.now().strftime(
                        "%Y%m%d%H%M%S%f"
                    )
                })

                # Build the MongoDB script for warm-up and measured runs.
                js = """
const cfg = __CFG__;
const pipeline = __PIPE__;
const col = db.getSiblingDB('nyc_traffic')
  .getCollection('violations_' + cfg.size.toLowerCase());

function run(tag) {
  if (cfg.q === 1) {
    let cursor = col.find(
      {vehicle_hash:cfg.vehicle},
      {_id:0,vehicle_hash:1,issue_date:1,
       zone:1,violation:1}
    ).hint({vehicle_hash:'hashed'}).limit(10);

    if (tag) cursor = cursor.comment(tag);
    return cursor.toArray();
  }

  const opts = {allowDiskUse:true};
  if (tag) opts.comment = tag;
  return col.aggregate(pipeline,opts).toArray();
}

for (let i=0;i<5;i++) run(null);

for (let i=1;i<=10;i++) {
  const tag = 'CSE512_CENTRAL_' + cfg.marker +
    '_Q' + cfg.q + '_' + cfg.size + '_R' + i;
  const start = Date.now();
  const result = run(tag);
  print('RUN|' + JSON.stringify({
    tag:tag, run:i, count:result.length,
    ms:Date.now()-start
  }));
}
"""
                js = js.replace("__CFG__", cfg)
                js = js.replace(
                    "__PIPE__", pipelines.get(q, "[]")
                )

                # Execute five warm-up runs and collect 10 measured runs.
                runs = extract(mongo(js), "RUN|")

                if len(runs) != 10:
                    raise RuntimeError(
                        f"Q{q}/{size}: missing runs"
                    )

                expected = (
                    {"100K":1, "500K":1, "1M":2}[size]
                    if q == 1 else 10
                )
                if any(r["count"] != expected for r in runs):
                    raise RuntimeError(
                        f"Q{q}/{size}: unexpected result count"
                    )

                # Collect run tags to identify matching profiler records.
                tags = json.dumps([r["tag"] for r in runs])

                # Find matching operations in the MongoDB profiler.
                profile_js = """
const d = db.getSiblingDB('nyc_traffic');
const tags = __TAGS__;
d.system.profile.find({
  $or:[
    {'command.comment':{$in:tags}},
    {'originatingCommand.comment':{$in:tags}}
  ]
}).forEach(p => {
  print('PROFILE|' + JSON.stringify({
    tag:p.command?.comment ||
        p.originatingCommand?.comment,
    ms:p.millis,
    op:p.op,
    docs:p.docsExamined ?? null,
    keys:p.keysExamined ?? null
  }));
});
""".replace("__TAGS__", tags)

                # Group profiler records by their run tags.
                prof = {}
                for p in extract(mongo(profile_js), "PROFILE|"):
                    prof.setdefault(p["tag"], []).append(p)

                recorded = 0
                # Save the available profiler records for each measured run.
                for r in runs:
                    matches = prof.get(r["tag"]) or [None]
                    for p in matches:
                        if p is not None:
                            recorded += 1
                        writer.writerow({
                            "query":f"Q{q}",
                            "dataset":size,
                            "run":r["run"],
                            "server_operation_ms":p["ms"] if p else "",
                            "operation":p["op"] if p else "",
                            "docs_examined":p["docs"] if p else "",
                            "keys_examined":p["keys"] if p else "",
                            "records_returned":r["count"],
                            "client_with_profiler_ms":r["ms"],
                            "status":"RECORDED" if p else "NO_MATCH",
                            "tag":r["tag"]
                        })

                # Write buffered CSV data after each query and dataset size.
                f.flush()
                print(
                    f"Saved Q{q}/{size}: 10 runs, "
                    f"{recorded} profiler entries",
                    flush=True
                )
# Attempt to disable profiling after the measurements.
finally:
    try:
        mongo(
            "db.getSiblingDB('nyc_traffic')"
            ".setProfilingLevel(0,{slowms:100});"
        )
        print("Profiler stopped")
    except Exception as e:
        print("WARNING: Stop Profiler manually:", e)

# Display the saved CSV file path.
print("CSV saved:", filename)
