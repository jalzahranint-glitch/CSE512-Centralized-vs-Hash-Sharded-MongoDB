import csv, json, re, subprocess
from pathlib import Path
from datetime import datetime

# Use the script folder as the project root.
root = Path(__file__).resolve().parent
out = root / 'results/benchmarks/server_execution'
# Create the output directory if needed.
out.mkdir(parents=True, exist_ok=True)
filename = out / ('Distributed_Q1_Q5_Server_' + datetime.now().strftime('%Y%m%d_%H%M%S') + '.csv')
router = 'nyc-mongo-distributed-mongos-1'
vehicle = 'e1b3dc8fd32093207a7a965292593e4601fe2825d72d02e58f37d0d438e84dd2'

# Execute MongoDB commands inside a Docker container.
def mongo(container, js):
    result = subprocess.run(
        ['docker', 'exec', container, 'mongosh', '--quiet', '--eval', js],
        text=True, capture_output=True
    )
    if result.returncode:
        raise RuntimeError(container + ': ' + result.stderr[-1000:])
    return result.stdout

# Extract JSON records with the specified prefix.
def tagged(text, prefix):
    return [json.loads(line[len(prefix):])
            for line in text.splitlines()
            if line.startswith(prefix)]

pipelines = {}
# Read the aggregation pipelines for Q2 through Q5.
for i in range(2, 6):
    code = (root / f'queries/q{i}_benchmark_30.js').read_text()
    match = re.search(
        rf'var\s+q{i}pipeline\s*=\s*(\[.*?^\];)',
        code, re.S | re.M
    )
    if not match:
        raise RuntimeError(f'Cannot locate original Q{i} pipeline')
    pipelines[i] = match.group(1)[:-1]

# Define the four MongoDB shard containers.
shards = {
    i: f'nyc-mongo-distributed-shard{i}-1'
    for i in range(1, 5)
}

for name in shards.values():
    # Verify that profiling is enabled on each shard.
    status = tagged(mongo(name, '''
      const s=db.getSiblingDB('nyc_traffic').getProfilingStatus();
      print('STATUS|' + JSON.stringify({
        level:s.was,slowms:s.slowms
      }));
    '''), 'STATUS|')[0]
    if status['level'] != 1 or status['slowms'] != 0:
        raise RuntimeError(f'Enable Profiler on {name} first')

# Define the CSV columns for profiler and client measurements.
columns = [
    'query','dataset','run','shard','server_operation_ms',
    'docs_examined','keys_examined','records_returned',
    'client_with_profiler_ms','status','tag'
]

try:
    # Create a new CSV file without overwriting existing results.
    with filename.open('x', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=columns)
        writer.writeheader()

        # Collect measurements for Q1 through Q5.
        for query in range(1, 6):
            for size in ('100K', '500K', '1M'):
                # Prepare the query settings and a unique run marker.
                cfg = json.dumps({
                    'query':query,'size':size,'vehicle':vehicle,
                    'prefix':datetime.now().strftime('%Y%m%d%H%M%S%f')
                })

                # Build MongoDB commands for five warm-up and 10 measured runs.
                js = '''
                const cfg=__CFG__;
                const c=db.getSiblingDB('nyc_traffic')
                  .getCollection('violations_'+cfg.size.toLowerCase());
                const pipe=__PIPE__;

                function run(tag) {
                  if(cfg.query===1) {
                    let cur=c.find(
                      {vehicle_hash:cfg.vehicle},
                      {_id:0,vehicle_hash:1,issue_date:1,
                       zone:1,violation:1}
                    ).limit(10);
                    if(tag) cur=cur.comment(tag);
                    return cur.toArray();
                  }
                  const opts={allowDiskUse:true};
                  if(tag) opts.comment=tag;
                  return c.aggregate(pipe,opts).toArray();
                }

                for(let i=0;i<5;i++) run(null);

                for(let i=1;i<=10;i++) {
                  const tag='CSE512_'+cfg.prefix+
                    '_Q'+cfg.query+'_'+cfg.size+'_R'+i;
                  const t=Date.now(), result=run(tag);
                  print('RUN|' + JSON.stringify({
                    tag,run:i,count:result.length,
                    ms:Date.now()-t
                  }));
                }
                '''.replace('__CFG__', cfg).replace(
                    '__PIPE__', pipelines.get(query, '[]')
                )

                # Execute the queries through mongos and collect client measurements.
                measures = tagged(mongo(router, js), 'RUN|')
                if len(measures) != 10:
                    raise RuntimeError(
                        f'Expected 10 measurements: Q{query}/{size}'
                    )

                # Collect tags used to identify profiler records.
                tags = [r['tag'] for r in measures]
                profile = {}

                # Prepare a query to find matching profiler records.
                lookup = '''
                const d=db.getSiblingDB('nyc_traffic');
                const tags=__TAGS__;
                const f={$or:[
                  {'command.comment':{$in:tags}},
                  {'originatingCommand.comment':{$in:tags}}
                ]};
                d.system.profile.find(f).forEach(p=>{
                  const tag=p.command?.comment||
                    p.originatingCommand?.comment;
                  print('PROFILE|'+JSON.stringify({
                    tag,ms:p.millis,
                    docs:p.docsExamined??null,
                    keys:p.keysExamined??null
                  }));
                });
                '''.replace('__TAGS__', json.dumps(tags))

                # Collect profiler records from all four shards.
                for n, name in shards.items():
                    for p in tagged(mongo(name, lookup), 'PROFILE|'):
                        profile.setdefault(
                            (p['tag'], n), []
                        ).append(p)

                # Process the profiler results for each measured run.
                for m in measures:
                    # Include each shard even when no profiler record matches.
                    for n in shards:
                        matches = profile.get((m['tag'], n), [])
                        if not matches:
                            matches = [None]
                        for p in matches:
                            writer.writerow(dict(
                                query=f'Q{query}',dataset=size,
                                run=m['run'],shard=n,
                                server_operation_ms=p['ms'] if p else '',
                                docs_examined=p['docs'] if p else '',
                                keys_examined=p['keys'] if p else '',
                                records_returned=m['count'],
                                client_with_profiler_ms=m['ms'],
                                status='RECORDED' if p else 'NO_MATCH',
                                tag=m['tag']
                            ))

                # Write buffered CSV data after each query and dataset size.
                file.flush()
                print(f'Saved Q{query}/{size}: 10 runs')

# Attempt to disable profiling on all four shards.
finally:
    for name in shards.values():
        try:
            mongo(
                name,
                "db.getSiblingDB('nyc_traffic').setProfilingLevel(0,{slowms:100});"
            )
        except Exception as exc:
            print('Could not stop Profiler on', name, exc)

# Display the saved CSV file path.
print('CSV saved:', filename)
