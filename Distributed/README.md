# Distributed MongoDB - NYC Speed Camera Violations

**Project:** Centralized vs. Hash-Sharded MongoDB: Query Performance Analysis

## 1. Project Overview

This project evaluates a distributed MongoDB system
using hash-based sharding to analyze New York City
school-zone speed camera violations.

The distributed system is compared with a centralized
MongoDB database to evaluate query performance and
scalability as dataset size increases.

## 2. System Architecture

- Database: MongoDB 8.0
- Architecture: Four-shard MongoDB cluster
- Query Router: mongos
- Configuration: Config Server
- Shard Key: vehicle_hash (hashed)
- Deployment: Docker Compose

The system distributes records across four shards
using a hashed shard key.

## 3. Datasets

Three dataset sizes are used:

- 100,000 records
- 500,000 records
- 1,000,000 records

Database: nyc_traffic

Collections:
- violations_100k
- violations_500k
- violations_1m

Vehicle identifiers are pseudonymized using
HMAC-SHA-256.

## 4. Query Implementation

Five queries (Q1-Q5) are implemented.

The main query scripts are located in queries/.

Q1 uses a vehicle_hash lookup.
The remaining queries analyze traffic violation
patterns using MongoDB aggregation pipelines.

## 5. Performance Evaluation

Client latency benchmarks:
- 5 warm-up runs
- 30 measured runs

Server profiling experiments:
- 5 warm-up runs
- 10 measured runs
- MongoDB Database Profiler

Profiler records are collected from the shards.

Client latency and per-shard operation duration
are treated as separate metrics.

## 6. Results

Client benchmark results:
results/benchmarks/

Server execution measurements:
results/benchmarks/server_execution/

The server CSV contains measurements for Q1-Q5
across all three dataset sizes.

## 7. Docker Setup

The Docker Compose configuration is in compose.yaml.

From the CSE512_Final_Project root directory,
start the existing containers:

docker compose -p nyc-mongo-distributed \
  -f Distributed/compose.yaml start

To check container status:

docker compose -p nyc-mongo-distributed \
  -f Distributed/compose.yaml ps

## 8. Important Notes

The existing MongoDB data is stored in Docker
volumes outside this project directory.

Copying this folder alone does not copy the
database contents.

The current setup uses the existing Docker
containers and volumes.

A separate data-loading procedure is required
to reproduce the system on another computer.

Do not use docker compose down -v when
preserving the existing data.

Shard failure and recovery results must be
documented separately after testing.

## 9. Replication and Fault Tolerance

### Replica Set Configuration

The original distributed system used four hash-sharded MongoDB shards, each with a single replica set member.

For the fault tolerance experiment, two additional members were added to shard4RS:

- shard4:27017
- shard4b:27017
- shard4c:27017

All three members were configured with one vote and a priority of one. Data synchronization was verified before failure testing.

The additional Docker services are defined in fault_tolerance.compose.yaml. Each member uses a separate Docker volume.

### Failure and Recovery

Before replication was enabled, Shard 4 was stopped to simulate a failure.

All 15 query checks failed during the failure period because the required shard was unavailable.

After restarting Shard 4, all 15 query checks passed across the 100K, 500K, and 1M datasets.

Detailed results are stored in results/recovery/.

### Automatic Failover

After configuring three replica set members, the original Primary was stopped.

MongoDB automatically elected shard4b as the new Primary.

All 15 query checks passed while the original Primary remained stopped.

After restarting the original member, it rejoined the replica set as a Secondary. All three members were healthy.

Detailed results are stored in results/fault_tolerance/.

### Docker Configuration

To start the existing MongoDB containers, including the additional replica set members, run the following from the project root:

docker compose -p nyc-mongo-distributed -f Distributed/compose.yaml -f Distributed/fault_tolerance.compose.yaml start

### Limitations

Replication was added only to Shard 4. The other shards still use single-member replica sets.

Therefore, the experiment demonstrates fault tolerance for a member failure within Shard 4, not full cluster-wide fault tolerance.

The original performance benchmarks were collected before adding the extra replica members.

Docker volumes are stored outside the project directory and are not included in the submitted project files.
