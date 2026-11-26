# Centralized MongoDB - NYC Speed Camera Violations

**Project:** Centralized vs. Hash-Sharded MongoDB: Query Performance Analysis

## Overview
This directory contains the centralized MongoDB implementation
used as the baseline for comparison with a four-shard
distributed MongoDB system.

The project analyzes New York City school-zone speed camera
violations using datasets of 100,000, 500,000, and
1,000,000 records.

## Database
- Database: MongoDB 8.0
- Database name: nyc_traffic
- Collections: violations_100k, violations_500k, violations_1m
- Architecture: Single MongoDB server
- Vehicle identifier: Pseudonymized vehicle_hash

## Queries
Five queries (Q1-Q5) are used to analyze traffic violations.

The main benchmark scripts are located in queries/.

## Performance Evaluation
Client latency benchmarks use:
- 5 warm-up runs
- 30 measured runs

Server profiling experiments use:
- 5 warm-up runs
- 10 measured runs
- MongoDB Database Profiler

## Results
Client benchmark results are stored in:
results/benchmarks/centralized/

Server measurements are stored in:
results/benchmarks/centralized/server_execution/

## Notes
The centralized system serves as the baseline for evaluating
the performance of hash-based sharding.

The data and Docker environment must be configured before
reproducing the experiments.
