# Centralized vs. Hash-Sharded MongoDB: Query Performance Analysis

CSE 512 - Distributed Database Systems
Arizona State University - Fall 2026

## Overview

This project compares query performance between a centralized MongoDB database and a distributed MongoDB system using four hash-based shards.

We used NYC school-zone speed camera violation data with three dataset sizes: 100K, 500K, and 1M records.

## Technologies

- MongoDB 8.0
- Python and PyMongo
- JavaScript and mongosh
- Docker and Docker Compose

## Project Structure

- Centralized: Data preprocessing, database implementation, queries, and results.
- Distributed: Four-shard MongoDB system, queries, benchmarks, execution plans, and fault tolerance experiments.

## Queries

- Q1: Vehicle-specific violation lookup
- Q2: Repeat-offender identification
- Q3: Cross-borough vehicle detection
- Q4: Borough and monthly violation analysis
- Q5: Monthly repeat-offender analysis

## Evaluation

We used five warm-up runs and 30 measured runs to compare median and P95 query latency.

We also examined indexing, data distribution, shard participation, and server execution time.

## Fault Tolerance

We tested shard failure and recovery.

We then added two secondary members to Shard 4 and tested automatic failover.

The other three shards remained single-member replica sets.

## Main Findings

Hash-based sharding distributed records almost evenly across four shards.

Q1 targeted one shard, while Q2-Q5 involved all four shards.

Query performance depended on the query type and dataset size. Sharding did not always outperform the centralized system.

## Reproducibility

The repository includes source code and saved experimental results.

The cleaned datasets containing 100K, 500K, and 1M records are included in Centralized/data/processed.

Private HMAC keys and Docker database volumes are excluded.

The included datasets can be used to reproduce the database experiments without generating new vehicle identifiers.

See the README files inside Centralized and Distributed for additional details.

## Team

Team 1

Taequanson Clarke

Jawaher Alzahrani
