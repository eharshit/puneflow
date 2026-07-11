# PuneFlow — City Traffic Streaming Pipeline

![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-000000?style=for-the-badge&logo=apachekafka)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A2C?style=for-the-badge&logo=apachespark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-007ACC?style=for-the-badge&logo=delta)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-336791?style=for-the-badge&logo=postgresql&logoColor=white)
![Apache Hive](https://img.shields.io/badge/Apache%20Hive-F1A816?style=for-the-badge&logo=apachehive&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![Power BI](https://img.shields.io/badge/Power%20BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)

This repository contains an end-to-end data pipeline that ingests a real-time stream of traffic events, processes them through a multi-stage (Bronze, Silver, Gold) streaming pipeline, stores them in Delta Lake format, and exposes them for reporting via Power BI.

The entire infrastructure (Kafka, Postgres Metastore DB, Hive Metastore, Spark Master, and Spark Worker) is containerized and run locally using Docker Compose.

---

## Architecture Overview

![Pipeline Workflow](workflow.png)

---

## Key Features

* **Delta Lake Storage**: Stores processed streams as Delta tables, providing ACID transactions, schema enforcement, and a persistent transaction log for all medallion layers.
* **Medallion Stream Processing**: Uses PySpark Structured Streaming to process events in three isolated, incremental stages (Bronze -> Silver -> Gold) under Delta format.
* **Hive Metastore DB Integration**: Employs a PostgreSQL-backed external Apache Hive Metastore database, allowing cross-system metadata visibility between Spark, Delta Lake, and BI connections.
* **Star Schema Modeling**: Gold layer outputs tables structured in a clean star schema optimized for analytical query performance.
* **Visualization Layer**: **Power BI DirectQuery Dashboard** connected via Hive Thrift JDBC HTTP transport to dynamically render and query live traffic schemas.

---

## Data Medallion Flow

1. **Bronze (Raw Ingestion)**: Ingests raw JSON events from Kafka and appends them directly to the raw Delta table.
2. **Silver (Cleaned)**: Cleans data (deduplicates, casts data types, filters outliers) and writes to the clean Delta table.
3. **Gold (Aggregated)**: Joins clean traffic facts with road and zone dimensions, creating star schema views for Power BI.

---

## Implemented Star Schema

The pipeline structures the gold data layer into a star schema for rapid reporting:
* **`fact_traffic` (Fact table)**: Records speed, congestion level, peak flags, and weather conditions.
* **`dim_zone` (Dimension table)**: Maps Pune areas to zone types (Commercial, IT Hub, Transit, Residential) and traffic risk levels.
* **`dim_road` (Dimension table)**: Maps road names to types (Highway vs. City road) and speed limits.

<img src="star_schema.png" width="350" alt="Star Schema Diagram">

---

## Port Reference Map

| Service | Port | Description |
| :--- | :--- | :--- |
| **Kafka UI** | `http://localhost:8090` | Visual manager for Kafka topics and messages |
| **Spark Master UI** | `http://localhost:8080` | Spark Standalone cluster manager dashboard |

---

## Getting Started

Please refer to the step-by-step commands and scripts documented in **[Commands.md](Commands.md)** to:
1. Spin up the Docker stack.
2. Launch the Faker simulation stream.
3. Submit the medallion streaming jobs to Spark.
4. Connect your reporting client (Power BI).

