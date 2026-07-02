# Operator Non-Compliance Predictive Engine

## Overview
A data engineering pipeline designed to ingest, join, and clean Bus Open Data Service (BODS) metadata and compliance reports.

## Technical Configuration
- **Environment**: PySpark (Standalone Local Mode).
- **Dynamic Path Detection**: Uses `os.path.join` and `os.getcwd()` to ensure environment-agnostic execution across different user systems.
- **Spark Session Settings**:
    - `spark.driver.memory`: 2g (Optimized for stable local execution).
    - `spark.sql.shuffle.partitions`: 4 (Configured for efficient join performance).
    - `spark.hadoop.fs.defaultFS`: `file:///` (Ensures local file system compatibility).

## Pipeline Status
- **Ingestion**: Multi-catalogue join implemented using a surgical right-join strategy.
- **Milestone**: Achieved 103,437 records.
- **Current Phase**: Data Ingestion and Join complete; Data Cleaning and Feature Engineering pending.