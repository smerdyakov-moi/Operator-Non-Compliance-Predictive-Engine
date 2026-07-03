# Operator Non-Compliance Predictive Engine

## Dataset Architecture & Mapping Logic
To maximize data density and performance efficiency, the pipeline consolidates tracking operations by combining two massive, highly complementary datasets instead of introducing redundant flat files:

1.  **Static Schedule Layer (`overall_data_catalogue.csv`)**: Contains the comprehensive national timetable metadata, service registration profiles, and operator codes.
2.  **Historical Tracking Layer (`overall_compliance_report.csv`)**: Captures the aggregated performance metrics derived from raw vehicle monitoring logs. The core engine relies on the `% AVL to Timetables feed matching score` column.

### Technical Alignment with the Brief:
*   **AVL (Automatic Vehicle Location)** tracking metrics represent the aggregated analytical snapshot of historical GPS tracking feeds compiled by transit authorities.
*   The "matching score" is the direct mathematical result of cross-referencing past location telemetry against scheduled timetables to measure reliability.
*   By executing a surgical join across these two specific layers, the pipeline structurally links **Timetable Schedule Data** with **Historical Location Performance Data** via primary service keys (`Service Code` / `Registration:Service Number`), fulfilling the architectural intent of the brief using an optimized footprint.

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
- **Current Phase**: EDA Complete, Working on ML Pipeline
