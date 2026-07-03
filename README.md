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


## 🏗️ System Architecture
The analytics pipeline operates as a coherent, decoupled software solution, scaling from raw data ingestion to a user-facing dashboard.

![Architectural Design](screenshots/archihtecture.png)


## Running the Project
Ensure you have Python 3 installed

Open your terminal and run following commands:
pip install -r requirements.txt
jupyter notebook BODS_Pipeline.ipynb #make sure this is running in another terminal
streamlit run src/app.py


# Key Findings & ML Evaluation

Three machine learning classification architectures (Logistic Regression, Decision Tree, and Cross-Validated Random Forest) were evaluated using Accuracy, Precision, Recall, F1-Score, and ROC-AUC.

Algorithmic Complexity & Performance:

    Logistic Regression: Converged the fastest (~2.8s) with a 0.996 F1-Score.

    Random Forest (CV): Most computationally expensive (~44.9s) but achieved a 1.000 F1-Score.

## Critical Analysis of the 1.0 F1-Score:

The tree-based models achieved perfect metrics, which highlights a strict deterministic relationship within the BODS network rather than a probabilistic one. Non-compliance is heavily dictated by categorical administrative rules tied to specific Operators and their Scope Status. Consequently, the recommendation for regulatory bodies is to pivot away from random auditing and enforce strict API-level data validation for the top 5 high-volume "mega-operators," who account for the vast majority of the network's data throughput.