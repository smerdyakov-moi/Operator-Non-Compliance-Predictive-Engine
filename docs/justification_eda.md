## Exploratory Data Analysis (EDA) & Data Profiling Pipeline


### Overview
This document details the distributed data profiling architecture, advanced statistical aggregation models, and decoupled plotting pipelines engineered to explore real-world compliance patterns.

## 1. Large-Scale Exploration: Summary Profiling

## Core Requirements Fulfilled

    Use PySpark DataFrame operations for large-scale exploration: Leveraging parallelized descriptive summaries over targeted structural attributes to audit dataset health.

    Perform data profiling and quality assessment: Auditing missing record volumes, null bounds, and category schemas   across the primary attributes.

## Code Implementation

``` python
import os
from pyspark.sql.functions import col

# Hardcoded schema column names confirmed by the active session schema output
operator_col = "Operator"
status_col = "Status"
scope_col = "Scope Status"
score_col = "% AVL to Timetables feed matching score"

# Generate general profile summary using distributed evaluation engine
summary_df = df_optimized.select(
    col(operator_col), 
    col(status_col), 
    col(scope_col), 
    col(score_col)
).summary("count", "mean", "stddev", "min", "max")

summary_df.show(truncate=False)
```

## Justification

Directly collecting raw records into local memory to analyze wide datasets introduces critical bottlenecking and driver memory exhaustion risks. The .summary() function optimizes execution by pushing down the computational queries directly to individual cluster executors. The Spark Catalyst Optimizer translates this request into a single physical execution plan that reads, maps, and aggregates partition data locally, returning a tiny, high-level structured profile to the driver node containing count, null state boundaries, and key limits.

## 2. Advanced Statistical Moments: Distributed Aggregations

## Core Requirements Fulfilled

Calculate statistical measures using PySpark functions: Formulating higher-order mathematical moments—including mean, median, standard deviation, skewness, and kurtosis—on distributed target features.

Demonstrate understanding of distributed computing concepts (lazy evaluation): Direct evaluation of statistical moments on zero-variance columns without execution crashes.

## Code Implementation

```python
from pyspark.sql.functions import col, mean, stddev, skewness, kurtosis, percentile_approx

# Explicitly demonstrating PySpark's native aggregation functions
statistical_moments_df = df_optimized.agg(
    mean(col(score_col)).alias("Mean"),
    percentile_approx(col(score_col), 0.5).alias("Median (50th Percentile)"),
    stddev(col(score_col)).alias("Standard_Deviation"),
    skewness(col(score_col)).alias("Skewness"),
    kurtosis(col(score_col)).alias("Kurtosis")
)

statistical_moments_df.show(truncate=False)
```

## Justification

Calculating precise median quantiles over massive tables typically demands an expensive global sort of the data, which triggers intensive cluster shuffles. To circumvent this, the pipeline utilizes percentile_approx(), which implements the Greenwald-Khanna algorithm. This algorithm approximates the median with configurable accuracy bounds while consuming minimal memory overhead per partition.

Furthermore, our target numeric column, % AVL to Timetables feed matching score, evaluated to exactly $0.0$ for its mean ($\mu = 0.0$), median ($Q_2 = 0.0$), and standard deviation ($\sigma = 0.0$) across all $103,437$ rows. This signifies a zero-variance column. In probability theory, calculating skewness ($S$) and kurtosis ($K$) requires dividing the third and fourth central moments by the standard deviation raised to the respective power:

$$S = \frac{\mathbb{E}[(X - \mu)^3]}{\sigma^3}$$

$$K = \frac{\mathbb{E}[(X - \mu)^4]}{\sigma^4}$$

Because $\sigma = 0.0$, computing these values requires dividing by zero. PySpark’s mathematical engine elegantly handles this division-by-zero boundary constraint by writing NULL to the output schema instead of raising an execution-halting exception. This proves that the feature possesses zero statistical variance and will be programmatically dropped during feature selection.

## 3. Outlier Profile Assessment: Parallelized IQR Bounds

## Core Requirements Fulfilled

Perform data profiling and quality assessment (outlier detection): Formulating Interquartile Range ($IQR$) metrics to detect highly skewed operational volume boundaries.

## Code Implementation

```python
# Group and calculate log frequencies per operator
operator_counts = df_optimized.groupby(col(operator_col)).count()

# Calculate first, second (median), and third quartile bounds over parallel partitions
quantiles = operator_counts.approxQuantile("count", [0.25, 0.50, 0.75], 0.05)
q1, median_val, q3 = quantiles[0], quantiles[1], quantiles[2]

# Compute Interquartile Range (IQR) and the outlier fence limits
iqr = q3 - q1
upper_bound = q3 + (1.5 * iqr)

print(f"IQR Summary Stats -> Q1: {q1}, Median: {median_val}, Q3: {q3}")
print(f"IQR Upper Threshold: Operators with > {upper_bound:.2f} logged records are volume outliers.")

outlier_count = operator_counts.filter(col("count") > upper_bound).count()
print(f"Total Outlier Operators Identified: {outlier_count}\n")
```

## Justification

We calculate outliers using the mathematical $IQR$ threshold formulation:

$$\text{IQR} = Q_3 - Q_1$$

$$\text{Upper Outlier Fence} = Q_3 + (1.5 \times \text{IQR})$$

Determining these precise quantiles globally requires a cluster-wide search. To avoid execution latency, we call .approxQuantile() with a strict precision epsilon parameter of $e = 0.05$. This guarantees that our quantile boundary limits are calculated within $5\%$ of the absolute rank positions while bypassing expensive global sorting passes.

The resulting metrics reveal an upper threshold of $1074.0$ records, identifying $10$ operators as extreme high-volume outliers. Because these spikes represent actual physical transit market densities (e.g., Arriva, Go-Ahead) rather than data entry failures, they are retained but flag a critical need for balanced weighting configurations during machine learning preparation.

## 4. Visualizations & Distributions: Strictly Decoupled Plotting

## Core Requirements Fulfilled

Create visualisations converting to Pandas only at the final step: Enforcing a strict memory boundary layout to isolate driver rendering tasks.

Demonstrate data distributions relevant to your business problem: Mapping record volumes across physical operating structures.

## Code Implementation

```python
# Aggregate high-volume operator metrics within the parallel cluster first
top_operators_spark = df_optimized.groupby(col(operator_col)) \
    .count() \
    .orderBy(col("count").desc()) \
    .limit(10)

# CRITICAL CONSTRAINTS: Convert to Pandas ONLY at the final plotting boundary line
pdf_operators = top_operators_spark.toPandas()

# Generate visualization using localized plotting tools
plt.figure(figsize=(12, 6))
sns.barplot(
    x="count", 
    y=operator_col, 
    data=pdf_operators, 
    palette="Blues_r", 
    hue=operator_col, 
    legend=False
)
plt.title("Top 10 Transport Operators by Total Logged Services (Data Distribution Profile)", fontsize=14, fontweight='bold')
plt.xlabel("Total Cleaned Record Volume", fontsize=12)
plt.ylabel("Operator Name", fontsize=12)
plt.tight_layout()

# Save image file to your document tracking paths
os.makedirs(os.path.join(BASE_DIR, "docs", "screenshots"), exist_ok=True)
plt.savefig(os.path.join(BASE_DIR, "docs", "screenshots", "eda_operator_distribution.png"))
plt.show()
```

## Justification

Converting a large-scale DataFrame directly to a localized Pandas object pulling raw rows onto a single driver memory space instantly risks triggering an Out Of Memory (OOM) heap allocation crash.

To safeguard the cluster architecture, we apply a strict Pandas Boundary Pattern. All heavy grouping, counting, and descending sorting operations are completed in a distributed fashion across the parallel executors. We then apply a .limit(10) projection at the Spark layer, reducing the final data structure to a tiny $10 \times 2$ matrix. This microscopic, highly aggregated matrix is the only piece transmitted over the network via .toPandas(), ensuring localized visualization tools compile the bar plot instantly and securely.

## 5. Feature Interaction: Regulatory Compliance Patterns

## Core Requirements Fulfilled

Demonstrate correlations and patterns relevant to your business problem: Investigating structural interactions across categorical features to expose class skew profiles.

## Code Implementation

```python
# Compute categorical crosstab relationship matrix using distributed tasks
cross_tab_spark = df_optimized.crosstab("Status", "Scope Status")

# Convert to Pandas exclusively for localized terminal rendering
pdf_cross_tab = cross_tab_spark.toPandas().set_index("Status_Scope Status")

# Construct stacked vertical horizontal distribution analysis
plt.figure(figsize=(10, 5))
pdf_cross_tab.plot(kind="barh", stacked=True, cmap="Blues", figsize=(10, 6), edgecolor="black")

plt.title("Regulatory Compliance Patterns: Service Status by Scope Classification", fontsize=14, fontweight='bold')
plt.xlabel("Total Cleaned Record Volume", fontsize=12)
plt.ylabel("Service Compliance Status", fontsize=12)
plt.legend(title="Scope Status", bbox_to_anchor=(1.05, 1), loc='upper left')
plt.tight_layout()

plt.savefig(os.path.join(BASE_DIR, "docs", "screenshots", "eda_compliance_patterns.png"))
plt.show()
```

## Justification

Because the target numerical matching metric contains zero variance, classic Pearson correlation ($r$) matrix evaluations are mathematically impossible. To explore meaningful connections, the pipeline constructs a distributed categorical cross-tabulation table using crosstab().

This analysis exposes two key challenges that dictate our machine learning engineering decisions:

Severe Target Class Imbalance: The target categorical column Status is overwhelmingly dominated by the inactive class, with almost zero support metrics representing error or UNKNOWN rows. Evaluating predictive models here using basic classification "Accuracy" is a mathematical trap, as a dummy model predicting inactive universally would score $\approx 95\%$ accuracy. Consequently, our modeling architecture will evaluate performance using Precision, Recall, and F1-Score.

Weak Attribute Correlation: The horizontal distribution confirms that whether a log record is classified as In Scope or Out of Scope, it lands uniformly in the inactive state. This visually demonstrates that Scope Status provides very little predictive leverage, instructing our predictive pipeline to rely on other, higher-entropy attributes (such as operator indices and geographic identifiers) for modeling.
