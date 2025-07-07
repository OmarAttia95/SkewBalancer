# SkewBalancer

**SkewBalancer** is a plug-and-play PySpark utility for **automatic skew detection and mitigation** using **value-based salting**, with built-in visual diagnostics, key detection, and optimized repartitioning.

> 🎯 Built for real-world Spark pipelines — no manual tuning, no black magic.  
> 🧠 Works out of the box for both junior engineers and Spark veterans.

---

## 🚀 Features

- 🔍 Automatically detects skewed numeric columns via IQR-based heuristics
- 🧠 Detects best column for `groupBy()` based on low cardinality
- 🔐 Detects primary, composite, or surrogate keys — even UUIDs or alphanumerics
- 🌶️ Applies salting to dense or extreme ranges using percentile buckets
- ⚙️ Repartitions based on salted keys for optimal parallelism
- 📊 Generates:
  - Z-Score bar charts
  - Box plots
  - Histograms (before/after salting)
- 🧾 Logs full Spark `.explain()` plans (original vs salted)

---

## 📦 Installation

```bash
git clone https://github.com/yourusername/SkewBalancer.git
cd SkewBalancer
pip install -e .
```

---

## 🧪 Minimal Usage

```python
from skewbalancer import auto_balance_skew
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("SkewBalancer").getOrCreate()
df = spark.read.csv("your_file.csv", header=True, inferSchema=True)

df_balanced = auto_balance_skew(df, output_dir="outputs", partitions=10, verbose=True)
```

> ✅ Automatically selects:
> - Most skewed numeric column
> - Best groupBy key (low-cardinality)
> ✅ Salts dense zones
> ✅ Repartitions on salted keys
> ✅ Logs and saves 3 types of plots

---

## 🧠 How It Works

### 🔬 Skew Detection (IQR Logic)
```
1. For each numeric column:
2.     Compute Q1, Q2, Q3 percentiles
3.     Calculate skew = abs((Q3 - Q2) - (Q2 - Q1)) / IQR
4. Select the column with the highest skew > threshold
```

### 🧂 Smart Salting
```
1. Compute N percentiles to divide column into buckets
2. For each row:
    - Assign salt ID based on range bucket
    - Concatenate salt ID with original value
3. Create new salted_key column
```

### 🔍 Key Detection
```
1. For each column:
    - Compute % of distinct values and nulls
    - Score = distinct_ratio - null_ratio
2. If score > 0.99 → primary key
3. Else → try composite combinations
4. Else → check UUIDs or surrogates by length + uniqueness
```

### 📏 Schema Inference
```
1. For all string columns:
    - Sample values
    - Infer if int, float, or string
2. Apply nullable=False if column is part of detected key
```

### 📉 Partition Rebalancing
```
1. Repartition by salted key using df.repartition(N, salted_key)
2. Ensures Spark executors share workload evenly
```

---

## 🧰 Methods Overview

| Method | Description |
|--------|-------------|
| `run_full_balance_pipeline()` | Executes analyze → salt → repartition |
| `analyze_distribution()` | Computes p05/p95/mean/std |
| `apply_smart_salting()` | Buckets values and adds salt column |
| `repartition_on_salt()` | Rebalances DataFrame on salted keys |
| `infer_numeric_columns()` | Returns list of numeric columns |
| `detectKey()` | Detects primary/composite/surrogate keys |
| `schemaVisor()` | Infers Spark schema with nullable logic |
| `detect_skewed_column()` | Returns name of most skewed column |
| `detect_low_cardinality_categorical()` | Detects low-card string column for groupBy |
| `log_explain()` | Logs `.explain()` plan to file |
| `show_partition_sizes()` | Shows # of records per partition |
| `timeit()` | Measures execution time of any function |

---

## 📂 Directory Structure

```
SkewBalancer/
├── outputs/                      ← all logs and plots
├── skewbalancer/
│   ├── __init__.py              ← exposes `auto_balance_skew`
│   └── value_skew_balancer.py   ← core logic + class
├── notebook_test.ipynb          ← usage example
├── requirements.txt
├── setup.py
├── README.md
└── LICENSE
```

---

## ⚡ Real-World Use Cases

- Heavy `groupBy().agg()` operations in PySpark
- Customer churn segmentation
- Fraud detection or rare-event tracking
- Resource imbalance in Spark jobs
- Autotuning for unsupervised ETL jobs

---

## 🔮 Future Roadmap

- [ ] Auto-plot Spark executor metrics (stage-level metrics)
- [ ] Detect skew in categorical fields
- [ ] Support for Spark Structured Streaming
- [ ] Rebalance inside Delta Live Tables (DLT)
- [ ] Publish to PyPI and Databricks Marketplace

---

## 🧠 For Morons — Visual Process Breakdown

```
[Spark DataFrame]
       ↓
[Detect Most Skewed Numeric Column] ←── infer_numeric_columns() + skew logic
       ↓
[Detect Best GroupBy Key] ←── detect_low_cardinality_categorical()
       ↓
[Analyze Distribution] ←── analyze_distribution()
       ↓
[Apply Smart Salting] ←── apply_smart_salting()
       ↓
[Repartition on Salted Key] ←── repartition_on_salt()
       ↓
[Balanced DataFrame + Diagnostic Visuals]
```

---

## 👨‍💻 Author

**Omar Attia**  
Mid-level Data Engineer @ IBM  
🔗 [github.com/OmarAttia95](https://github.com/OmarAttia95)

> This library was crafted with real-world Spark pain in mind — enjoy the speed and sanity.

---

## 🪪 License

[MIT License](LICENSE)
