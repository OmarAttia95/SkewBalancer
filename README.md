# SkewBalancer

**SkewBalancer** is a plug-and-play PySpark utility for **automatically detecting value-based skew** in numeric columns and applying **salted keys** with visual analysis, repartitioning, and logical plan logging — all without requiring the user to understand the internals of Spark shuffling.

> ✅ Designed for Data Engineers and beginners alike.  
> ✅ Optimize `groupBy`, `count`, and other aggregations without tuning Spark manually.

---

## 🚀 Features

- Auto-detect the most skewed numeric column using IQR percentile-based skew analysis
- Auto-detect the best column for `groupBy()` based on low cardinality (≤ 20 distinct values)
- Smart, value-aware salting based on percentile-zoned ranges (e.g., low, dense, high)
- Built-in `skewVisor()` generates:
  - Z-Score bar plots (before vs. after)
  - Boxplots (original vs. salted)
  - Histograms with overlaid distributions
- Intelligent schema optimizer:
  - `detectKey()` for primary, surrogate, or composite key detection
  - `schemaVisor()` for non-nullable inference, character limits, and data type tuning
- Seamless repartitioning on salted compound key for optimal parallelism
- Logs and saves `.explain()` physical plans to visualize improvements in Spark’s DAG
- Fully autonomous with fallback logic — designed for batch or real-time pipelines
- Chainable via `.run_full_balance_pipeline()` and extensible via `auto_balance_skew()`

---

## 📦 Installation

Clone the repo and install locally:

```bash
git clone https://github.com/yourusername/SkewBalancer.git
cd SkewBalancer
pip install -e .
```

---

## 🧪 Usage

Inside your Jupyter Notebook or PySpark script:

```python
from skewbalancer import auto_balance_skew, ValueSkewBalancer
from pyspark.sql import SparkSession

# Start Spark session
spark = SparkSession.builder.master("local[*]").appName("SkewBalancer").getOrCreate()

# Load CSV with all fields as strings (safe default)
df_raw = spark.read.csv("your_file.csv", header=True)

# Use built-in schemaVisor to infer data types and non-nullable keys
new_schema = ValueSkewBalancer.schemaVisor(df_raw)

# Reload data using the optimized schema
df = spark.read.csv("your_file.csv", header=True, schema=new_schema)

# Run SkewBalancer
df_balanced = auto_balance_skew(df, output_dir="outputs", partitions=10, verbose=True)
```

This:
- Automatically detects skewed column and group key
- Applies salting to dense/extreme value ranges
- Logs `.explain()` plans for before and after
- Saves 3 comparison plots via `skewVisor()` inside `outputs/`

---

## 🧠 How it Works

1. **Skew Detection**
   - Uses percentiles and IQR to locate skewed numeric columns
2. **Cardinality Check**
   - Finds a suitable column for `groupBy()` (≤ 20 unique values)
3. **Smart Salting**
   - Divides values into zones (e.g., 10 percentiles)
   - Appends a salt value based on value range
4. **skewVisor Visualization**
   - Auto-generates:
     - ✅ Z-Score bar comparison (original vs salted)
     - ✅ Boxplot of both distributions
     - ✅ Overlaid histogram
5. **Repartitioning**
   - Repartitions on salted key for parallelism and shuffle avoidance
6. **Logical Plan Logs**
   - Saves `.explain()` outputs for performance review
7. **Schema Intelligence**
   - `detectKey()` identifies primary, composite, or surrogate keys
   - `schemaVisor()` builds optimal schema with inferred types

---

## 📂 Directory Structure

```
SkewBalancer/
├── outputs/                      # All logs and plots saved here
├── skewbalancer/
│   ├── __init__.py              # Exposes auto_balance_skew + class
│   └── value_skew_balancer.py   # Core logic including skewVisor
├── notebook_test.ipynb          # Example Jupyter notebook
├── requirements.txt
├── setup.py
├── README.md
└── LICENSE
```

---

## 🔮 Future Enhancements

- Auto-plot Spark executor metrics (task times, shuffle reads)
- Add categorical skew detection (not just numeric)
- Support for structured streaming aggregations (micro-batch)
- Direct integration with Spark UI tooltips / GraphFrames support

---

## 👨‍💻 Author

**Omar Attia** – IBM Data Engineer  
Inventor of `skewVisor` and the first **value-based dynamic salting optimizer** for Spark groupBy.  
🔗 [github.com/OmarAttia95](https://github.com/OmarAttia95)

---

## 🪪 License

[MIT License](LICENSE)
