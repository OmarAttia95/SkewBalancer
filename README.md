# SkewBalancer: Automatic Value-Based Skew Fix for Apache Spark

SkewBalancer is a plug-and-play PySpark utility that **automatically detects** value-based skew in numeric columns and **applies smart salting** to balance your data—no deep Spark knowledge required. 🚀

> **For Beginners:** Think of *skewed data* as having too many of the same values in one group, which slows down processing. SkewBalancer spreads those values out evenly.

---

## 🚀 Key Features

- **Auto Skew Detection:** Finds the most skewed numeric column using IQR and percentiles.
- **Smart Group Key Selection:** Picks a low-cardinality column (≤ 20 uniques) for grouping.
- **Value-Aware Salting:** Splits data into percentile-based “zones” and salts keys.
- **Interactive Visual Reports:**  
  - Z-Score bar charts (before vs. after)  
  - Box plots comparing distributions  
  - Histograms with overlays  
- **Repartition for Performance:** Rebalances partitions on the new salted key.
- **Schema Intelligence:**  
  - `.detectKey()` finds primary/composite keys  
  - `.schemaVisor()` infers optimal Spark types and nullability
- **Logging & Explain Plans:** Saves `.explain()` outputs for side-by-side DAG comparison.
- **One-Click Pipeline:** Chain everything with `.run_full_balance_pipeline()` or `auto_balance_skew()`.

---

## 🛠️ Installation

```bash
git clone https://github.com/yourusername/SkewBalancer.git
cd SkewBalancer
pip install -e .
```

> **Tip:** Installing in “editable” mode lets you tweak the code locally.

---

## 📘 Usage Example

```python
from pyspark.sql import SparkSession
from skewbalancer import auto_balance_skew, ValueSkewBalancer

# Start Spark
spark = SparkSession.builder     .appName("SkewBalancerTest")     .master("local[*]")     .getOrCreate()

# Load data (all columns as strings)
df_raw = spark.read.csv("data.csv", header=True)

# Smart schema detection
optimized_schema = ValueSkewBalancer.schemaVisor(df_raw)
df = spark.read.csv("data.csv", header=True, schema=optimized_schema)

# Auto-fix skew
df_balanced = auto_balance_skew(
    df,
    output_dir="outputs",   # where plots/logs go
    partitions=10,          # target number of partitions
    verbose=True
)
```

> **Note for Newcomers:**  
> 1. **Salted Key**: a new column (`salted_key`) that mixes your original value with a small salt number to spread out heavy hitters.  
> 2. **Partitions**: think of these as “buckets” Spark uses; more even buckets = faster jobs.

---

## 🧠 How It Works

1. **Detect Skew:** Computes percentiles and IQR to find skewed numeric columns.  
2. **Select Group Key:** Chooses a categorical (string) column with few unique values.  
3. **Apply Salting:** Divides numeric values into zones and appends a salt suffix.  
4. **Visualize & Log:** Generates plots and `.explain()` plans for before/after.  
5. **Repartition:** Rebalances data on the salted key for optimal parallelism.

---

## 📂 Project Structure

```
SkewBalancer/
├── outputs/                   # Plots & logs
├── skewbalancer/
│   ├── __init__.py            # Exposes public API
│   └── value_skew_balancer.py # Core logic
├── notebook_test.ipynb        # Step-by-step example
├── README.md                  # (You are here)
├── requirements.txt
└── setup.py
```

---

## 🔮 Future Enhancements

- Auto-plot Spark executor metrics (task times, shuffle stats).  
- Categorical skew detection (beyond numeric).  
- Streaming mode support (micro-batches).  
- Spark UI integration (GraphFrames links).

---

## 👨‍💻 Author

**Omar Attia** – IBM Data Engineer  
Creator of SkewBalancer and the first **value-based dynamic salting optimizer** for Spark.  
🔗 [GitHub: OmarAttia95](https://github.com/OmarAttia95)

---

## 🪪 License

[MIT License](LICENSE)
