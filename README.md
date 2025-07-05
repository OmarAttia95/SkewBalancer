# SkewBalancer

**SkewBalancer** is a plug-and-play PySpark utility for **automatically detecting value-based skew** in numeric columns and applying **salted keys** with visual analysis, repartitioning, and logical plan logging — all without requiring the user to understand internals of Spark shuffling.

> ✅ Designed for Data Engineers and beginners alike.  
> ✅ Optimize `groupBy`, `count`, and other aggregations without tuning Spark manually.

---

## 🚀 Features

- 🔍 Auto-detect the most skewed numeric column
- 🧠 Auto-detect best column for `groupBy()` based on cardinality
- 🌶️ Value-aware salting using **high**, **dense**, and **low** value zones
- 📊 Generates Z-Score, Boxplot, and Histogram comparison visuals
- ⚙️ Repartitions on salted key for optimal parallelism
- 🧾 Saves `.explain()` physical plans for both original and optimized versions

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
from skewbalancer import auto_balance_skew
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("SkewBalancer").getOrCreate()
df = spark.read.csv("your_file.csv", header=True, inferSchema=True)

df_balanced = auto_balance_skew(df, output_dir="outputs", partitions=10, verbose=True)
```

This:
- Automatically detects skewed column and group key
- Applies salting to dense/extreme values
- Logs `.explain()` plans
- Saves 3 plots inside `outputs/`

---

## 🧠 How it Works

- Detects skew using IQR and percentile analysis
- Classifies values into **low**, **dense**, **high**, **normal**
- Applies `rand()` salting on selected zones
- Replaces group keys with salted compound key
- Repartitions Spark DataFrame to minimize shuffle skew

---

## 📂 Directory Structure

```
SkewBalancer/
├── outputs/                      # All logs and plots saved here
├── skewbalancer/
│   ├── __init__.py              # Exposes auto_balance_skew + class
│   └── value_skew_balancer.py   # Core logic
├── notebook_test.ipynb          # Example Jupyter notebook
├── requirements.txt
├── setup.py
├── README.md
└── LICENSE
```

---

## 🔮 Future Enhancements

- Auto-plot Spark executor metrics
- Add categorical skew detection (not just numeric)
- Support for streaming aggregations (micro-batch)
- Integration with Spark UI tooltips

---

## 👨‍💻 Author

**Omar Attia** – IBM Data Engineer  
🔗 [github.com/OmarAttia95](https://github.com/omattia)

---

## 🪪 License

[MIT License](LICENSE)