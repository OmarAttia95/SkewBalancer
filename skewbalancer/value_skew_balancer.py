import os
import time
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from typing import Optional, Dict, Callable, Any
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat_ws, when, countDistinct, count, lit, length
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType


class ValueSkewBalancer:
    def __init__(self, df: DataFrame, column: str, salt_col: str = "salted_key", salt_count: int = 10, output_dir: str = "outputs"):
        self.df = df
        self.column = column
        self.salt_col = salt_col
        self.salt_count = salt_count
        self.output_dir = output_dir
        self._stats = None
        self.df_salted = None

        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(os.path.join(self.output_dir, "logs"), exist_ok=True)

    def run_full_balance_pipeline(self, visualize: bool = True, num_partitions: int = 10) -> DataFrame:
        self.analyze_distribution()
        self.apply_smart_salting(visualize=visualize)
        return self.repartition_on_salt(num_partitions=num_partitions)

    def analyze_distribution(self) -> Dict[str, float]:
        stats = self.df.selectExpr(
            f'percentile_approx({self.column}, 0.95) as p95',
            f'percentile_approx({self.column}, 0.05) as p05',
            f'mean({self.column}) as mean',
            f'stddev({self.column}) as std'
        ).first()

        self._stats = {
            "p95": stats["p95"],
            "p05": stats["p05"],
            "mean": stats["mean"],
            "std": stats["std"]
        }
        return self._stats

    def apply_smart_salting(self, visualize: bool = True) -> DataFrame:
        if not self._stats:
            self.analyze_distribution()

        percentiles = list(np.linspace(0, 1, self.salt_count + 1))
        boundaries = self.df.selectExpr(
            f"percentile_approx({self.column}, array({','.join(map(str, percentiles))})) as b"
        ).first()["b"]

        salt_expr = None
        for i in range(len(boundaries) - 1):
            lower = boundaries[i]
            upper = boundaries[i + 1]
            condition = (col(self.column) >= lower) & (col(self.column) < upper if i < len(boundaries) - 2 else col(self.column) <= upper)
            salt_expr = when(condition, i) if salt_expr is None else salt_expr.when(condition, i)
        salt_expr = salt_expr.otherwise(0)

        df = self.df.withColumn("salt", salt_expr)
        df = df.withColumn(self.salt_col, concat_ws("_", col(self.column).cast("string"), col("salt").cast("string")))

        self.df_salted = df

        if visualize:
            self.visualize_comparison()

        return df

    def visualize_comparison(self):
        original = self.df.select(self.column).sample(0.05).toPandas()
        salted = self.df_salted.select(self.column).sample(0.05).toPandas()

        z1 = (original[self.column] - original[self.column].mean()) / original[self.column].std()
        z2 = (salted[self.column] - salted[self.column].mean()) / salted[self.column].std()

        plt.figure(figsize=(14, 4))
        plt.subplot(1, 2, 1)
        plt.bar(range(len(z1)), z1, alpha=0.6)
        plt.axhline(3, color='red', linestyle='--')
        plt.axhline(-3, color='blue', linestyle='--')
        plt.title("Z-Score (Original)")
        plt.subplot(1, 2, 2)
        plt.bar(range(len(z2)), z2, alpha=0.6, color='orange')
        plt.axhline(3, color='red', linestyle='--')
        plt.axhline(-3, color='blue', linestyle='--')
        plt.title("Z-Score (Salted)")
        plt.tight_layout()
        plt.savefig(os.path.join(self.output_dir, f"z_score_comparison_{self.column}.png"))
        plt.close()

        plt.figure(figsize=(12, 4))
        plt.boxplot([original[self.column], salted[self.column]], vert=False, labels=["Original", "Salted"])
        plt.title(f"Box Plot for '{self.column}'")
        plt.tight_layout()
        plt.savefig(os.path.join(self.output_dir, f"box_plot_{self.column}.png"))
        plt.close()

        plt.figure(figsize=(12, 4))
        sns.histplot(original[self.column], kde=True, color="blue", stat="density", label="Original")
        sns.histplot(salted[self.column], kde=True, color="orange", stat="density", label="Salted", alpha=0.6)
        plt.title(f"Histogram for '{self.column}'")
        plt.legend()
        plt.tight_layout()
        plt.savefig(os.path.join(self.output_dir, f"histogram_{self.column}.png"))
        plt.close()

    def repartition_on_salt(self, num_partitions: int = 10) -> DataFrame:
        if self.df_salted is None:
            raise ValueError("Call apply_smart_salting() first.")
        return self.df_salted.repartition(num_partitions, col(self.salt_col))

    @staticmethod
    def infer_numeric_columns(df: DataFrame) -> list[str]:
        return [f.name for f in df.schema.fields if f.dataType.simpleString() in {"int", "double", "float", "bigint", "long"}]

    @staticmethod
    def detectKey(df: DataFrame, max_composite: int = 3, verbose: bool = False) -> Optional[Dict[str, Any]]:
        total = df.count()
        fields = df.schema.fields
        columns = [f.name for f in fields]

        null_counts = df.select([count(when(col(c).isNull(), c)).alias(c) for c in columns]).first().asDict()
        distinct_counts = df.select([countDistinct(col(c)).alias(c) for c in columns]).first().asDict()

        scores = {}
        for c in columns:
            if total > 0:
                score = (distinct_counts[c] / total) - (null_counts[c] / total)
                scores[c] = score
                if verbose:
                    print(f"{c}: distinct={distinct_counts[c]}, nulls={null_counts[c]}, score={score:.4f}")

        primary = max(scores.items(), key=lambda x: x[1])
        if primary[1] >= 0.99:
            return {"type": "primary", "columns": [primary[0]], "confidence": round(primary[1], 4)}

        from itertools import combinations
        candidates = [c for c in columns if null_counts[c] == 0]
        for r in range(2, max_composite + 1):
            for combo in combinations(candidates, r):
                combo_name = "__combo__"
                distinct_combo = df.withColumn(combo_name, concat_ws("-", *[col(c).cast("string") for c in combo]))
                unique_count = distinct_combo.select(countDistinct(col(combo_name))).first()[0]
                if verbose:
                    print(f"Checking combo {combo}: distinct={unique_count}")
                if unique_count / total >= 0.99:
                    return {"type": "composite", "columns": list(combo), "confidence": round(unique_count / total, 4)}

        for c in columns:
            col_sample = df.select(col(c)).dropna().limit(10000)
            uniq_ratio = col_sample.distinct().count() / total
            if uniq_ratio >= 0.99:
                avg_len = col_sample.select(length(col(c))).agg({f"length({c})": "avg"}).first()[0]
                if avg_len > 8:
                    return {"type": "surrogate", "columns": [c], "confidence": round(uniq_ratio, 4)}

        return None

    @staticmethod
    def schemaVisor(df: DataFrame, n_chunks: int = 8) -> StructType:
        key_info = ValueSkewBalancer.detectKey(df, max_composite=3)
        key_columns = key_info["columns"] if key_info else []

        string_cols = [f.name for f in df.schema.fields if f.dataType.simpleString() == "string"]
        schema = []

        for col_name in string_cols:
            sampled = df.select(col_name).dropna().sample(0.2).limit(5000).toPandas()[col_name]
            if sampled.empty:
                dtype = StringType()
            elif sampled.astype(str).str.isnumeric().mean() > 0.95:
                dtype = IntegerType()
            elif sampled.astype(str).str.replace('.', '', 1).str.isnumeric().mean() > 0.95:
                dtype = DoubleType()
            else:
                dtype = StringType()
            is_nullable = col_name not in key_columns
            schema.append(StructField(col_name, dtype, is_nullable))

        for f in df.schema.fields:
            if f.name not in string_cols:
                is_nullable = f.name not in key_columns
                schema.append(StructField(f.name, f.dataType, is_nullable))

        return StructType(schema)

    @staticmethod
    def detect_skewed_column(df: DataFrame, verbose: bool = False) -> Optional[str]:
        max_skew = -1
        skew_col = None
        for col_name in ValueSkewBalancer.infer_numeric_columns(df):
            try:
                q1, q2, q3 = df.selectExpr(f"percentile({col_name}, array(0.25, 0.5, 0.75)) as p").first()["p"]
                iqr = q3 - q1
                skew = abs((q3 - q2) - (q2 - q1)) / iqr if iqr else 0
                if verbose:
                    print(f"{col_name}: skewness = {skew:.2f}")
                if skew > max_skew and skew > 0.1:
                    skew_col = col_name
                    max_skew = skew
            except:
                continue
        return skew_col

    @staticmethod
    def detect_low_cardinality_categorical(df: DataFrame) -> Optional[str]:
        for field in df.schema.fields:
            if field.dataType.simpleString() == "string":
                count = df.select(field.name).distinct().count()
                if count <= 20:
                    return field.name
        raise ValueError("No low-cardinality categorical column found for groupBy.")

    @staticmethod
    def show_partition_sizes(df: DataFrame, label: str = ""):
        sizes = df.rdd.glom().map(len).collect()
        print(f"\n[{label}] Partition sizes:")
        for i, size in enumerate(sizes):
            print(f"  Partition {i}: {size} records")

    @staticmethod
    def log_explain(df: DataFrame, filename: str):
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, "w") as f:
            f.write(df._jdf.queryExecution().toString())

    @staticmethod
    def timeit(func: Callable[..., Any], *args, label: str = "", **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        print(f"[{label}] Time: {end - start:.3f} sec")
        return result

def auto_balance_skew(df: DataFrame, output_dir="outputs", partitions=10, verbose=False) -> DataFrame:
    skew_col = ValueSkewBalancer.detect_skewed_column(df, verbose=verbose)
    group_by_col = ValueSkewBalancer.detect_low_cardinality_categorical(df)

    if not skew_col:
        raise ValueError("No skewed numeric column found")
    if not group_by_col:
        raise ValueError("No categorical groupBy column found")

    print(f"[AUTO-DETECTED] Skewed column = {skew_col}, GroupBy = {group_by_col}", end="\n Thank you for using this tool. - Omar Attia")

    balancer = ValueSkewBalancer(df, column=skew_col, salt_count=partitions, output_dir=output_dir)
    df_salted = balancer.run_full_balance_pipeline()

    ValueSkewBalancer.timeit(lambda: df.groupBy(group_by_col).count().show(), label="Before_Salting")
    ValueSkewBalancer.log_explain(df.groupBy(group_by_col).count(), os.path.join(output_dir, "original_plan.txt"))

    ValueSkewBalancer.timeit(lambda: df_salted.groupBy(group_by_col).count().show(), label="After_Salting")
    ValueSkewBalancer.log_explain(df_salted.groupBy(group_by_col).count(), os.path.join(output_dir, "salted_plan.txt"))

    ValueSkewBalancer.show_partition_sizes(df, "Before_Salting")
    ValueSkewBalancer.show_partition_sizes(df_salted, "After_Salting")

    return df_salted
