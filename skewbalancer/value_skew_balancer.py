import os
import time
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from typing import Optional, Dict, Callable, Any
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, rand, concat_ws, when


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

    def apply_salting(self, visualize: bool = True) -> DataFrame:
        if not self._stats:
            self.analyze_distribution()

        p95 = self._stats["p95"]
        p05 = self._stats["p05"]
        mean = self._stats["mean"]
        std = self._stats["std"]

        df = self.df.withColumn(
            "value_group",
            when(col(self.column) > p95, "high")
            .when(col(self.column) < p05, "low")
            .when((col(self.column) >= mean - 0.1 * std) & (col(self.column) <= mean + 0.1 * std), "dense")
            .otherwise("normal")
        )

        df = df.withColumn(
            "salt",
            when(col("value_group") == "high", (rand() * self.salt_count).cast("int"))
            .when(col("value_group") == "dense", (rand() * (self.salt_count // 2)).cast("int"))
            .otherwise(0)
        )

        df = df.withColumn(
            self.salt_col,
            concat_ws("_", col(self.column).cast("string"), col("salt").cast("string"))
        )

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
            raise ValueError("Call apply_salting() first.")
        return self.df_salted.repartition(num_partitions, col(self.salt_col))


    def infer_numeric_columns(df: DataFrame) -> list[str]:
        return [f.name for f in df.schema.fields if f.dataType.simpleString() in {"int", "double", "float", "bigint", "long"}]


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


    def detect_low_cardinality_categorical(df: DataFrame) -> Optional[str]:
        for field in df.schema.fields:
            if field.dataType.simpleString() == "string":
                count = df.select(field.name).distinct().count()
                if count <= 20:
                    return field.name
        return None

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

    print(f"[AUTO-DETECTED] Skewed column = {skew_col}, GroupBy = {group_by_col}")

    balancer = ValueSkewBalancer(df, column=skew_col, salt_count=partitions, output_dir=output_dir)
    df_salted = balancer.apply_salting()
    df_repartitioned = balancer.repartition_on_salt()

    ValueSkewBalancer.timeit(lambda: df.groupBy(group_by_col).count().show(), label="Before Salting")
    ValueSkewBalancer.log_explain(df.groupBy(group_by_col).count(), os.path.join(output_dir, "original_plan.txt"))

    ValueSkewBalancer.timeit(lambda: df_repartitioned.groupBy(group_by_col).count().show(), label="After Salting")
    ValueSkewBalancer.log_explain(df_repartitioned.groupBy(group_by_col).count(), os.path.join(output_dir, "salted_plan.txt"))

    ValueSkewBalancer.show_partition_sizes(df, "Before Salting")
    ValueSkewBalancer.show_partition_sizes(df_repartitioned, "After Salting")

    return df_repartitioned
