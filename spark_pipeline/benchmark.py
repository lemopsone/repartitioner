#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Callable, Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


@dataclass
class BenchmarkResult:
    workload: str
    dataset_label: str
    dataset_path: str
    elapsed_seconds: float
    rows: int
    result_rows: int
    partitions: int
    spark_app_id: str
    extra: dict


def main() -> None:
    parser = base_parser("Run Spark groupBy/join benchmarks against original and preprocessed datasets.")
    parser.add_argument(
        "--workload",
        choices=["group_by", "join", "all"],
        default="all",
        help="Spark workload to run.",
    )
    args = parser.parse_args()
    run_from_args(args)


def base_parser(description: str) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--original", required=True, type=Path, help="Original Parquet dataset path.")
    parser.add_argument(
        "--preprocessed",
        required=True,
        type=Path,
        help="Preprocessed Spark-compatible Parquet dataset path.",
    )
    parser.add_argument("--key-column", default="user_id")
    parser.add_argument(
        "--join-right",
        type=Path,
        help="Optional right-side Parquet dataset for join workload. Must contain the key column.",
    )
    parser.add_argument("--json-report", required=True, type=Path)
    parser.add_argument("--csv-report", required=True, type=Path)
    parser.add_argument("--app-name", default="repartitioner-benchmark")
    parser.add_argument("--shuffle-partitions", type=int, default=200)
    parser.add_argument("--warmup", action="store_true", help="Run one unmeasured action before timing.")
    return parser


def run_from_args(args: argparse.Namespace, workload_override: str | None = None) -> list[BenchmarkResult]:
    spark = (
        SparkSession.builder.appName(args.app_name)
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        .getOrCreate()
    )
    try:
        workload = workload_override or args.workload
        workloads = ["group_by", "join"] if workload == "all" else [workload]
        original = spark.read.parquet(str(args.original))
        preprocessed = spark.read.parquet(str(args.preprocessed))
        right = prepare_join_right(spark, original, args.join_right, args.key_column)

        results: list[BenchmarkResult] = []
        for workload_name in workloads:
            for label, path, dataframe in [
                ("original", args.original, original),
                ("preprocessed", args.preprocessed, preprocessed),
            ]:
                if args.warmup:
                    dataframe.select(args.key_column).limit(1).count()

                if workload_name == "group_by":
                    results.append(
                        run_group_by(
                            spark,
                            dataframe,
                            dataset_label=label,
                            dataset_path=path,
                            key_column=args.key_column,
                        )
                    )
                elif workload_name == "join":
                    results.append(
                        run_join(
                            spark,
                            dataframe,
                            right,
                            dataset_label=label,
                            dataset_path=path,
                            key_column=args.key_column,
                        )
                    )
                else:
                    raise ValueError(f"unsupported workload: {workload_name}")

        write_reports(results, args.json_report, args.csv_report)
        return results
    finally:
        spark.stop()


def run_group_by(
    spark: SparkSession,
    dataframe: DataFrame,
    *,
    dataset_label: str,
    dataset_path: Path,
    key_column: str,
) -> BenchmarkResult:
    def action() -> dict:
        grouped = dataframe.groupBy(key_column).count()
        row = grouped.agg(
            F.count(F.lit(1)).alias("result_rows"),
            F.sum("count").alias("rows"),
            F.max("count").alias("max_group_count"),
        ).collect()[0]
        return {
            "rows": int(row["rows"] or 0),
            "result_rows": int(row["result_rows"] or 0),
            "max_group_count": int(row["max_group_count"] or 0),
        }

    elapsed, metrics = timed(action)
    return BenchmarkResult(
        workload="group_by",
        dataset_label=dataset_label,
        dataset_path=str(dataset_path),
        elapsed_seconds=elapsed,
        rows=metrics["rows"],
        result_rows=metrics["result_rows"],
        partitions=dataframe.rdd.getNumPartitions(),
        spark_app_id=spark.sparkContext.applicationId,
        extra={"max_group_count": metrics["max_group_count"]},
    )


def run_join(
    spark: SparkSession,
    left: DataFrame,
    right: DataFrame,
    *,
    dataset_label: str,
    dataset_path: Path,
    key_column: str,
) -> BenchmarkResult:
    def action() -> dict:
        joined = left.join(right, on=key_column, how="inner")
        row = joined.agg(
            F.count(F.lit(1)).alias("rows"),
            F.countDistinct(key_column).alias("result_rows"),
        ).collect()[0]
        return {
            "rows": int(row["rows"] or 0),
            "result_rows": int(row["result_rows"] or 0),
        }

    elapsed, metrics = timed(action)
    return BenchmarkResult(
        workload="join",
        dataset_label=dataset_label,
        dataset_path=str(dataset_path),
        elapsed_seconds=elapsed,
        rows=metrics["rows"],
        result_rows=metrics["result_rows"],
        partitions=left.rdd.getNumPartitions(),
        spark_app_id=spark.sparkContext.applicationId,
        extra={"right_partitions": right.rdd.getNumPartitions()},
    )


def prepare_join_right(
    spark: SparkSession,
    original: DataFrame,
    join_right: Path | None,
    key_column: str,
) -> DataFrame:
    if join_right is not None:
        right = spark.read.parquet(str(join_right)).select(key_column).dropDuplicates([key_column])
    else:
        right = original.select(key_column).dropDuplicates([key_column])

    return right.withColumn("join_payload", F.xxhash64(F.col(key_column))).cache()


def timed(action: Callable[[], dict]) -> tuple[float, dict]:
    started = time.perf_counter()
    result = action()
    return time.perf_counter() - started, result


def write_reports(results: Iterable[BenchmarkResult], json_report: Path, csv_report: Path) -> None:
    rows = [asdict(result) for result in results]
    json_report.parent.mkdir(parents=True, exist_ok=True)
    csv_report.parent.mkdir(parents=True, exist_ok=True)
    json_report.write_text(json.dumps({"results": rows}, indent=2), encoding="utf-8")

    with csv_report.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=[
                "workload",
                "dataset_label",
                "dataset_path",
                "elapsed_seconds",
                "rows",
                "result_rows",
                "partitions",
                "spark_app_id",
                "extra_json",
            ],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "workload": row["workload"],
                    "dataset_label": row["dataset_label"],
                    "dataset_path": row["dataset_path"],
                    "elapsed_seconds": row["elapsed_seconds"],
                    "rows": row["rows"],
                    "result_rows": row["result_rows"],
                    "partitions": row["partitions"],
                    "spark_app_id": row["spark_app_id"],
                    "extra_json": json.dumps(row["extra"], sort_keys=True),
                }
            )


if __name__ == "__main__":
    main()
