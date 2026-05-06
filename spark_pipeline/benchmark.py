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
    mode: str
    dataset_label: str
    dataset_path: str
    elapsed_seconds: float
    rows: int
    result_rows: int
    partitions: int
    spark_app_id: str
    correctness: dict
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
    parser.add_argument(
        "--include-method-aware",
        action="store_true",
        help=(
            "Run method-aware groupBy for preprocessed datasets. This is also enabled "
            "automatically when _partition_plan.json exists under --preprocessed."
        ),
    )
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
        partition_plan = read_partition_plan(args.preprocessed)
        manifest = read_manifest(args.preprocessed)
        preprocessed_data_path = resolve_preprocessed_data_path(args.preprocessed, manifest)
        input_reused = bool(manifest.get("input_reused", False)) if manifest else False
        original = spark.read.parquet(str(args.original))
        preprocessed = spark.read.parquet(str(preprocessed_data_path))
        right = prepare_join_right(spark, original, args.join_right, args.key_column)

        results: list[BenchmarkResult] = []
        for workload_name in workloads:
            if workload_name == "group_by":
                results.extend(
                    run_group_by_benchmark(
                        spark,
                        original,
                        preprocessed,
                        original_path=args.original,
                        preprocessed_path=args.preprocessed,
                        key_column=args.key_column,
                        partition_plan=partition_plan,
                        include_method_aware=(args.include_method_aware or partition_plan is not None)
                        and not input_reused,
                        warmup=args.warmup,
                    )
                )
            elif workload_name == "join":
                for mode, label, path, dataframe in [
                    ("baseline", "original", args.original, original),
                    ("physical_only", "preprocessed", args.preprocessed, preprocessed),
                ]:
                    if args.warmup:
                        dataframe.select(args.key_column).limit(1).count()

                    results.append(
                        run_join(
                            spark,
                            dataframe,
                            right,
                            mode=mode,
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


def run_group_by_benchmark(
    spark: SparkSession,
    original: DataFrame,
    preprocessed: DataFrame,
    *,
    original_path: Path,
    preprocessed_path: Path,
    key_column: str,
    partition_plan: dict | None,
    include_method_aware: bool,
    warmup: bool,
) -> list[BenchmarkResult]:
    if warmup:
        original.select(key_column).limit(1).count()
    baseline = run_group_by(
        spark,
        original,
        mode="baseline",
        dataset_label="original",
        dataset_path=original_path,
        key_column=key_column,
    )
    baseline.correctness = correctness_against_baseline(baseline, baseline)

    if warmup:
        preprocessed.select(key_column).limit(1).count()
    physical_only = run_group_by(
        spark,
        preprocessed,
        mode="physical_only",
        dataset_label="preprocessed",
        dataset_path=preprocessed_path,
        key_column=key_column,
    )
    physical_only.correctness = correctness_against_baseline(physical_only, baseline)

    results = [baseline, physical_only]
    if include_method_aware:
        partition_column = resolve_method_aware_partition_column(preprocessed, partition_plan)
        if warmup:
            preprocessed.select(key_column).limit(1).count()
        method_aware = run_method_aware_group_by(
            spark,
            preprocessed,
            mode="method_aware",
            dataset_label="preprocessed",
            dataset_path=preprocessed_path,
            key_column=key_column,
            partition_column=partition_column,
        )
        method_aware.correctness = correctness_against_baseline(method_aware, baseline)
        results.append(method_aware)

    return results


def run_group_by(
    spark: SparkSession,
    dataframe: DataFrame,
    *,
    mode: str,
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
        mode=mode,
        dataset_label=dataset_label,
        dataset_path=str(dataset_path),
        elapsed_seconds=elapsed,
        rows=metrics["rows"],
        result_rows=metrics["result_rows"],
        partitions=dataframe.rdd.getNumPartitions(),
        spark_app_id=spark.sparkContext.applicationId,
        correctness={},
        extra={"max_group_count": metrics["max_group_count"]},
    )


def run_method_aware_group_by(
    spark: SparkSession,
    dataframe: DataFrame,
    *,
    mode: str,
    dataset_label: str,
    dataset_path: Path,
    key_column: str,
    partition_column: str,
) -> BenchmarkResult:
    def action() -> dict:
        partial = dataframe.groupBy(partition_column, key_column).count()
        final = partial.groupBy(key_column).agg(F.sum("count").alias("count"))
        row = final.agg(
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
        mode=mode,
        dataset_label=dataset_label,
        dataset_path=str(dataset_path),
        elapsed_seconds=elapsed,
        rows=metrics["rows"],
        result_rows=metrics["result_rows"],
        partitions=dataframe.rdd.getNumPartitions(),
        spark_app_id=spark.sparkContext.applicationId,
        correctness={},
        extra={
            "max_group_count": metrics["max_group_count"],
            "partition_column": partition_column,
        },
    )


def run_join(
    spark: SparkSession,
    left: DataFrame,
    right: DataFrame,
    *,
    mode: str,
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
        mode=mode,
        dataset_label=dataset_label,
        dataset_path=str(dataset_path),
        elapsed_seconds=elapsed,
        rows=metrics["rows"],
        result_rows=metrics["result_rows"],
        partitions=left.rdd.getNumPartitions(),
        spark_app_id=spark.sparkContext.applicationId,
        correctness={},
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


def correctness_against_baseline(result: BenchmarkResult, baseline: BenchmarkResult) -> dict:
    return {
        "row_count_matches_baseline": result.rows == baseline.rows,
        "result_rows_match_baseline": result.result_rows == baseline.result_rows,
    }


def read_partition_plan(preprocessed_path: Path) -> dict | None:
    plan_path = preprocessed_path / "_partition_plan.json"
    if not plan_path.is_file():
        return None
    return json.loads(plan_path.read_text(encoding="utf-8"))


def read_manifest(preprocessed_path: Path) -> dict | None:
    manifest_path = preprocessed_path / "_manifest.json"
    if not manifest_path.is_file():
        return None
    return json.loads(manifest_path.read_text(encoding="utf-8"))


def resolve_preprocessed_data_path(preprocessed_path: Path, manifest: dict | None) -> Path:
    if manifest and manifest.get("input_reused") and manifest.get("dataset_location"):
        return Path(manifest["dataset_location"])
    return preprocessed_path


def resolve_method_aware_partition_column(
    preprocessed: DataFrame,
    partition_plan: dict | None,
) -> str:
    candidates: list[str] = []
    if partition_plan is not None:
        technical_columns = partition_plan.get("technical_columns") or {}
        partition_column = technical_columns.get("partition_column")
        if partition_column:
            candidates.append(partition_column)

    candidates.extend(["rp_partition", "ap_partition"])
    for column in candidates:
        if column in preprocessed.columns:
            return column

    raise ValueError(
        "method-aware groupBy requires a materialized partition column; "
        f"checked {candidates}, available columns: {preprocessed.columns}"
    )


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
                "mode",
                "dataset_label",
                "dataset_path",
                "elapsed_seconds",
                "rows",
                "result_rows",
                "partitions",
                "spark_app_id",
                "correctness_json",
                "extra_json",
            ],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "workload": row["workload"],
                    "mode": row["mode"],
                    "dataset_label": row["dataset_label"],
                    "dataset_path": row["dataset_path"],
                    "elapsed_seconds": row["elapsed_seconds"],
                    "rows": row["rows"],
                    "result_rows": row["result_rows"],
                    "partitions": row["partitions"],
                    "spark_app_id": row["spark_app_id"],
                    "correctness_json": json.dumps(row["correctness"], sort_keys=True),
                    "extra_json": json.dumps(row["extra"], sort_keys=True),
                }
            )


if __name__ == "__main__":
    main()
