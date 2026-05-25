#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
import subprocess
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable, Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


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
    skipped: bool = False
    skip_reason: str | None = None


def main() -> None:
    parser = base_parser("Run Spark scan/filter/groupBy/join benchmarks.")
    parser.add_argument(
        "--workload",
        choices=["scan", "filter", "group_by", "join", "all"],
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
    parser.add_argument("--driver-memory", default="8g")
    parser.add_argument("--executor-memory", default="8g")
    parser.add_argument(
        "--auto-broadcast-threshold-bytes",
        type=int,
        default=-1,
        help="Spark auto broadcast join threshold. Default disables auto broadcast.",
    )
    parser.add_argument(
        "--enable-aqe",
        action="store_true",
        help="Enable Spark adaptive query execution. Disabled by default for repeatable skew tests.",
    )
    parser.add_argument(
        "--parquet-batch-size",
        type=int,
        default=1024,
        help="Spark vectorized Parquet reader batch size if vectorized reader is enabled.",
    )
    parser.add_argument(
        "--enable-vectorized-parquet",
        action="store_true",
        help="Enable Spark vectorized Parquet reader. Disabled by default to reduce heap spikes.",
    )
    parser.add_argument(
        "--enable-vectored-io",
        action="store_true",
        help="Enable Parquet/Hadoop vectored IO. Disabled by default to reduce local FS heap spikes.",
    )
    parser.add_argument("--warmup", action="store_true", help="Run one unmeasured action before timing.")
    parser.add_argument(
        "--include-method-aware",
        action="store_true",
        help=(
            "Run method-aware groupBy for preprocessed datasets. This is also enabled "
            "automatically when _partition_plan.json exists under --preprocessed."
        ),
    )
    parser.add_argument(
        "--correctness-level",
        choices=["none", "basic", "full"],
        default="basic",
        help=(
            "Correctness checks after timing. basic compares row/result counts; "
            "full also runs expensive group/join checksum checks."
        ),
    )
    return parser


def verify_java_runtime() -> None:
    completed = subprocess.run(
        ["java", "-version"],
        text=True,
        capture_output=True,
        check=False,
    )
    output = "\n".join(part for part in [completed.stdout, completed.stderr] if part)
    if completed.returncode != 0:
        raise SystemExit(
            "Cannot run `java -version`. Install JDK 17 and make sure java is on PATH."
        )

    major = parse_java_major(output)
    if major is not None and major > 21:
        raise SystemExit(
            "Unsupported Java runtime for Spark/Hadoop: detected Java "
            f"{major}. The benchmark scripts should be run with JDK 17. "
            "Set it in the current shell, for example:\n"
            "  export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64\n"
            '  export PATH="$JAVA_HOME/bin:$PATH"'
        )


def parse_java_major(version_output: str) -> int | None:
    match = re.search(r'version "([^"]+)"', version_output)
    if match is None:
        return None

    version = match.group(1)
    if version.startswith("1."):
        parts = version.split(".")
        return int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else None

    major = version.split(".", maxsplit=1)[0].split("-", maxsplit=1)[0]
    return int(major) if major.isdigit() else None


def bool_string(value: bool) -> str:
    return "true" if value else "false"


def run_from_args(args: argparse.Namespace, workload_override: str | None = None) -> list[BenchmarkResult]:
    verify_java_runtime()
    spark = (
        SparkSession.builder.appName(args.app_name)
        .config("spark.driver.memory", args.driver_memory)
        .config("spark.executor.memory", args.executor_memory)
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        .config("spark.sql.autoBroadcastJoinThreshold", str(args.auto_broadcast_threshold_bytes))
        .config("spark.sql.adaptive.enabled", "true" if args.enable_aqe else "false")
        .config("spark.sql.parquet.columnarReaderBatchSize", str(args.parquet_batch_size))
        .config("spark.sql.parquet.enableVectorizedReader", bool_string(args.enable_vectorized_parquet))
        .config("spark.hadoop.parquet.hadoop.vectored.io.enabled", bool_string(args.enable_vectored_io))
        .getOrCreate()
    )
    try:
        return run_with_spark(spark, args, workload_override=workload_override)
    finally:
        spark.stop()


def run_with_spark(
    spark: SparkSession,
    args: argparse.Namespace,
    workload_override: str | None = None,
) -> list[BenchmarkResult]:
    workload = workload_override or args.workload
    workloads = ["scan", "filter", "group_by", "join"] if workload == "all" else [workload]
    partition_plan = read_partition_plan(args.preprocessed)
    manifest = read_manifest(args.preprocessed)
    preprocessed_data_path = resolve_preprocessed_data_path(args.preprocessed, manifest)
    input_reused = bool(manifest.get("input_reused", False)) if manifest else False
    original = spark.read.parquet(str(args.original))
    preprocessed = spark.read.parquet(str(preprocessed_data_path))
    right = prepare_join_right(spark, original, args.join_right, args.key_column)

    try:
        results: list[BenchmarkResult] = []
        for workload_name in workloads:
            if workload_name == "scan":
                results.extend(
                    run_scan_benchmark(
                        spark,
                        original,
                        preprocessed,
                        original_path=args.original,
                        preprocessed_path=args.preprocessed,
                        warmup=args.warmup,
                    )
                )
            elif workload_name == "filter":
                results.extend(
                    run_filter_benchmark(
                        spark,
                        original,
                        preprocessed,
                        original_path=args.original,
                        preprocessed_path=args.preprocessed,
                        key_column=args.key_column,
                        warmup=args.warmup,
                    )
                )
            elif workload_name == "group_by":
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
                        correctness_level=args.correctness_level,
                        warmup=args.warmup,
                    )
                )
            elif workload_name == "join":
                results.extend(
                    run_join_benchmark(
                        spark,
                        original,
                        preprocessed,
                        right,
                        original_path=args.original,
                        preprocessed_path=args.preprocessed,
                        key_column=args.key_column,
                        partition_plan=partition_plan,
                        include_method_aware=(args.include_method_aware or partition_plan is not None),
                        input_reused=input_reused,
                        correctness_level=args.correctness_level,
                        warmup=args.warmup,
                    )
                )
            else:
                raise ValueError(f"unsupported workload: {workload_name}")

        write_reports(results, args.json_report, args.csv_report)
        return results
    finally:
        try:
            right.unpersist(blocking=False)
            spark.catalog.clearCache()
        except Exception:
            pass


def run_scan_benchmark(
    spark: SparkSession,
    original: DataFrame,
    preprocessed: DataFrame,
    *,
    original_path: Path,
    preprocessed_path: Path,
    warmup: bool,
) -> list[BenchmarkResult]:
    if warmup:
        original.limit(1).count()
    baseline = run_scan(
        spark,
        original,
        mode="baseline",
        dataset_label="original",
        dataset_path=original_path,
    )

    if warmup:
        preprocessed.limit(1).count()
    physical_only = run_scan(
        spark,
        preprocessed,
        mode="physical_only",
        dataset_label="preprocessed",
        dataset_path=preprocessed_path,
    )
    physical_only.correctness = correctness_against_baseline(physical_only, baseline)
    baseline.correctness = correctness_against_baseline(baseline, baseline)
    return [baseline, physical_only]


def run_filter_benchmark(
    spark: SparkSession,
    original: DataFrame,
    preprocessed: DataFrame,
    *,
    original_path: Path,
    preprocessed_path: Path,
    key_column: str,
    warmup: bool,
) -> list[BenchmarkResult]:
    if warmup:
        original.select(key_column).limit(1).count()
    baseline = run_filter(
        spark,
        original,
        mode="baseline",
        dataset_label="original",
        dataset_path=original_path,
        key_column=key_column,
    )

    if warmup:
        preprocessed.select(key_column).limit(1).count()
    physical_only = run_filter(
        spark,
        preprocessed,
        mode="physical_only",
        dataset_label="preprocessed",
        dataset_path=preprocessed_path,
        key_column=key_column,
    )
    physical_only.correctness = correctness_against_baseline(physical_only, baseline)
    baseline.correctness = correctness_against_baseline(baseline, baseline)
    return [baseline, physical_only]


def run_scan(
    spark: SparkSession,
    dataframe: DataFrame,
    *,
    mode: str,
    dataset_label: str,
    dataset_path: Path,
) -> BenchmarkResult:
    def action() -> dict:
        work_columns = numeric_work_columns(dataframe)
        row = dataframe.agg(
            F.count(F.lit(1)).alias("rows"),
            *sum_aggregations(work_columns),
        ).collect()[0]
        return {
            "rows": int(row["rows"] or 0),
            "work_checksum": checksum_from_row(row, work_columns),
            "work_columns": work_columns,
        }

    elapsed, metrics = timed(action)
    return BenchmarkResult(
        workload="scan",
        mode=mode,
        dataset_label=dataset_label,
        dataset_path=str(dataset_path),
        elapsed_seconds=elapsed,
        rows=metrics["rows"],
        result_rows=metrics["rows"],
        partitions=dataframe.rdd.getNumPartitions(),
        spark_app_id=spark.sparkContext.applicationId,
        correctness={},
        extra={
            "operation": "count_and_payload_sums",
            "work_columns": metrics["work_columns"],
            "work_checksum": metrics["work_checksum"],
        },
    )


def run_filter(
    spark: SparkSession,
    dataframe: DataFrame,
    *,
    mode: str,
    dataset_label: str,
    dataset_path: Path,
    key_column: str,
) -> BenchmarkResult:
    def action() -> dict:
        filtered = dataframe.filter(F.pmod(F.xxhash64(F.col(key_column)), F.lit(10)) == F.lit(0))
        work_columns = numeric_work_columns(filtered)
        row = filtered.agg(
            F.count(F.lit(1)).alias("rows"),
            *sum_aggregations(work_columns),
        ).collect()[0]
        return {
            "rows": int(row["rows"] or 0),
            "work_checksum": checksum_from_row(row, work_columns),
            "work_columns": work_columns,
        }

    elapsed, metrics = timed(action)
    return BenchmarkResult(
        workload="filter",
        mode=mode,
        dataset_label=dataset_label,
        dataset_path=str(dataset_path),
        elapsed_seconds=elapsed,
        rows=metrics["rows"],
        result_rows=metrics["rows"],
        partitions=dataframe.rdd.getNumPartitions(),
        spark_app_id=spark.sparkContext.applicationId,
        correctness={},
        extra={
            "predicate": "pmod(xxhash64(key), 10) == 0",
            "operation": "filter_count_and_payload_sums",
            "work_columns": metrics["work_columns"],
            "work_checksum": metrics["work_checksum"],
        },
    )


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
    correctness_level: str,
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

    results = [baseline, physical_only]
    if correctness_level == "none":
        baseline.correctness = {}
        physical_only.correctness = {}
    elif correctness_level == "basic":
        baseline.correctness = correctness_against_baseline(baseline, baseline)
        physical_only.correctness = correctness_against_baseline(physical_only, baseline)
    else:
        baseline_grouped = group_by_result(original, key_column)
        physical_grouped = group_by_result(preprocessed, key_column)
        baseline.correctness = group_by_correctness_against_baseline(
            baseline,
            baseline,
            baseline_grouped,
            baseline_grouped,
            key_column,
        )
        physical_only.correctness = group_by_correctness_against_baseline(
            physical_only,
            baseline,
            baseline_grouped,
            physical_grouped,
            key_column,
        )
    if include_method_aware:
        partition_column = resolve_method_aware_partition_column(preprocessed, partition_plan)
        salt_column = resolve_method_aware_salt_column(preprocessed, partition_plan)
        partial_group_keys, method_aware_extra = resolve_method_aware_partial_group_keys(
            preprocessed,
            partition_plan,
            key_column=key_column,
            partition_column=partition_column,
            salt_column=salt_column,
        )
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
            salt_column=salt_column,
            partial_group_keys=partial_group_keys,
            method_aware_extra=method_aware_extra,
        )
        if correctness_level == "none":
            method_aware.correctness = {}
        elif correctness_level == "basic":
            method_aware.correctness = correctness_against_baseline(method_aware, baseline)
        else:
            method_aware_grouped = method_aware_group_by_result(
                preprocessed,
                key_column=key_column,
                partial_group_keys=partial_group_keys,
            )
            method_aware.correctness = group_by_correctness_against_baseline(
                method_aware,
                baseline,
                baseline_grouped,
                method_aware_grouped,
                key_column,
            )
        results.append(method_aware)

    return results


def run_join_benchmark(
    spark: SparkSession,
    original: DataFrame,
    preprocessed: DataFrame,
    right: DataFrame,
    *,
    original_path: Path,
    preprocessed_path: Path,
    key_column: str,
    partition_plan: dict | None,
    include_method_aware: bool,
    input_reused: bool,
    correctness_level: str,
    warmup: bool,
) -> list[BenchmarkResult]:
    if warmup:
        original.select(key_column).limit(1).count()
    baseline = run_join(
        spark,
        original,
        right,
        mode="baseline",
        dataset_label="original",
        dataset_path=original_path,
        key_column=key_column,
    )

    if warmup:
        preprocessed.select(key_column).limit(1).count()
    physical_only = run_join(
        spark,
        preprocessed,
        right,
        mode="physical_only",
        dataset_label="preprocessed",
        dataset_path=preprocessed_path,
        key_column=key_column,
    )

    results = [baseline, physical_only]
    if correctness_level == "none":
        baseline.correctness = {}
        physical_only.correctness = {}
    elif correctness_level == "basic":
        baseline.correctness = correctness_against_baseline(baseline, baseline)
        physical_only.correctness = correctness_against_baseline(physical_only, baseline)
    else:
        baseline_joined = join_result(original, right, key_column)
        physical_joined = join_result(preprocessed, right, key_column)
        baseline.correctness = join_correctness_against_baseline(
            baseline,
            baseline,
            baseline_joined,
            baseline_joined,
            key_column,
        )
        physical_only.correctness = join_correctness_against_baseline(
            physical_only,
            baseline,
            baseline_joined,
            physical_joined,
            key_column,
        )
    if include_method_aware:
        skip_reason = method_aware_join_skip_reason(
            preprocessed,
            partition_plan,
            key_column=key_column,
            input_reused=input_reused,
        )
        if skip_reason is not None:
            results.append(
                skipped_benchmark_result(
                    spark,
                    preprocessed,
                    workload="join",
                    mode="method_aware",
                    dataset_label="preprocessed",
                    dataset_path=preprocessed_path,
                    skip_reason=skip_reason,
                    extra={"method_aware_join_supported": False},
                )
            )
        else:
            if warmup:
                preprocessed.select(key_column).limit(1).count()
            method_aware = run_method_aware_join(
                spark,
                preprocessed,
                right,
                partition_plan=partition_plan or {},
                key_column=key_column,
                dataset_path=preprocessed_path,
            )
            if correctness_level == "none":
                method_aware.correctness = {}
            elif correctness_level == "basic":
                method_aware.correctness = correctness_against_baseline(method_aware, baseline)
            else:
                method_aware_joined = method_aware_join_result(
                    spark,
                    preprocessed,
                    right,
                    partition_plan=partition_plan or {},
                    key_column=key_column,
                )
                method_aware.correctness = join_correctness_against_baseline(
                    method_aware,
                    baseline,
                    baseline_joined,
                    method_aware_joined,
                    key_column,
                )
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
        grouped = group_by_result(dataframe, key_column)
        aggregate_columns = grouped_aggregate_columns(grouped)
        row = grouped.agg(
            F.count(F.lit(1)).alias("result_rows"),
            F.sum("count").alias("rows"),
            F.max("count").alias("max_group_count"),
            *grouped_sum_aggregations(aggregate_columns),
        ).collect()[0]
        return {
            "rows": int(row["rows"] or 0),
            "result_rows": int(row["result_rows"] or 0),
            "max_group_count": int(row["max_group_count"] or 0),
            "work_checksum": checksum_from_row(row, aggregate_columns),
            "work_columns": aggregate_columns,
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
            "operation": "group_by_count_and_payload_sums",
            "max_group_count": metrics["max_group_count"],
            "work_columns": metrics["work_columns"],
            "work_checksum": metrics["work_checksum"],
        },
    )


def group_by_result(dataframe: DataFrame, key_column: str) -> DataFrame:
    work_columns = numeric_work_columns(dataframe)
    return dataframe.groupBy(key_column).agg(
        F.count(F.lit(1)).alias("count"),
        *sum_aggregations(work_columns),
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
    salt_column: str | None,
    partial_group_keys: list[str] | None = None,
    method_aware_extra: dict | None = None,
) -> BenchmarkResult:
    group_keys = partial_group_keys or default_method_aware_partial_group_keys(
        key_column=key_column,
        partition_column=partition_column,
        salt_column=salt_column,
    )

    def action() -> dict:
        final = method_aware_group_by_result(
            dataframe,
            key_column=key_column,
            partial_group_keys=group_keys,
        )
        aggregate_columns = grouped_aggregate_columns(final)
        row = final.agg(
            F.count(F.lit(1)).alias("result_rows"),
            F.sum("count").alias("rows"),
            F.max("count").alias("max_group_count"),
            *grouped_sum_aggregations(aggregate_columns),
        ).collect()[0]
        return {
            "rows": int(row["rows"] or 0),
            "result_rows": int(row["result_rows"] or 0),
            "max_group_count": int(row["max_group_count"] or 0),
            "work_checksum": checksum_from_row(row, aggregate_columns),
            "work_columns": aggregate_columns,
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
            "operation": "partial_then_final_group_by_count_and_payload_sums",
            "work_columns": metrics["work_columns"],
            "work_checksum": metrics["work_checksum"],
            "partition_column": partition_column,
            "salt_column": salt_column,
            "partial_group_keys": group_keys,
            **(method_aware_extra or {}),
        },
    )


def method_aware_group_by_result(
    dataframe: DataFrame,
    *,
    key_column: str,
    partial_group_keys: list[str],
) -> DataFrame:
    work_columns = numeric_work_columns(dataframe)
    partial = dataframe.groupBy(*partial_group_keys).agg(
        F.count(F.lit(1)).alias("count"),
        *sum_aggregations(work_columns),
    )
    return partial.groupBy(key_column).agg(
        F.sum("count").alias("count"),
        *[F.sum(sum_alias(column)).alias(sum_alias(column)) for column in work_columns],
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
        return collect_join_metrics(join_result(left, right, key_column), key_column)

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
        extra={
            "operation": "join_count_distinct_and_payload_sums",
            "right_partitions": right.rdd.getNumPartitions(),
            "work_columns": metrics["work_columns"],
            "work_checksum": metrics["work_checksum"],
        },
    )


def join_result(left: DataFrame, right: DataFrame, key_column: str) -> DataFrame:
    return left.join(right, on=key_column, how="inner")


def broadcast_join_result(left: DataFrame, right: DataFrame, key_column: str) -> DataFrame:
    return left.join(F.broadcast(right), on=key_column, how="inner")


def run_method_aware_join(
    spark: SparkSession,
    left: DataFrame,
    right: DataFrame,
    *,
    partition_plan: dict,
    key_column: str,
    dataset_path: Path,
) -> BenchmarkResult:
    recommendation = partition_plan.get("recommended_downstream_plan") or {}
    strategy = recommendation.get("strategy") or "generic_join_repartitioning"
    join_plan = partition_plan.get("join_plan") or {}
    technical = partition_plan.get("technical_columns") or {}
    salt_column = technical.get("salt_column")
    heavy_key_column = technical.get("heavy_key_column")

    if strategy == "broadcast_join":
        def action() -> dict:
            joined = broadcast_join_result(left, right, key_column)
            metrics = collect_join_metrics(joined, key_column)
            metrics["right_replication_rows"] = 0
            return metrics

        extra = {
            "strategy": strategy,
            "method_aware_operator_rewrite": True,
            "right_side_size_mb": join_plan.get("right_side_size_mb"),
            "broadcast_threshold_mb": join_plan.get("broadcast_threshold_mb"),
        }
    elif strategy == "physical_repartitioning":
        def action() -> dict:
            joined = join_result(left, right, key_column)
            metrics = collect_join_metrics(joined, key_column)
            metrics["right_replication_rows"] = 0
            return metrics

        extra = {
            "strategy": strategy,
            "method_aware_operator_rewrite": False,
        }
    elif strategy in {"salted_heavy_key_join", "heavy_key_isolation_join"}:
        def action() -> dict:
            joined, right_heavy_replicated = salted_join_result_with_replication(
                spark,
                left,
                right,
                partition_plan=partition_plan,
                key_column=key_column,
            )
            metrics = collect_join_metrics(joined, key_column)
            metrics["right_replication_rows"] = right_heavy_replicated.count()
            return metrics

        extra = {
            "strategy": strategy,
            "heavy_key_count": len(
                heavy_key_literals_for_join(
                    partition_plan,
                    strategy=strategy,
                    key_column=key_column,
                )
            ),
            "heavy_key_side": "shared" if strategy == "salted_heavy_key_join" else "union",
            "salt_column": salt_column,
            "heavy_key_column": heavy_key_column,
            "method_aware_operator_rewrite": True,
        }
    else:
        def action() -> dict:
            joined = join_result(left, right, key_column)
            metrics = collect_join_metrics(joined, key_column)
            metrics["right_replication_rows"] = 0
            return metrics

        extra = {
            "strategy": strategy,
            "method_aware_operator_rewrite": True,
            "degraded_reason": "unsupported_join_strategy",
        }

    elapsed, metrics = timed(action)
    return BenchmarkResult(
        workload="join",
        mode="method_aware",
        dataset_label="preprocessed",
        dataset_path=str(dataset_path),
        elapsed_seconds=elapsed,
        rows=metrics["rows"],
        result_rows=metrics["result_rows"],
        partitions=left.rdd.getNumPartitions(),
        spark_app_id=spark.sparkContext.applicationId,
        correctness={},
        extra={
            **extra,
            "right_partitions": right.rdd.getNumPartitions(),
            "right_replication_rows": metrics["right_replication_rows"],
            "work_columns": metrics["work_columns"],
            "work_checksum": metrics["work_checksum"],
        },
    )


def method_aware_join_result(
    spark: SparkSession,
    left: DataFrame,
    right: DataFrame,
    *,
    partition_plan: dict,
    key_column: str,
) -> DataFrame:
    strategy = (partition_plan.get("recommended_downstream_plan") or {}).get("strategy")
    if strategy == "broadcast_join":
        return broadcast_join_result(left, right, key_column)
    if strategy == "physical_repartitioning":
        return join_result(left, right, key_column)
    if strategy in {"salted_heavy_key_join", "heavy_key_isolation_join"}:
        joined, _ = salted_join_result_with_replication(
            spark,
            left,
            right,
            partition_plan=partition_plan,
            key_column=key_column,
        )
        return joined
    return join_result(left, right, key_column)


def salted_join_result_with_replication(
    spark: SparkSession,
    left: DataFrame,
    right: DataFrame,
    *,
    partition_plan: dict,
    key_column: str,
) -> tuple[DataFrame, DataFrame]:
    strategy = (partition_plan.get("recommended_downstream_plan") or {}).get("strategy")
    technical = partition_plan.get("technical_columns") or {}
    salt_column = technical.get("salt_column")
    heavy_key_column = technical.get("heavy_key_column")
    heavy_key_literals = heavy_key_literals_for_join(
        partition_plan,
        strategy=strategy,
        key_column=key_column,
    )
    heavy_condition = heavy_key_filter_condition(key_column, heavy_key_literals)
    salt_mapping = build_salt_mapping_dataframe(
        spark,
        partition_plan,
        key_column=key_column,
        salt_column=salt_column,
        key_data_type=right.schema[key_column].dataType,
        heavy_key_literals=heavy_key_literals,
    )
    left_heavy = left.where((F.col(heavy_key_column) == F.lit(True)) | heavy_condition)
    left_normal = left.where(~heavy_condition)
    right_heavy = right.where(heavy_condition)
    right_normal = right.join(
        right_heavy.select(key_column).dropDuplicates([key_column]),
        key_column,
        "left_anti",
    )
    right_heavy_replicated = right_heavy.join(salt_mapping, on=key_column, how="inner")
    left_heavy_salted = left_heavy.where(F.col(salt_column).isNotNull())
    left_heavy_unsalted = left_heavy.where(F.col(salt_column).isNull())
    heavy_joined_salted = left_heavy_salted.join(
        right_heavy_replicated,
        on=[key_column, salt_column],
        how="inner",
    )
    heavy_joined_unsalted = left_heavy_unsalted.join(
        right_heavy,
        on=key_column,
        how="inner",
    )
    normal_joined = left_normal.join(right_normal, on=key_column, how="inner")
    result = normal_joined.unionByName(
        heavy_joined_salted,
        allowMissingColumns=True,
    ).unionByName(
        heavy_joined_unsalted,
        allowMissingColumns=True,
    )
    return result, right_heavy_replicated


def collect_join_metrics(joined: DataFrame, key_column: str) -> dict:
    work_columns = numeric_work_columns(joined)
    if "join_payload" in joined.columns:
        work_columns.append("join_payload")
    row = joined.agg(
        F.count(F.lit(1)).alias("rows"),
        F.countDistinct(key_column).alias("result_rows"),
        *sum_aggregations(work_columns),
    ).collect()[0]
    return {
        "rows": int(row["rows"] or 0),
        "result_rows": int(row["result_rows"] or 0),
        "work_checksum": checksum_from_row(row, work_columns),
        "work_columns": work_columns,
    }


def skipped_benchmark_result(
    spark: SparkSession,
    dataframe: DataFrame,
    *,
    workload: str,
    mode: str,
    dataset_label: str,
    dataset_path: Path,
    skip_reason: str,
    extra: dict | None = None,
) -> BenchmarkResult:
    return BenchmarkResult(
        workload=workload,
        mode=mode,
        dataset_label=dataset_label,
        dataset_path=str(dataset_path),
        elapsed_seconds=0.0,
        rows=0,
        result_rows=0,
        partitions=dataframe.rdd.getNumPartitions(),
        spark_app_id=spark.sparkContext.applicationId,
        correctness={},
        extra={**(extra or {}), "skip_reason": skip_reason},
        skipped=True,
        skip_reason=skip_reason,
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


def numeric_work_columns(dataframe: DataFrame) -> list[str]:
    columns = []
    if "value" in dataframe.columns:
        columns.append("value")
    columns.extend(
        sorted(
            [column for column in dataframe.columns if re.fullmatch(r"payload_\d+", column)],
            key=lambda column: int(column.rsplit("_", maxsplit=1)[1]),
        )
    )
    return columns


def sum_aggregations(columns: list[str]) -> list:
    return [
        F.sum(F.col(column).cast("decimal(38, 0)")).alias(sum_alias(column))
        for column in columns
    ]


def grouped_aggregate_columns(dataframe: DataFrame) -> list[str]:
    columns = []
    if "value_sum" in dataframe.columns:
        columns.append("value_sum")
    columns.extend(
        sorted(
            [
                column
                for column in dataframe.columns
                if re.fullmatch(r"payload_\d+_sum", column)
            ],
            key=lambda column: int(column.split("_")[1]),
        )
    )
    return columns


def numeric_work_columns_from_names(columns: Iterable[str]) -> list[str]:
    selected = []
    if "value" in columns:
        selected.append("value")
    selected.extend(
        sorted(
            [column for column in columns if re.fullmatch(r"payload_\d+", column)],
            key=lambda column: int(column.rsplit("_", maxsplit=1)[1]),
        )
    )
    return selected


def grouped_sum_aggregations(columns: list[str]) -> list:
    return [
        F.sum(F.col(column).cast("decimal(38, 0)")).alias(total_alias(column))
        for column in columns
    ]


def checksum_from_row(row, columns: list[str]) -> int:
    values = row.asDict()
    total = 0
    for column in columns:
        value = (
            values.get(sum_alias(column))
            if not column.endswith("_sum")
            else values.get(total_alias(column))
        )
        total += int(value or 0)
    return total


def sum_alias(column: str) -> str:
    return f"{column}_sum"


def total_alias(column: str) -> str:
    return f"{column}_total"


def timed(action: Callable[[], dict]) -> tuple[float, dict]:
    started = time.perf_counter()
    result = action()
    return time.perf_counter() - started, result


def correctness_against_baseline(result: BenchmarkResult, baseline: BenchmarkResult) -> dict:
    return {
        "row_count_matches_baseline": result.rows == baseline.rows,
        "result_rows_match_baseline": result.result_rows == baseline.result_rows,
    }


def group_by_correctness_against_baseline(
    result: BenchmarkResult,
    baseline: BenchmarkResult,
    baseline_grouped: DataFrame,
    candidate_grouped: DataFrame,
    key_column: str,
) -> dict:
    correctness = correctness_against_baseline(result, baseline)
    correctness.update(
        compare_group_by_results(
            baseline_grouped,
            candidate_grouped,
            key_column,
        )
    )
    correctness.update(
        checksum_comparison(
            baseline_grouped,
            candidate_grouped,
            checksum_columns=comparable_group_by_checksum_columns(
                baseline_grouped,
                candidate_grouped,
                key_column,
            ),
        )
    )
    return correctness


def compare_group_by_results(
    baseline: DataFrame,
    candidate: DataFrame,
    key_column: str,
) -> dict:
    baseline_counts = baseline.select(
        F.col(key_column).alias("baseline_key"),
        F.col("count").alias("baseline_count"),
    )
    candidate_counts = candidate.select(
        F.col(key_column).alias("candidate_key"),
        F.col("count").alias("candidate_count"),
    )
    diff = baseline_counts.join(
        candidate_counts,
        F.col("baseline_key").eqNullSafe(F.col("candidate_key")),
        "full_outer",
    ).where(
        F.coalesce(F.col("baseline_count"), F.lit(-1))
        != F.coalesce(F.col("candidate_count"), F.lit(-1))
    )
    diff_count = diff.count()
    return {
        "exact_group_counts_match": diff_count == 0,
        "group_count_diff_rows": diff_count,
    }


def join_correctness_against_baseline(
    result: BenchmarkResult,
    baseline: BenchmarkResult,
    baseline_joined: DataFrame,
    candidate_joined: DataFrame,
    key_column: str,
) -> dict:
    correctness = correctness_against_baseline(result, baseline)
    checksum_columns = comparable_join_checksum_columns(
        baseline_joined,
        candidate_joined,
        key_column,
    )
    correctness.update(
        checksum_comparison(
            baseline_joined,
            candidate_joined,
            checksum_columns=checksum_columns,
        )
    )
    correctness["checksum_columns"] = checksum_columns
    return correctness


def checksum_comparison(
    baseline: DataFrame,
    candidate: DataFrame,
    *,
    checksum_columns: list[str],
) -> dict:
    baseline_checksum = dataframe_checksum(baseline, checksum_columns)
    candidate_checksum = dataframe_checksum(candidate, checksum_columns)
    return {
        "checksum_matches_baseline": baseline_checksum == candidate_checksum,
        "baseline_checksum": baseline_checksum,
        "candidate_checksum": candidate_checksum,
    }


def dataframe_checksum(dataframe: DataFrame, columns: list[str]) -> int:
    selected_columns = [F.col(column).cast("string") for column in columns]
    row = dataframe.select(F.xxhash64(*selected_columns).alias("h")).agg(
        F.sum(F.col("h").cast("decimal(38, 0)")).alias("checksum")
    ).collect()[0]
    return int(row["checksum"] or 0)


def comparable_join_checksum_columns(
    baseline: DataFrame,
    candidate: DataFrame,
    key_column: str,
) -> list[str]:
    return comparable_join_checksum_column_names(
        baseline.columns,
        candidate.columns,
        key_column,
    )


def comparable_group_by_checksum_columns(
    baseline: DataFrame,
    candidate: DataFrame,
    key_column: str,
) -> list[str]:
    baseline_columns = set(baseline.columns)
    candidate_columns = set(candidate.columns)
    columns = [key_column, "count"]
    columns.extend(
        sorted(
            column
            for column in baseline_columns & candidate_columns
            if column == "value_sum" or re.fullmatch(r"payload_\d+_sum", column)
        )
    )
    return [column for column in columns if column in baseline_columns and column in candidate_columns]


def comparable_join_checksum_column_names(
    baseline_columns: Iterable[str],
    candidate_columns: Iterable[str],
    key_column: str,
) -> list[str]:
    baseline_columns = set(logical_result_columns(baseline_columns))
    candidate_columns = set(logical_result_columns(candidate_columns))
    columns = sorted(baseline_columns & candidate_columns)
    if key_column in columns:
        columns.remove(key_column)
        columns.insert(0, key_column)
    return columns or [key_column]


def logical_result_columns(columns: Iterable[str]) -> list[str]:
    return [
        column
        for column in unique_preserving_order(columns)
        if not is_technical_result_column(column)
    ]


def is_technical_result_column(column: str) -> bool:
    return column.startswith("_rp_") or column.startswith("_ap_") or column in {
        "rp_partition",
        "ap_partition",
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


def method_aware_join_skip_reason(
    left: DataFrame,
    partition_plan: dict | None,
    *,
    key_column: str,
    input_reused: bool,
) -> str | None:
    if partition_plan is None:
        return "partition_plan_missing"
    if input_reused:
        return "input_reused"
    if partition_plan.get("job_type") != "join":
        return "partition_plan_job_type_not_join"

    recommendation = partition_plan.get("recommended_downstream_plan") or {}
    join_keys = recommendation.get("join_keys") or partition_plan.get("key_columns") or []
    if len(join_keys) != 1:
        return "composite_join_key_unsupported"
    if join_keys[0] != key_column:
        return "join_key_mismatch"

    technical = partition_plan.get("technical_columns") or {}
    if not technical.get("included", False):
        return "missing_technical_columns"
    technical_columns = [
        technical.get("partition_column"),
        technical.get("salt_column"),
        technical.get("heavy_key_column"),
    ]
    if any(not column or column not in left.columns for column in technical_columns):
        return "missing_technical_columns"

    strategy = recommendation.get("strategy")
    if strategy not in {
        "broadcast_join",
        "physical_repartitioning",
        "salted_heavy_key_join",
        "heavy_key_isolation_join",
    }:
        return "unsupported_join_strategy"

    try:
        if strategy == "salted_heavy_key_join":
            heavy_key_literals_for_join(
                partition_plan,
                strategy=strategy,
                key_column=key_column,
            )
        elif strategy == "heavy_key_isolation_join":
            heavy_key_literals_for_join(
                partition_plan,
                strategy=strategy,
                key_column=key_column,
            )
    except ValueError as exc:
        if "single-column heavy keys only" in str(exc):
            return "composite_join_key_unsupported"
        return "invalid_structured_heavy_keys"

    return None


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


def resolve_method_aware_salt_column(
    preprocessed: DataFrame,
    partition_plan: dict | None,
) -> str | None:
    candidates: list[str] = []
    if partition_plan is not None:
        recommended = partition_plan.get("recommended_downstream_plan") or {}
        technical_columns = partition_plan.get("technical_columns") or {}
        salt_column = technical_columns.get("salt_column")
        if salt_column:
            candidates.append(salt_column)
        for column in recommended.get("partial_group_keys") or []:
            if column.endswith("_salt") or column in {"_rp_salt", "_ap_salt"}:
                candidates.append(column)

    candidates.extend(["_rp_salt", "_ap_salt"])
    for column in unique_preserving_order(candidates):
        if column in preprocessed.columns:
            return column

    return None


def resolve_method_aware_partial_group_keys(
    preprocessed: DataFrame,
    partition_plan: dict | None,
    *,
    key_column: str,
    partition_column: str,
    salt_column: str | None,
) -> tuple[list[str], dict]:
    extra = {
        "salt_column_used": False,
        "method_aware_degraded": False,
        "degraded_reason": None,
    }
    available_columns = set(preprocessed.columns)
    recommended_keys = recommended_partial_group_keys(partition_plan)

    if recommended_keys:
        missing = [column for column in recommended_keys if column not in available_columns]
        allowed_missing = {column for column in missing if is_salt_column(column, partition_plan)}
        unexpected_missing = [column for column in missing if column not in allowed_missing]
        if unexpected_missing:
            raise ValueError(
                "method-aware groupBy partial_group_keys are missing from DataFrame: "
                f"{unexpected_missing}; available columns: {preprocessed.columns}"
            )

        partial_keys = [column for column in recommended_keys if column in available_columns]
        if salt_column is not None and salt_column not in partial_keys:
            insert_at = 1 if partition_column in partial_keys else 0
            partial_keys.insert(insert_at, salt_column)
        if partition_column not in partial_keys:
            partial_keys.insert(0, partition_column)
        if key_column not in partial_keys:
            partial_keys.append(key_column)
    else:
        partial_keys = default_method_aware_partial_group_keys(
            key_column=key_column,
            partition_column=partition_column,
            salt_column=salt_column,
        )

    if salt_column is not None and salt_column in partial_keys:
        extra["salt_column_used"] = True
    else:
        extra["method_aware_degraded"] = True
        extra["degraded_reason"] = "salt_column_missing"

    return unique_preserving_order(partial_keys), extra


def recommended_partial_group_keys(partition_plan: dict | None) -> list[str]:
    if partition_plan is None:
        return []
    recommended = partition_plan.get("recommended_downstream_plan") or {}
    return list(recommended.get("partial_group_keys") or [])


def default_method_aware_partial_group_keys(
    *,
    key_column: str,
    partition_column: str,
    salt_column: str | None,
) -> list[str]:
    partial_keys = [partition_column]
    if salt_column is not None:
        partial_keys.append(salt_column)
    partial_keys.append(key_column)
    return partial_keys


def is_salt_column(column: str, partition_plan: dict | None) -> bool:
    if partition_plan is not None:
        salt_column = (partition_plan.get("technical_columns") or {}).get("salt_column")
        if column == salt_column:
            return True
    return column.endswith("_salt") or column in {"_rp_salt", "_ap_salt"}


def single_column_heavy_key_literals(
    partition_plan: dict | None,
    *,
    side: str,
    key_column: str,
) -> list[dict]:
    if partition_plan is None:
        return []

    join_plan = partition_plan.get("join_plan") or {}
    field_name = f"{side}_heavy_key_values"
    plan_keys = join_plan.get(field_name) or []
    literals = []
    for plan_key in plan_keys:
        parts = plan_key.get("parts") or []
        if len(parts) != 1:
            raise ValueError(
                "method-aware join currently supports single-column heavy keys only; "
                f"got {len(parts)} parts for encoded key {plan_key.get('encoded')}"
            )
        part = parts[0]
        if part.get("column") != key_column:
            raise ValueError(
                "method-aware join heavy key column does not match benchmark key column; "
                f"expected {key_column}, got {part.get('column')}"
            )
        literals.append(
            {
                "column": part.get("column"),
                "value_type": part.get("value_type"),
                "value": part.get("value"),
            }
        )

    return literals


def heavy_key_literals_for_join(
    partition_plan: dict,
    *,
    strategy: str,
    key_column: str,
) -> list[dict]:
    join_plan = partition_plan.get("join_plan") or {}
    if strategy == "salted_heavy_key_join":
        plan_keys = join_plan.get("shared_heavy_key_values") or []
    elif strategy == "heavy_key_isolation_join":
        plan_keys = unique_plan_keys(
            list(join_plan.get("left_heavy_key_values") or [])
            + list(join_plan.get("right_heavy_key_values") or [])
        )
    else:
        plan_keys = []

    literals = []
    for plan_key in plan_keys:
        parts = plan_key.get("parts") or []
        if len(parts) != 1:
            raise ValueError(
                "method-aware join currently supports single-column heavy keys only; "
                f"got {len(parts)} parts for encoded key {plan_key.get('encoded')}"
            )
        part = parts[0]
        if part.get("column") != key_column:
            raise ValueError(
                "method-aware join heavy key column does not match benchmark key column; "
                f"expected {key_column}, got {part.get('column')}"
            )
        literals.append(
            {
                "encoded": plan_key.get("encoded"),
                "column": part.get("column"),
                "value_type": part.get("value_type"),
                "value": part.get("value"),
            }
        )

    return literals


def unique_plan_keys(plan_keys: Iterable[dict]) -> list[dict]:
    unique = []
    seen = set()
    for plan_key in plan_keys:
        encoded = plan_key.get("encoded")
        if encoded not in seen:
            unique.append(plan_key)
            seen.add(encoded)
    return unique


def heavy_key_filter_condition(key_column: str, heavy_key_literals: list[dict]):
    condition = F.lit(False)
    for heavy_key in heavy_key_literals:
        value_type = heavy_key.get("value_type")
        value = spark_literal_value(value_type, heavy_key.get("value"))
        if value_type == "null":
            next_condition = F.col(key_column).isNull()
        else:
            next_condition = F.col(key_column) == F.lit(value)
        condition = condition | next_condition
    return condition


def build_salt_mapping_dataframe(
    spark: SparkSession,
    partition_plan: dict,
    *,
    key_column: str,
    salt_column: str,
    key_data_type: T.DataType,
    heavy_key_literals: list[dict],
) -> DataFrame:
    salt_counts = salt_counts_by_encoded_key(partition_plan)
    rows = []
    for heavy_key in heavy_key_literals:
        encoded = heavy_key.get("encoded")
        salt_count = salt_counts.get(encoded, 1)
        value = spark_literal_value(heavy_key.get("value_type"), heavy_key.get("value"))
        for salt_index in range(salt_count):
            rows.append((value, salt_index))

    schema = T.StructType(
        [
            T.StructField(key_column, key_data_type, True),
            T.StructField(salt_column, T.IntegerType(), False),
        ]
    )
    return spark.createDataFrame(rows, schema)


def salt_counts_by_encoded_key(partition_plan: dict) -> dict[str, int]:
    counts = {}
    for heavy_key in partition_plan.get("heavy_keys") or []:
        encoded = heavy_key.get("key")
        structured = heavy_key.get("structured_key") or {}
        encoded = structured.get("encoded") or encoded
        if encoded:
            counts[encoded] = int(heavy_key.get("salt_count") or 1)
    return counts


def spark_literal_value(value_type: str | None, value: str | None):
    if value_type == "null":
        return None
    if value_type in {"int64", "uint64", "date32", "timestamp_micros"}:
        return int(value)
    if value_type == "boolean":
        return str(value).lower() == "true"
    return value


def unique_preserving_order(values: Iterable[str]) -> list[str]:
    unique: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value not in seen:
            unique.append(value)
            seen.add(value)
    return unique


def write_reports(results: Iterable[BenchmarkResult], json_report: Path, csv_report: Path) -> None:
    rows = [asdict(result) for result in results]
    json_report.parent.mkdir(parents=True, exist_ok=True)
    csv_report.parent.mkdir(parents=True, exist_ok=True)
    json_report.write_text(
        json.dumps({"summary": benchmark_summary(rows), "results": rows}, indent=2),
        encoding="utf-8",
    )

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
                "skipped",
                "skip_reason",
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
                    "skipped": row["skipped"],
                    "skip_reason": row["skip_reason"],
                    "correctness_json": json.dumps(row["correctness"], sort_keys=True),
                    "extra_json": json.dumps(row["extra"], sort_keys=True),
                }
            )


def benchmark_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    scan = results_by_workload_and_mode(rows, "scan")
    filter_results = results_by_workload_and_mode(rows, "filter")
    group_by = results_by_workload_and_mode(rows, "group_by")
    join = results_by_workload_and_mode(rows, "join")
    active = scan or filter_results or group_by or join

    active_method_aware = active.get("method_aware")
    join_method_aware = join.get("method_aware")
    group_by_method_aware = group_by.get("method_aware")

    return {
        "spark_baseline_seconds": elapsed_seconds(active.get("baseline")),
        "spark_physical_only_seconds": elapsed_seconds(active.get("physical_only")),
        "spark_method_aware_seconds": elapsed_seconds(active_method_aware),
        "spark_method_aware_join_seconds": elapsed_seconds(join_method_aware),
        "group_by_exact_correctness": correctness_value(
            group_by_method_aware,
            "exact_group_counts_match",
        ),
        "join_checksum_correctness": correctness_value(
            join_method_aware,
            "checksum_matches_baseline",
        ),
        "method_aware_join_applied": bool(join_method_aware and not join_method_aware.get("skipped")),
        "method_aware_join_skipped": join_method_aware.get("skipped") if join_method_aware else None,
        "method_aware_join_skip_reason": join_method_aware.get("skip_reason") if join_method_aware else None,
        "method_aware_join_strategy": ((join_method_aware or {}).get("extra") or {}).get("strategy"),
    }


def results_by_workload_and_mode(rows: list[dict[str, Any]], workload: str) -> dict[str, dict[str, Any]]:
    return {
        row["mode"]: row
        for row in rows
        if row.get("workload") == workload
    }


def elapsed_seconds(row: dict[str, Any] | None) -> float | None:
    if row is None or row.get("skipped"):
        return None
    return row.get("elapsed_seconds")


def correctness_value(row: dict[str, Any] | None, key: str) -> bool | None:
    if row is None or row.get("skipped"):
        return None
    return (row.get("correctness") or {}).get(key)


if __name__ == "__main__":
    main()
