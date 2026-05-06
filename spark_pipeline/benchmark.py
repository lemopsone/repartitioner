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
                        warmup=args.warmup,
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
        method_aware.correctness = correctness_against_baseline(method_aware, baseline)
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
    baseline.correctness = correctness_against_baseline(baseline, baseline)

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
    physical_only.correctness = correctness_against_baseline(physical_only, baseline)

    results = [baseline, physical_only]
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
        partial = dataframe.groupBy(*group_keys).count()
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
            "salt_column": salt_column,
            "partial_group_keys": group_keys,
            **(method_aware_extra or {}),
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
            joined = left.join(F.broadcast(right), on=key_column, how="inner")
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
            joined = left.join(right, on=key_column, how="inner")
            metrics = collect_join_metrics(joined, key_column)
            metrics["right_replication_rows"] = 0
            return metrics

        extra = {
            "strategy": strategy,
            "method_aware_operator_rewrite": False,
        }
    elif strategy in {"salted_heavy_key_join", "heavy_key_isolation_join"}:
        heavy_side = "shared" if strategy == "salted_heavy_key_join" else "union"
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

        def action() -> dict:
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
            metrics = collect_join_metrics(result, key_column)
            metrics["right_replication_rows"] = right_heavy_replicated.count()
            return metrics

        extra = {
            "strategy": strategy,
            "heavy_key_count": len(heavy_key_literals),
            "heavy_key_side": heavy_side,
            "salt_column": salt_column,
            "heavy_key_column": heavy_key_column,
            "method_aware_operator_rewrite": True,
        }
    else:
        def action() -> dict:
            joined = left.join(right, on=key_column, how="inner")
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
        },
    )


def collect_join_metrics(joined: DataFrame, key_column: str) -> dict:
    row = joined.agg(
        F.count(F.lit(1)).alias("rows"),
        F.countDistinct(key_column).alias("result_rows"),
    ).collect()[0]
    return {
        "rows": int(row["rows"] or 0),
        "result_rows": int(row["result_rows"] or 0),
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


if __name__ == "__main__":
    main()
