#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from pyspark.sql import SparkSession

from benchmark import bool_string, run_with_spark, verify_java_runtime


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run multiple Spark benchmarks inside one SparkSession."
    )
    parser.add_argument("--tasks", required=True, type=Path, help="JSON file with benchmark tasks.")
    parser.add_argument("--app-name", default="repartitioner-benchmark-suite")
    parser.add_argument("--shuffle-partitions", type=int, default=200)
    parser.add_argument("--driver-memory", default="8g")
    parser.add_argument("--executor-memory", default="8g")
    parser.add_argument("--auto-broadcast-threshold-bytes", type=int, default=-1)
    parser.add_argument("--enable-aqe", action="store_true")
    parser.add_argument("--parquet-batch-size", type=int, default=1024)
    parser.add_argument("--enable-vectorized-parquet", action="store_true")
    parser.add_argument("--enable-vectored-io", action="store_true")
    args = parser.parse_args()

    verify_java_runtime()
    tasks = read_tasks(args.tasks)
    if not tasks:
        print("No Spark benchmark tasks to run.")
        return
    validate_tasks(tasks)

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
        for index, task in enumerate(tasks, start=1):
            print(
                f"[{index}/{len(tasks)}] Spark workload={task.workload} "
                f"original={task.original} preprocessed={task.preprocessed}",
                flush=True,
            )
            run_with_spark(spark, task)
    finally:
        try:
            spark.stop()
        except Exception:
            pass


def read_tasks(path: Path) -> list[argparse.Namespace]:
    data = json.loads(path.read_text(encoding="utf-8"))
    raw_tasks = data["tasks"] if isinstance(data, dict) else data
    return [task_from_dict(item) for item in raw_tasks]


def task_from_dict(item: dict) -> argparse.Namespace:
    return argparse.Namespace(
        workload=item["workload"],
        original=Path(item["original"]),
        preprocessed=Path(item["preprocessed"]),
        join_right=Path(item["join_right"]) if item.get("join_right") else None,
        json_report=Path(item["json_report"]),
        csv_report=Path(item["csv_report"]),
        key_column=item.get("key_column", "user_id"),
        app_name=item.get("app_name", "repartitioner-benchmark-suite"),
        shuffle_partitions=int(item.get("shuffle_partitions", 200)),
        warmup=bool(item.get("warmup", False)),
        include_method_aware=bool(item.get("include_method_aware", False)),
        correctness_level=item.get("correctness_level", "basic"),
        driver_memory=item.get("driver_memory", "8g"),
        executor_memory=item.get("executor_memory", "8g"),
        auto_broadcast_threshold_bytes=int(item.get("auto_broadcast_threshold_bytes", -1)),
        enable_aqe=bool(item.get("enable_aqe", False)),
        parquet_batch_size=int(item.get("parquet_batch_size", 1024)),
        enable_vectorized_parquet=bool(item.get("enable_vectorized_parquet", False)),
        enable_vectored_io=bool(item.get("enable_vectored_io", False)),
    )


def validate_tasks(tasks: list[argparse.Namespace]) -> None:
    missing = []
    for task in tasks:
        for label, path in [
            ("original", task.original),
            ("preprocessed", task.preprocessed),
            ("join_right", task.join_right),
        ]:
            if path is not None and not path.exists():
                missing.append(f"{label}: {path}")

    if missing:
        preview = "\n".join(missing[:20])
        suffix = "" if len(missing) <= 20 else f"\n... and {len(missing) - 20} more"
        raise SystemExit(
            "Spark benchmark task paths are missing. Re-run preprocessing without "
            "--skip-existing, or use the updated runner so stale reports are not "
            f"trusted.\n{preview}{suffix}"
        )


if __name__ == "__main__":
    main()
