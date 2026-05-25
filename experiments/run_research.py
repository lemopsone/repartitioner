#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import sys
from pathlib import Path


DEFAULT_SKEWS = ["uniform", "heavy_key", "multi_heavy_key", "zipf"]
DEFAULT_WORKLOADS = ["scan", "filter", "group_by", "join"]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the full repartitioner research matrix and write a summary CSV."
    )
    parser.add_argument("--data-dir", type=Path, default=Path("data/research"))
    parser.add_argument("--reports-dir", type=Path, default=Path("reports/research"))
    parser.add_argument("--rows", help="Comma-separated row counts. Defaults to diploma range.")
    parser.add_argument("--skews", nargs="+", choices=DEFAULT_SKEWS, default=DEFAULT_SKEWS)
    parser.add_argument(
        "--workloads",
        nargs="+",
        choices=DEFAULT_WORKLOADS,
        default=DEFAULT_WORKLOADS,
    )
    parser.add_argument("--key-cardinality", type=int, default=10_000)
    parser.add_argument("--part-rows", type=int, default=1_000_000)
    parser.add_argument("--payload-columns", type=int, default=8)
    parser.add_argument("--heavy-fraction", type=float, default=0.50)
    parser.add_argument("--multi-heavy-keys", type=int, default=5)
    parser.add_argument("--multi-heavy-fraction", type=float, default=0.60)
    parser.add_argument("--zipf-exponent", type=float, default=1.2)
    parser.add_argument("--min-partitions", type=int, default=1)
    parser.add_argument("--max-partitions", type=int, default=16)
    parser.add_argument("--local-threads", type=int, default=os.cpu_count() or 1)
    parser.add_argument("--target-partition-size-mb", type=int, default=128)
    parser.add_argument("--shuffle-partitions", type=int, default=16)
    parser.add_argument("--spark-driver-memory", default="8g")
    parser.add_argument("--spark-executor-memory", default="8g")
    parser.add_argument("--auto-broadcast-threshold-bytes", type=int, default=-1)
    parser.add_argument("--enable-aqe", action="store_true")
    parser.add_argument("--parquet-batch-size", type=int, default=1024)
    parser.add_argument("--enable-vectorized-parquet", action="store_true")
    parser.add_argument("--enable-vectored-io", action="store_true")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--dataset-repetitions",
        type=int,
        default=1,
        help="Number of independently generated datasets per configuration.",
    )
    parser.add_argument(
        "--spark-repetitions",
        "--repetitions",
        dest="spark_repetitions",
        type=int,
        default=1,
        help="Number of Spark repetitions per generated dataset.",
    )
    parser.add_argument(
        "--trim-fraction",
        type=float,
        default=0.0,
        help="Fraction to trim from each tail before averaging Spark timings, e.g. 0.2.",
    )
    parser.add_argument(
        "--spark-mode",
        choices=["suite", "per_process"],
        default="suite",
        help=(
            "suite keeps one SparkSession for all Spark measurements; per_process "
            "starts Spark separately for every measurement."
        ),
    )
    parser.add_argument(
        "--correctness-level",
        choices=["none", "basic", "full"],
        default="basic",
        help="Spark correctness checks. Use full only on small smoke runs.",
    )
    parser.add_argument(
        "--plots-dir",
        type=Path,
        help="Directory for generated PNG plots. Defaults to <reports-dir>/plots.",
    )
    parser.add_argument("--no-plots", action="store_true", help="Do not generate PNG plots.")
    parser.add_argument("--release", action="store_true")
    parser.add_argument("--force-rewrite", action="store_true")
    parser.add_argument("--skip-existing", action="store_true")
    args = parser.parse_args()
    if args.dataset_repetitions <= 0:
        raise SystemExit("--dataset-repetitions must be greater than zero")
    if args.spark_repetitions <= 0:
        raise SystemExit("--spark-repetitions must be greater than zero")
    if not 0.0 <= args.trim_fraction < 0.5:
        raise SystemExit("--trim-fraction must be in [0.0, 0.5)")

    rows = parse_rows(args.rows)
    summary_path = args.reports_dir / "summary.csv"
    raw_summary_path = args.reports_dir / "summary_raw.csv"
    raw_summary_rows: list[dict] = []
    spark_tasks: list[dict] = []
    report_specs: list[dict] = []

    for skew in args.skews:
        for row_count in rows:
            for dataset_repetition in range(1, args.dataset_repetitions + 1):
                dataset_seed = args.seed + dataset_repetition - 1
                dataset_prefix = f"{skew}_{row_count}_ds{dataset_repetition:02}"
                dataset_path = args.data_dir / f"{dataset_prefix}.parquet"
                dimension_path = args.data_dir / f"{dataset_prefix}_dimension.parquet"
                generate_dataset(args, skew, row_count, dataset_path, dataset_seed)
                generate_dimension(dataset_path, dimension_path)

                for workload in args.workloads:
                    preprocessed_path = args.data_dir / f"{dataset_prefix}_{workload}_partitioned"
                    preprocess_result_path = (
                        args.reports_dir
                        / "preprocess"
                        / f"{dataset_prefix}_{workload}.json"
                    )
                    run_preprocessor(
                        args,
                        workload,
                        dataset_path,
                        preprocessed_path,
                        preprocess_result_path,
                        dimension_path if workload == "join" else None,
                    )

                    for spark_repetition in range(1, args.spark_repetitions + 1):
                        spark_json = (
                            args.reports_dir
                            / "spark"
                            / f"{dataset_prefix}_{workload}_spark{spark_repetition:02}.json"
                        )
                        spark_csv = (
                            args.reports_dir
                            / "spark"
                            / f"{dataset_prefix}_{workload}_spark{spark_repetition:02}.csv"
                        )
                        join_right = dimension_path if workload == "join" else None
                        if args.spark_mode == "suite":
                            if should_run_spark(args, spark_json, spark_csv):
                                spark_tasks.append(
                                    spark_task(
                                        args,
                                        workload,
                                        dataset_path,
                                        preprocessed_path,
                                        spark_json,
                                        spark_csv,
                                        join_right,
                                    )
                                )
                            report_specs.append(
                                {
                                    "skew": skew,
                                    "row_count": row_count,
                                    "workload": workload,
                                    "dataset_repetition": dataset_repetition,
                                    "dataset_seed": dataset_seed,
                                    "spark_repetition": spark_repetition,
                                    "preprocess_result_path": preprocess_result_path,
                                    "spark_report_path": spark_json,
                                }
                            )
                        else:
                            run_spark(
                                args,
                                workload,
                                dataset_path,
                                preprocessed_path,
                                spark_json,
                                spark_csv,
                                join_right,
                            )

                            raw_summary_rows.extend(
                                rows_from_reports(
                                    skew=skew,
                                    row_count=row_count,
                                    workload=workload,
                                    dataset_repetition=dataset_repetition,
                                    dataset_seed=dataset_seed,
                                    spark_repetition=spark_repetition,
                                    preprocess_result_path=preprocess_result_path,
                                    spark_report_path=spark_json,
                                )
                            )

                    if args.spark_mode == "per_process":
                        write_summary(raw_summary_path, raw_summary_rows, raw=True)
                        write_summary(
                            summary_path,
                            aggregate_summary_rows(raw_summary_rows, args.trim_fraction),
                            raw=False,
                        )

    if args.spark_mode == "suite":
        run_spark_suite(args, spark_tasks)
        for spec in report_specs:
            raw_summary_rows.extend(rows_from_reports(**spec))
        write_summary(raw_summary_path, raw_summary_rows, raw=True)
        write_summary(
            summary_path,
            aggregate_summary_rows(raw_summary_rows, args.trim_fraction),
            raw=False,
        )
    if not args.no_plots and summary_path.exists():
        run_plots(args, summary_path)

    print(f"Wrote {summary_path}")


def parse_rows(rows: str | None) -> list[int]:
    if rows:
        return [int(value) for value in rows.split(",") if value.strip()]
    return [1_000_000, *range(5_000_000, 25_000_001, 5_000_000)]


def generate_dataset(
    args: argparse.Namespace,
    skew: str,
    rows: int,
    output: Path,
    seed: int,
) -> None:
    if args.skip_existing and output.exists():
        return

    command = [sys.executable, str(repo_root() / "experiments" / generator_script(skew))]
    command.extend(
        [
            "--output",
            str(output),
            "--rows",
            str(rows),
            "--seed",
            str(seed),
            "--part-rows",
            str(args.part_rows),
            "--key-cardinality",
            str(args.key_cardinality),
            "--payload-columns",
            str(args.payload_columns),
        ]
    )
    if skew == "heavy_key":
        command.extend(["--heavy-fraction", str(args.heavy_fraction)])
    elif skew == "multi_heavy_key":
        command.extend(
            [
                "--heavy-keys",
                str(args.multi_heavy_keys),
                "--heavy-fraction",
                str(args.multi_heavy_fraction),
            ]
        )
    elif skew == "zipf":
        command.extend(["--zipf-exponent", str(args.zipf_exponent)])

    run(command)


def generator_script(skew: str) -> str:
    return {
        "uniform": "generate_uniform.py",
        "heavy_key": "generate_heavy_key.py",
        "multi_heavy_key": "generate_multi_heavy_key.py",
        "zipf": "generate_zipf.py",
    }[skew]


def generate_dimension(input_path: Path, output_path: Path) -> None:
    if output_path.exists():
        return
    run(
        [
            sys.executable,
            str(repo_root() / "experiments" / "generate_dimension.py"),
            "--input",
            str(input_path),
            "--output",
            str(output_path),
        ]
    )


def run_preprocessor(
    args: argparse.Namespace,
    workload: str,
    input_path: Path,
    output_path: Path,
    result_path: Path,
    join_right: Path | None,
) -> None:
    if args.skip_existing and preprocessor_outputs_exist(workload, result_path, output_path):
        return

    command = [
        sys.executable,
        str(repo_root() / "experiments" / "run_preprocessor.py"),
        "--input",
        str(input_path),
        "--output",
        str(output_path),
        "--result",
        str(result_path),
        "--job-type",
        workload,
        "--max-partitions",
        str(args.max_partitions),
        "--local-threads",
        str(args.local_threads),
        "--min-partitions",
        str(args.min_partitions),
        "--target-partition-size-mb",
        str(args.target_partition_size_mb),
        "--seed",
        str(args.seed),
        "--input-metadata",
        str(input_path.with_suffix(input_path.suffix + ".json")),
    ]
    if args.release:
        command.append("--release")
    if args.force_rewrite:
        command.append("--force-rewrite")
    if join_right is not None:
        command.extend(["--join-right", str(join_right)])

    run(command)


def preprocessor_outputs_exist(workload: str, result_path: Path, output_path: Path) -> bool:
    if not result_path.exists():
        return False

    metadata_paths = [
        output_path / "_partition_plan.json",
        output_path / "_stats.json",
        output_path / "_manifest.json",
    ]
    if not output_path.exists() or not all(path.exists() for path in metadata_paths):
        return False

    if workload in {"scan", "filter"}:
        try:
            plan = json.loads((output_path / "_partition_plan.json").read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return False
        return (
            plan.get("rewrite_required") is False
            and plan.get("recommended_downstream_plan", {}).get("strategy") == "input_reuse_no_op"
        )

    return True


def run_spark(
    args: argparse.Namespace,
    workload: str,
    original_path: Path,
    preprocessed_path: Path,
    json_report: Path,
    csv_report: Path,
    join_right: Path | None,
) -> None:
    if not should_run_spark(args, json_report, csv_report):
        return

    command = [
        sys.executable,
        str(repo_root() / "spark_pipeline" / "benchmark.py"),
        "--workload",
        workload,
        "--original",
        str(original_path),
        "--preprocessed",
        str(preprocessed_path),
        "--json-report",
        str(json_report),
        "--csv-report",
        str(csv_report),
        "--shuffle-partitions",
        str(args.shuffle_partitions),
        "--driver-memory",
        args.spark_driver_memory,
        "--executor-memory",
        args.spark_executor_memory,
        "--correctness-level",
        args.correctness_level,
        "--auto-broadcast-threshold-bytes",
        str(args.auto_broadcast_threshold_bytes),
        "--parquet-batch-size",
        str(args.parquet_batch_size),
    ]
    if args.enable_aqe:
        command.append("--enable-aqe")
    if args.enable_vectorized_parquet:
        command.append("--enable-vectorized-parquet")
    if args.enable_vectored_io:
        command.append("--enable-vectored-io")
    if join_right is not None:
        command.extend(["--join-right", str(join_right)])

    run(command)


def should_run_spark(args: argparse.Namespace, json_report: Path, csv_report: Path) -> bool:
    return not (args.skip_existing and json_report.exists() and csv_report.exists())


def spark_task(
    args: argparse.Namespace,
    workload: str,
    original_path: Path,
    preprocessed_path: Path,
    json_report: Path,
    csv_report: Path,
    join_right: Path | None,
) -> dict:
    return {
        "workload": workload,
        "original": str(original_path),
        "preprocessed": str(preprocessed_path),
        "json_report": str(json_report),
        "csv_report": str(csv_report),
        "join_right": str(join_right) if join_right is not None else None,
        "shuffle_partitions": args.shuffle_partitions,
        "correctness_level": args.correctness_level,
        "driver_memory": args.spark_driver_memory,
        "executor_memory": args.spark_executor_memory,
        "auto_broadcast_threshold_bytes": args.auto_broadcast_threshold_bytes,
        "enable_aqe": args.enable_aqe,
        "parquet_batch_size": args.parquet_batch_size,
        "enable_vectorized_parquet": args.enable_vectorized_parquet,
        "enable_vectored_io": args.enable_vectored_io,
    }


def run_spark_suite(args: argparse.Namespace, tasks: list[dict]) -> None:
    if not tasks:
        return

    tasks_path = args.reports_dir / "spark" / "benchmark_tasks.json"
    tasks_path.parent.mkdir(parents=True, exist_ok=True)
    tasks_path.write_text(
        json.dumps({"tasks": tasks}, indent=2),
        encoding="utf-8",
    )
    command = [
        sys.executable,
        str(repo_root() / "spark_pipeline" / "benchmark_suite.py"),
        "--tasks",
        str(tasks_path),
        "--shuffle-partitions",
        str(args.shuffle_partitions),
        "--driver-memory",
        args.spark_driver_memory,
        "--executor-memory",
        args.spark_executor_memory,
        "--auto-broadcast-threshold-bytes",
        str(args.auto_broadcast_threshold_bytes),
        "--parquet-batch-size",
        str(args.parquet_batch_size),
    ]
    if args.enable_aqe:
        command.append("--enable-aqe")
    if args.enable_vectorized_parquet:
        command.append("--enable-vectorized-parquet")
    if args.enable_vectored_io:
        command.append("--enable-vectored-io")
    run(command)


def run_plots(args: argparse.Namespace, summary_path: Path) -> None:
    plots_dir = args.plots_dir or (args.reports_dir / "plots")
    command = [
        sys.executable,
        str(repo_root() / "experiments" / "plot_research.py"),
        "--summary",
        str(summary_path),
        "--plots-dir",
        str(plots_dir),
    ]
    run(command)


def rows_from_reports(
    *,
    skew: str,
    row_count: int,
    workload: str,
    dataset_repetition: int,
    dataset_seed: int,
    spark_repetition: int,
    preprocess_result_path: Path,
    spark_report_path: Path,
) -> list[dict]:
    preprocess = json.loads(preprocess_result_path.read_text(encoding="utf-8"))
    spark = json.loads(spark_report_path.read_text(encoding="utf-8"))
    before_tau = preprocess.get("before_max_mean_ratio") or preprocess.get("before", {}).get(
        "max_mean_ratio"
    )
    after_tau = preprocess.get("after_max_mean_ratio") or preprocess.get("after", {}).get(
        "max_mean_ratio"
    )
    preprocessing_seconds = (
        preprocess.get("preprocessing_total_seconds")
        or preprocess.get("elapsed_seconds")
        or 0.0
    )
    metric_values = partition_metric_values(preprocess)

    results_by_mode = {
        result.get("mode"): result
        for result in spark.get("results", [])
        if not result.get("skipped")
    }
    selected_results = []
    if "baseline" in results_by_mode:
        selected_results.append(("baseline", results_by_mode["baseline"]))
    if "method_aware" in results_by_mode:
        selected_results.append(("repartitioner", results_by_mode["method_aware"]))
    elif "physical_only" in results_by_mode:
        selected_results.append(("repartitioner", results_by_mode["physical_only"]))

    rows = []
    for variant, result in selected_results:
        mode = result.get("mode")
        if variant == "baseline":
            tau = before_tau
            preprocessing = 0.0
            partition_metrics = metric_values["before"]
        else:
            tau = after_tau
            preprocessing = preprocessing_seconds
            partition_metrics = metric_values["after"]
        rows.append(
            {
                "skew": skew,
                "workload": workload,
                "rows": row_count,
                "dataset_repetition": dataset_repetition,
                "dataset_seed": dataset_seed,
                "spark_repetition": spark_repetition,
                "variant": variant,
                "spark_time_seconds": result.get("elapsed_seconds"),
                "tau": tau,
                **partition_metrics,
                "preprocessing_seconds": preprocessing,
                "total_with_preprocessing_seconds": (
                    float(result.get("elapsed_seconds") or 0.0) + float(preprocessing)
                ),
                "spark_mode": mode,
                "correctness_json": json.dumps(result.get("correctness") or {}, sort_keys=True),
            }
        )
    return rows


def partition_metric_values(preprocess: dict) -> dict[str, dict]:
    total_rows = float(preprocess.get("rows") or 0.0)
    target_rows = float(
        preprocess.get("target_partition_rows")
        or preprocess.get("partition_bound", {}).get("target_partition_rows")
        or 0.0
    )
    bytes_per_row = estimated_bytes_per_row(preprocess)
    before = phase_partition_metrics(
        preprocess,
        phase="before",
        total_rows=total_rows,
        target_rows=target_rows,
        bytes_per_row=bytes_per_row,
    )
    after = phase_partition_metrics(
        preprocess,
        phase="after",
        total_rows=total_rows,
        target_rows=target_rows,
        bytes_per_row=bytes_per_row,
    )
    before_max = float(before.get("max_partition_rows") or 0.0)
    after_max = float(after.get("max_partition_rows") or 0.0)
    reduction_factor = before_max / after_max if before_max > 0 and after_max > 0 else None
    before["skew_reduction_factor"] = 1.0 if reduction_factor is not None else None
    after["skew_reduction_factor"] = reduction_factor
    before["skew_remaining_ratio"] = 1.0 if reduction_factor is not None else None
    after["skew_remaining_ratio"] = after_max / before_max if before_max > 0 else None
    return {"before": before, "after": after}


def phase_partition_metrics(
    preprocess: dict,
    *,
    phase: str,
    total_rows: float,
    target_rows: float,
    bytes_per_row: float | None,
) -> dict:
    skew = preprocess.get(f"{phase}_skew") or {}
    legacy = preprocess.get(phase) or {}
    max_rows = numeric_value(
        preprocess.get(f"{phase}_max_partition_size"),
        skew.get("max_partition_size"),
        legacy.get("max"),
    )
    mean_rows = numeric_value(
        preprocess.get(f"{phase}_mean_partition_size"),
        skew.get("mean_partition_size"),
        legacy.get("mean"),
    )
    p95_rows = numeric_value(skew.get("p95_partition_size"))
    median_rows = numeric_value(skew.get("median_partition_size"))
    variance = numeric_value(skew.get("partition_size_variance"))
    cv = numeric_value(preprocess.get(f"{phase}_cv"), skew.get("coefficient_of_variation"))
    max_mean_ratio = numeric_value(
        preprocess.get(f"{phase}_max_mean_ratio"),
        skew.get("max_mean_imbalance_ratio"),
        legacy.get("max_mean_ratio"),
    )
    max_minus_mean = max_rows - mean_rows if max_rows is not None and mean_rows is not None else None
    largest_share = max_rows / total_rows if max_rows is not None and total_rows > 0 else None
    max_over_target = max_rows / target_rows if max_rows is not None and target_rows > 0 else None
    return {
        "max_partition_rows": max_rows,
        "mean_partition_rows": mean_rows,
        "median_partition_rows": median_rows,
        "p95_partition_rows": p95_rows,
        "partition_size_variance": variance,
        "coefficient_of_variation": cv,
        "max_mean_ratio": max_mean_ratio,
        "largest_partition_share": largest_share,
        "max_minus_mean_partition_rows": max_minus_mean,
        "max_over_target_partition_rows": max_over_target,
        "max_partition_bytes_estimated": multiply_optional(max_rows, bytes_per_row),
        "p95_partition_bytes_estimated": multiply_optional(p95_rows, bytes_per_row),
    }


def estimated_bytes_per_row(preprocess: dict) -> float | None:
    cost = preprocess.get("cost_estimate") or {}
    bytes_value = numeric_value(
        preprocess.get("cost_estimated_bytes_written"),
        cost.get("estimated_bytes_written"),
        cost.get("estimated_bytes_read"),
    )
    rows_value = numeric_value(
        preprocess.get("cost_estimated_rows_written"),
        cost.get("estimated_rows_written"),
        preprocess.get("rows"),
    )
    if bytes_value is None or rows_value is None or rows_value <= 0:
        return None
    return bytes_value / rows_value


def numeric_value(*values) -> float | None:
    for value in values:
        if value is None or value == "":
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def multiply_optional(left: float | None, right: float | None) -> float | None:
    if left is None or right is None:
        return None
    return left * right


def aggregate_summary_rows(rows: list[dict], trim_fraction: float) -> list[dict]:
    grouped: dict[tuple, list[dict]] = {}
    for row in rows:
        key = (row["skew"], row["workload"], row["rows"], row["variant"])
        grouped.setdefault(key, []).append(row)

    aggregated = []
    for (skew, workload, row_count, variant), group in sorted(grouped.items()):
        spark_values = [float(row["spark_time_seconds"]) for row in group]
        total_values = [float(row["total_with_preprocessing_seconds"]) for row in group]
        tau_by_dataset = {
            row["dataset_repetition"]: float(row["tau"])
            for row in group
            if row["tau"] is not None
        }
        tau_values = list(tau_by_dataset.values())
        preprocessing_values = [float(row["preprocessing_seconds"]) for row in group]
        representative = group[0]
        metric_columns = partition_metric_columns()
        aggregated.append(
            {
                "skew": skew,
                "workload": workload,
                "rows": row_count,
                "variant": variant,
                "spark_time_seconds": trimmed_mean(spark_values, trim_fraction),
                "tau": trimmed_mean(tau_values, trim_fraction) if tau_values else None,
                **{
                    column: trimmed_mean(
                        [float(row[column]) for row in group if row.get(column) not in {"", None}],
                        trim_fraction,
                    )
                    for column in metric_columns
                },
                "preprocessing_seconds": trimmed_mean(preprocessing_values, trim_fraction),
                "total_with_preprocessing_seconds": trimmed_mean(total_values, trim_fraction),
                "spark_mode": representative["spark_mode"],
                "correctness_json": representative["correctness_json"],
                "dataset_repetitions": len({row["dataset_repetition"] for row in group}),
                "spark_repetitions": len({row["spark_repetition"] for row in group}),
                "total_repetitions": len(group),
                "trim_fraction": trim_fraction,
                "spark_time_min": min(spark_values),
                "spark_time_max": max(spark_values),
                "tau_min": min(tau_values) if tau_values else None,
                "tau_max": max(tau_values) if tau_values else None,
            }
        )

    return aggregated


def partition_metric_columns() -> list[str]:
    return [
        "max_partition_rows",
        "mean_partition_rows",
        "median_partition_rows",
        "p95_partition_rows",
        "partition_size_variance",
        "coefficient_of_variation",
        "max_mean_ratio",
        "largest_partition_share",
        "max_minus_mean_partition_rows",
        "max_over_target_partition_rows",
        "max_partition_bytes_estimated",
        "p95_partition_bytes_estimated",
        "skew_reduction_factor",
        "skew_remaining_ratio",
    ]


def trimmed_mean(values: list[float], trim_fraction: float) -> float:
    if not values:
        return 0.0
    if not 0.0 <= trim_fraction < 0.5:
        raise ValueError("--trim-fraction must be in [0.0, 0.5)")

    values = sorted(values)
    trim_count = int(len(values) * trim_fraction)
    if trim_count > 0 and len(values) > 2 * trim_count:
        values = values[trim_count:-trim_count]
    return sum(values) / len(values)


def write_summary(path: Path, rows: list[dict], *, raw: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "skew",
        "workload",
        "rows",
        *(["dataset_repetition", "dataset_seed", "spark_repetition"] if raw else []),
        "variant",
        "spark_time_seconds",
        "tau",
        *partition_metric_columns(),
        "preprocessing_seconds",
        "total_with_preprocessing_seconds",
        "spark_mode",
        "correctness_json",
        *(
            []
            if raw
            else [
                "dataset_repetitions",
                "spark_repetitions",
                "total_repetitions",
                "trim_fraction",
                "spark_time_min",
                "spark_time_max",
                "tau_min",
                "tau_max",
            ]
        ),
    ]
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def run(command: list[str]) -> None:
    print("+", " ".join(command), flush=True)
    subprocess.run(command, cwd=repo_root(), check=True)


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


if __name__ == "__main__":
    main()
