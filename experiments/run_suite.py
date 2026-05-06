#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import itertools
import json
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


GROUP_BY_SCENARIOS = {
    "uniform_no_skew",
    "single_heavy",
    "multi_heavy",
    "zipf",
    "normal_key_hash_skew",
}
JOIN_SCENARIOS = {
    "small_right_join",
    "shared_heavy_join",
    "one_sided_heavy_join",
}
DEFAULT_DISTRIBUTIONS = [
    "uniform_no_skew",
    "single_heavy",
    "multi_heavy",
    "zipf",
    "normal_key_hash_skew",
    "small_right_join",
    "shared_heavy_join",
    "one_sided_heavy_join",
]
DEFAULT_ROWS = [10_000, 100_000, 1_000_000]
DEFAULT_HEAVY_FRACTIONS = [0.10, 0.25, 0.50, 0.75]
DEFAULT_MAX_PARTITIONS = [4, 8, 16, 32]
DEFAULT_TARGET_PARTITION_SIZE_MB = [16, 64, 128]
DEFAULT_ZIPF_EXPONENT = 1.20
DEFAULT_HEAVY_KEY_ALPHA = 2.0
SUMMARY_COLUMNS = [
    "scenario",
    "scenario_id",
    "workload",
    "rows",
    "distinct_keys",
    "heavy_fraction",
    "zipf_exponent",
    "max_partitions",
    "target_partition_size_mb",
    "partitioning_strategy",
    "target_file_size_mb",
    "min_file_size_mb",
    "heavy_key_alpha",
    "heavy_hitter_mode",
    "approximate_capacity",
    "rewrite_required",
    "cost_estimated_rows_written",
    "cost_estimated_bytes_written",
    "preprocessing_writing_seconds",
    "preprocessing_total_seconds",
    "spark_baseline_seconds",
    "spark_physical_only_seconds",
    "spark_method_aware_seconds",
    "spark_method_aware_join_seconds",
    "end_to_end_physical_only_seconds",
    "end_to_end_method_aware_seconds",
    "before_mean_partition_size",
    "after_mean_partition_size",
    "before_max_mean_ratio",
    "after_max_mean_ratio",
    "before_max_partition_size",
    "after_max_partition_size",
    "before_cv",
    "after_cv",
    "target_rows_satisfied_after",
    "skew_reduction_ratio",
    "heavy_hitter_count",
    "output_partitions",
    "output_file_count",
    "target_partition_size_satisfied",
    "method_aware_row_count_matches_baseline",
    "method_aware_result_rows_match_baseline",
    "method_aware_exact_group_counts_match",
    "method_aware_checksum_matches_baseline",
    "group_by_exact_correctness",
    "join_checksum_correctness",
    "method_aware_join_applied",
    "method_aware_join_skipped",
    "method_aware_join_skip_reason",
    "method_aware_join_strategy",
]


@dataclass(frozen=True)
class SuiteCase:
    distribution: str
    rows: int
    heavy_fraction: float
    max_partitions: int
    target_partition_size_mb: int
    zipf_exponent: float
    heavy_key_alpha: float
    seed: int
    key_cardinality: int

    @property
    def scenario_id(self) -> str:
        heavy = format_decimal(self.heavy_fraction)
        zipf = format_decimal(self.zipf_exponent)
        return (
            f"{self.distribution}"
            f"__rows_{self.rows}"
            f"__hf_{heavy}"
            f"__mp_{self.max_partitions}"
            f"__target_{self.target_partition_size_mb}"
            f"__zipf_{zipf}"
        )

    @property
    def workload(self) -> str:
        if self.distribution in JOIN_SCENARIOS:
            return "join"
        return "group_by"

    @property
    def is_join(self) -> bool:
        return self.workload == "join"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the full repartitioner experiment matrix for research reports."
    )
    parser.add_argument(
        "--run-dir",
        type=Path,
        default=None,
        help="Directory for all artifacts. Defaults to experiments/runs/<timestamp>.",
    )
    parser.add_argument("--distributions", default=",".join(DEFAULT_DISTRIBUTIONS))
    parser.add_argument("--rows", default=",".join(str(value) for value in DEFAULT_ROWS))
    parser.add_argument(
        "--heavy-fractions",
        default=",".join(str(value) for value in DEFAULT_HEAVY_FRACTIONS),
    )
    parser.add_argument(
        "--max-partitions",
        default=",".join(str(value) for value in DEFAULT_MAX_PARTITIONS),
    )
    parser.add_argument(
        "--target-partition-size-mb",
        default=",".join(str(value) for value in DEFAULT_TARGET_PARTITION_SIZE_MB),
    )
    parser.add_argument("--target-file-size-mb", type=int, default=128)
    parser.add_argument("--min-file-size-mb", type=int, default=16)
    parser.add_argument("--zipf-exponent", type=float, default=DEFAULT_ZIPF_EXPONENT)
    parser.add_argument("--heavy-key-alpha", type=float, default=DEFAULT_HEAVY_KEY_ALPHA)
    parser.add_argument("--heavy-hitter-mode", choices=["exact", "approximate"], default="exact")
    parser.add_argument("--approximate-capacity", type=int, default=10000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--key-cardinality", type=int, default=10_000)
    parser.add_argument("--shuffle-partitions", type=int, default=200)
    parser.add_argument("--release", action="store_true", help="Run Rust preprocessor in release mode.")
    parser.add_argument("--force-rewrite", action="store_true")
    parser.add_argument("--skip-spark", action="store_true")
    parser.add_argument("--warmup-spark", action="store_true")
    parser.add_argument("--continue-on-error", action="store_true")
    parser.add_argument("--limit", type=int, help="Run only the first N matrix cases.")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    run_dir = args.run_dir or repo_root() / "experiments" / "runs" / timestamp_slug()
    cases = build_cases(args)
    if args.limit is not None:
        cases = cases[: args.limit]

    if args.dry_run:
        print(json.dumps([case.__dict__ | {"scenario_id": case.scenario_id} for case in cases], indent=2))
        return

    run_dir.mkdir(parents=True, exist_ok=True)
    summary_rows: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []

    for index, case in enumerate(cases, start=1):
        print(f"[{index}/{len(cases)}] {case.scenario_id}", flush=True)
        try:
            summary_rows.append(run_case(case, run_dir, args))
            write_summary(run_dir, summary_rows, failures)
        except Exception as exc:
            failure = {"scenario": case.scenario_id, "error": str(exc)}
            failures.append(failure)
            write_summary(run_dir, summary_rows, failures)
            if not args.continue_on_error:
                raise
            print(json.dumps(failure, indent=2), file=sys.stderr, flush=True)

    write_summary(run_dir, summary_rows, failures)
    print(json.dumps({"run_dir": str(run_dir), "cases": len(cases), "failures": failures}, indent=2))


def build_cases(args: argparse.Namespace) -> list[SuiteCase]:
    distributions = parse_str_list(args.distributions)
    rows_values = parse_int_list(args.rows)
    heavy_fractions = parse_float_list(args.heavy_fractions)
    max_partitions = parse_int_list(args.max_partitions)
    target_sizes = parse_int_list(args.target_partition_size_mb)

    return [
        SuiteCase(
            distribution=distribution,
            rows=rows,
            heavy_fraction=heavy_fraction,
            max_partitions=max_partition_count,
            target_partition_size_mb=target_size,
            zipf_exponent=args.zipf_exponent,
            heavy_key_alpha=args.heavy_key_alpha,
            seed=args.seed,
            key_cardinality=args.key_cardinality,
        )
        for distribution, rows, heavy_fraction, max_partition_count, target_size in itertools.product(
            distributions,
            rows_values,
            heavy_fractions,
            max_partitions,
            target_sizes,
        )
    ]


def run_case(case: SuiteCase, run_dir: Path, args: argparse.Namespace) -> dict[str, Any]:
    case_dir = run_dir / case.scenario_id
    data_dir = case_dir / "data"
    reports_dir = case_dir / "reports"
    input_path = data_dir / "input.parquet"
    join_right_path = data_dir / "right.parquet" if case.is_join else None
    output_path = data_dir / "preprocessed"
    preprocessor_result_path = reports_dir / "preprocessor.json"
    preprocessor_config_path = case_dir / "config.yaml"
    spark_json_path = reports_dir / "spark_report.json"
    spark_csv_path = reports_dir / "spark_report.csv"

    generated_metadata = generate_dataset(case, input_path, join_right_path)
    preprocessor_result = run_preprocessor(
        case,
        input_path,
        output_path,
        preprocessor_result_path,
        preprocessor_config_path,
        join_right_path,
        args,
    )
    spark_result = None
    if not args.skip_spark:
        spark_result = run_spark_benchmark(
            case,
            input_path,
            output_path,
            join_right_path,
            spark_json_path,
            spark_csv_path,
            args,
        )
        validate_method_aware_correctness(spark_result)

    summary = build_summary_row(case, generated_metadata, preprocessor_result, spark_result)
    summary["artifacts_dir"] = str(case_dir)
    summary["config_path"] = str(preprocessor_config_path)
    summary["generated_metadata_path"] = str(input_path.with_suffix(input_path.suffix + ".json"))
    summary["partition_plan_path"] = str(output_path / "_partition_plan.json")
    summary["stats_path"] = str(output_path / "_stats.json")
    summary["manifest_path"] = str(output_path / "_manifest.json")
    summary["preprocessor_result_path"] = str(preprocessor_result_path)
    summary["spark_result_path"] = str(spark_json_path) if spark_result is not None else None
    summary["join_right_path"] = str(join_right_path) if join_right_path is not None else None
    write_case_summary(case_dir, summary)
    return summary


def generate_dataset(case: SuiteCase, input_path: Path, join_right_path: Path | None) -> dict[str, Any]:
    input_path.parent.mkdir(parents=True, exist_ok=True)
    if case.distribution == "normal_key_hash_skew":
        metadata = generate_normal_key_hash_skew_dataset(case, input_path)
        return metadata

    effective_distribution = left_distribution(case.distribution)
    command = [
        sys.executable,
        str(repo_root() / "experiments" / generator_script(effective_distribution)),
        "--output",
        str(input_path),
        "--rows",
        str(case.rows),
        "--seed",
        str(case.seed),
        "--key-cardinality",
        str(case.key_cardinality),
    ]
    if effective_distribution == "single_heavy":
        command.extend(["--heavy-fraction", str(case.heavy_fraction)])
    elif effective_distribution == "multi_heavy":
        command.extend(["--heavy-fraction", str(case.heavy_fraction), "--heavy-keys", "4"])
    elif effective_distribution == "zipf":
        command.extend(["--zipf-exponent", str(case.zipf_exponent)])
    elif effective_distribution == "uniform_no_skew":
        command.extend(["--balanced", "--scenario-name", "uniform_no_skew"])

    run_command(command, cwd=repo_root())
    metadata = read_json(input_path.with_suffix(input_path.suffix + ".json"))
    metadata["suite_scenario"] = case.distribution
    if join_right_path is not None:
        metadata["join_right"] = generate_join_right_dataset(case, join_right_path)
    input_path.with_suffix(input_path.suffix + ".json").write_text(
        json.dumps(metadata, indent=2),
        encoding="utf-8",
    )
    return metadata


def run_preprocessor(
    case: SuiteCase,
    input_path: Path,
    output_path: Path,
    result_path: Path,
    config_path: Path,
    join_right_path: Path | None,
    args: argparse.Namespace,
) -> dict[str, Any]:
    command = [
        sys.executable,
        str(repo_root() / "experiments" / "run_preprocessor.py"),
        "--input",
        str(input_path),
        "--output",
        str(output_path),
        "--result",
        str(result_path),
        "--config",
        str(config_path),
        "--target-partition-size-mb",
        str(case.target_partition_size_mb),
        "--target-file-size-mb",
        str(args.target_file_size_mb),
        "--min-file-size-mb",
        str(args.min_file_size_mb),
        "--max-partitions",
        str(case.max_partitions),
        "--heavy-key-alpha",
        str(case.heavy_key_alpha),
        "--heavy-hitter-mode",
        args.heavy_hitter_mode,
        "--approximate-capacity",
        str(args.approximate_capacity),
        "--seed",
        str(case.seed),
        "--input-metadata",
        str(input_path.with_suffix(input_path.suffix + ".json")),
        "--job-type",
        case.workload,
    ]
    if join_right_path is not None:
        command.extend(["--join-right", str(join_right_path)])
        command.extend(["--broadcast-threshold-mb", str(join_broadcast_threshold_mb(case))])
    if args.release:
        command.append("--release")
    if args.force_rewrite:
        command.append("--force-rewrite")

    run_command(command, cwd=repo_root())
    return read_json(result_path)


def run_spark_benchmark(
    case: SuiteCase,
    input_path: Path,
    output_path: Path,
    join_right_path: Path | None,
    json_report: Path,
    csv_report: Path,
    args: argparse.Namespace,
) -> dict[str, Any]:
    command = [
        sys.executable,
        str(repo_root() / "spark_pipeline" / "benchmark.py"),
        "--original",
        str(input_path),
        "--preprocessed",
        str(output_path),
        "--json-report",
        str(json_report),
        "--csv-report",
        str(csv_report),
        "--shuffle-partitions",
        str(args.shuffle_partitions),
        "--workload",
        case.workload,
        "--include-method-aware",
    ]
    if join_right_path is not None:
        command.extend(["--join-right", str(join_right_path)])
    if args.warmup_spark:
        command.append("--warmup")

    run_command(command, cwd=repo_root())
    return read_json(json_report)


def build_summary_row(
    case: SuiteCase,
    generated_metadata: dict[str, Any],
    preprocessor_result: dict[str, Any],
    spark_result: dict[str, Any] | None,
) -> dict[str, Any]:
    spark_by_mode = spark_results_by_mode(spark_result, case.workload)
    group_by_modes = spark_results_by_mode(spark_result, "group_by")
    join_modes = spark_results_by_mode(spark_result, "join")
    baseline = spark_by_mode.get("baseline")
    physical_only = spark_by_mode.get("physical_only")
    method_aware = spark_by_mode.get("method_aware")
    group_by_method_aware = group_by_modes.get("method_aware")
    join_method_aware = join_modes.get("method_aware")
    preprocessing_total = preprocessor_result.get("preprocessing_total_seconds")
    spark_physical = elapsed(physical_only)
    spark_method = elapsed(method_aware)
    feasibility = preprocessor_result.get("feasibility") or {}
    spark_summary = spark_result.get("summary") if spark_result else {}
    spark_summary = spark_summary or {}

    return {
        "scenario": case.distribution,
        "scenario_id": case.scenario_id,
        "workload": case.workload,
        "rows": case.rows,
        "distinct_keys": generated_metadata.get("distinct_keys") or preprocessor_result.get("distinct_keys"),
        "heavy_fraction": case.heavy_fraction,
        "zipf_exponent": case.zipf_exponent if case.distribution == "zipf" else None,
        "max_partitions": case.max_partitions,
        "target_partition_size_mb": case.target_partition_size_mb,
        "partitioning_strategy": preprocessor_result.get("partitioning_strategy"),
        "target_file_size_mb": (preprocessor_result.get("storage") or {}).get("target_file_size_mb"),
        "min_file_size_mb": (preprocessor_result.get("storage") or {}).get("min_file_size_mb"),
        "heavy_key_alpha": case.heavy_key_alpha,
        "heavy_hitter_mode": args_heavy_hitter_mode(preprocessor_result),
        "approximate_capacity": args_approximate_capacity(preprocessor_result),
        "rewrite_required": preprocessor_result.get("rewrite_required"),
        "cost_estimated_rows_written": preprocessor_result.get("cost_estimated_rows_written"),
        "cost_estimated_bytes_written": preprocessor_result.get("cost_estimated_bytes_written"),
        "preprocessing_writing_seconds": preprocessor_result.get("preprocessing_writing_seconds"),
        "preprocessing_total_seconds": preprocessing_total,
        "spark_baseline_seconds": spark_summary.get("spark_baseline_seconds") or elapsed(baseline),
        "spark_physical_only_seconds": spark_summary.get("spark_physical_only_seconds") or spark_physical,
        "spark_method_aware_seconds": spark_summary.get("spark_method_aware_seconds") or spark_method,
        "spark_method_aware_join_seconds": spark_summary.get("spark_method_aware_join_seconds")
        or elapsed(join_method_aware),
        "end_to_end_physical_only_seconds": add_optional(preprocessing_total, spark_physical),
        "end_to_end_method_aware_seconds": add_optional(preprocessing_total, spark_method),
        "before_mean_partition_size": preprocessor_result.get("before_mean_partition_size")
        or (preprocessor_result.get("before") or {}).get("mean"),
        "after_mean_partition_size": preprocessor_result.get("after_mean_partition_size")
        or (preprocessor_result.get("after") or {}).get("mean"),
        "before_max_mean_ratio": preprocessor_result.get("before_max_mean_ratio")
        or (preprocessor_result.get("before") or {}).get("max_mean_ratio"),
        "after_max_mean_ratio": preprocessor_result.get("after_max_mean_ratio")
        or (preprocessor_result.get("after") or {}).get("max_mean_ratio"),
        "before_max_partition_size": preprocessor_result.get("before_max_partition_size")
        or (preprocessor_result.get("before") or {}).get("max"),
        "after_max_partition_size": preprocessor_result.get("after_max_partition_size")
        or (preprocessor_result.get("after") or {}).get("max"),
        "before_cv": preprocessor_result.get("before_cv"),
        "after_cv": preprocessor_result.get("after_cv"),
        "target_rows_satisfied_after": preprocessor_result.get("target_rows_satisfied_after"),
        "skew_reduction_ratio": preprocessor_result.get("skew_reduction_ratio"),
        "heavy_hitter_count": preprocessor_result.get("heavy_hitter_count"),
        "output_partitions": preprocessor_result.get("output_partitions"),
        "output_file_count": preprocessor_result.get("output_file_count"),
        "target_partition_size_satisfied": feasibility.get("target_partition_size_satisfied"),
        "method_aware_row_count_matches_baseline": correctness(method_aware, "row_count_matches_baseline"),
        "method_aware_result_rows_match_baseline": correctness(method_aware, "result_rows_match_baseline"),
        "method_aware_exact_group_counts_match": correctness(method_aware, "exact_group_counts_match"),
        "method_aware_checksum_matches_baseline": correctness(method_aware, "checksum_matches_baseline"),
        "group_by_exact_correctness": spark_summary.get("group_by_exact_correctness")
        if "group_by_exact_correctness" in spark_summary
        else correctness(group_by_method_aware, "exact_group_counts_match"),
        "join_checksum_correctness": spark_summary.get("join_checksum_correctness")
        if "join_checksum_correctness" in spark_summary
        else correctness(join_method_aware, "checksum_matches_baseline"),
        "method_aware_join_applied": spark_summary.get("method_aware_join_applied"),
        "method_aware_join_skipped": spark_summary.get("method_aware_join_skipped"),
        "method_aware_join_skip_reason": spark_summary.get("method_aware_join_skip_reason"),
        "method_aware_join_strategy": spark_summary.get("method_aware_join_strategy")
        or ((join_method_aware or {}).get("extra") or {}).get("strategy"),
    }


def spark_results_by_mode(
    spark_result: dict[str, Any] | None,
    workload: str,
) -> dict[str, dict[str, Any]]:
    if spark_result is None:
        return {}
    return {
        result["mode"]: result
        for result in spark_result.get("results", [])
        if result.get("workload") == workload
    }


def args_heavy_hitter_mode(preprocessor_result: dict[str, Any]) -> str | None:
    detection = preprocessor_result.get("heavy_hitter_detection") or {}
    return detection.get("mode")


def args_approximate_capacity(preprocessor_result: dict[str, Any]) -> int | None:
    detection = preprocessor_result.get("heavy_hitter_detection") or {}
    return detection.get("capacity")


def validate_method_aware_correctness(spark_result: dict[str, Any]) -> None:
    for result in spark_result.get("results", []):
        if result.get("mode") != "method_aware" or result.get("skipped"):
            continue

        workload = result.get("workload")
        correctness_payload = result.get("correctness") or {}
        if not correctness_payload.get("row_count_matches_baseline"):
            raise RuntimeError(f"{workload} method-aware row count differs from baseline")
        if not correctness_payload.get("result_rows_match_baseline"):
            raise RuntimeError(f"{workload} method-aware result row count differs from baseline")
        if workload == "group_by" and not correctness_payload.get("exact_group_counts_match"):
            raise RuntimeError("group_by method-aware counts differ from baseline")
        if workload == "join" and not correctness_payload.get("checksum_matches_baseline"):
            raise RuntimeError("join method-aware checksum differs from baseline")


def write_summary(
    run_dir: Path,
    summary_rows: list[dict[str, Any]],
    failures: list[dict[str, Any]],
) -> None:
    run_dir.mkdir(parents=True, exist_ok=True)
    summary_csv = run_dir / "summary.csv"
    with summary_csv.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=summary_fieldnames())
        writer.writeheader()
        writer.writerows(summary_rows)

    (run_dir / "summary.json").write_text(
        json.dumps({"results": summary_rows, "failures": failures}, indent=2),
        encoding="utf-8",
    )


def write_case_summary(case_dir: Path, summary: dict[str, Any]) -> None:
    with (case_dir / "summary.csv").open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=summary_fieldnames())
        writer.writeheader()
        writer.writerow(summary)


def summary_fieldnames() -> list[str]:
    return SUMMARY_COLUMNS + [
        "artifacts_dir",
        "config_path",
        "generated_metadata_path",
        "partition_plan_path",
        "stats_path",
        "manifest_path",
        "preprocessor_result_path",
        "spark_result_path",
        "join_right_path",
    ]


def generator_script(distribution: str) -> str:
    mapping = {
        "uniform": "generate_uniform.py",
        "uniform_no_skew": "generate_uniform.py",
        "single_heavy": "generate_heavy_key.py",
        "multi_heavy": "generate_multi_heavy_key.py",
        "zipf": "generate_zipf.py",
    }
    try:
        return mapping[distribution]
    except KeyError as exc:
        raise ValueError(f"unsupported distribution: {distribution}") from exc


def left_distribution(distribution: str) -> str:
    if distribution in {"small_right_join", "shared_heavy_join", "one_sided_heavy_join"}:
        return "single_heavy"
    return distribution


def join_broadcast_threshold_mb(case: SuiteCase) -> int:
    if case.distribution == "small_right_join":
        return 10
    return 0


def generate_join_right_dataset(case: SuiteCase, output: Path) -> dict[str, Any]:
    rows = max(1, min(case.rows, 10_000))
    rng_seed = case.seed + 10_000
    if case.distribution == "small_right_join":
        keys = ["heavy_00000000"] + [f"user_{index:08d}" for index in range(min(128, case.key_cardinality))]
    elif case.distribution == "shared_heavy_join":
        heavy_rows = max(1, round(rows * max(case.heavy_fraction, 0.25)))
        normal_rows = max(0, rows - heavy_rows)
        keys = ["heavy_00000000"] * heavy_rows
        keys.extend(f"user_{index % case.key_cardinality:08d}" for index in range(normal_rows))
    elif case.distribution == "one_sided_heavy_join":
        keys = ["heavy_00000000"]
        keys.extend(f"user_{index % case.key_cardinality:08d}" for index in range(max(0, rows - 1)))
    else:
        raise ValueError(f"unsupported join scenario: {case.distribution}")

    metadata = write_parquet_dataset(output, keys, scenario=f"{case.distribution}_right", seed=rng_seed)
    metadata["broadcast_threshold_mb"] = join_broadcast_threshold_mb(case)
    return metadata


def generate_normal_key_hash_skew_dataset(case: SuiteCase, output: Path) -> dict[str, Any]:
    partition_count = max(2, case.max_partitions)
    collision_keys = collision_keys_for_partition(
        partition_count=partition_count,
        seed=case.seed,
        target_partition=0,
        count=min(64, max(4, case.key_cardinality)),
    )
    user_ids = [collision_keys[index % len(collision_keys)] for index in range(case.rows)]
    metadata = write_parquet_dataset(output, user_ids, scenario="normal_key_hash_skew", seed=case.seed)
    metadata.update(
        {
            "key_cardinality": len(collision_keys),
            "hash_collision_partition": 0,
            "hash_collision_partition_count": partition_count,
        }
    )
    return metadata


def collision_keys_for_partition(
    *,
    partition_count: int,
    seed: int,
    target_partition: int,
    count: int,
) -> list[str]:
    keys: list[str] = []
    candidate = 0
    while len(keys) < count:
        key = f"collision_{candidate:08d}"
        encoded = encode_utf8_key("user_id", key)
        if partition_id(encoded, partition_count, seed) == target_partition:
            keys.append(key)
        candidate += 1
    return keys


def write_parquet_dataset(output: Path, user_ids: Iterable[str], *, scenario: str, seed: int) -> dict[str, Any]:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise SystemExit(
            "pyarrow is required for experiment suite generation. Install with: pip install pyarrow"
        ) from exc

    output.parent.mkdir(parents=True, exist_ok=True)
    user_ids = list(user_ids)
    row_ids = list(range(len(user_ids)))
    values = [stable_value(user_id, row_id, seed) for row_id, user_id in enumerate(user_ids)]
    table = pa.table(
        {
            "user_id": pa.array(user_ids, type=pa.string()),
            "row_id": pa.array(row_ids, type=pa.int64()),
            "value": pa.array(values, type=pa.int64()),
        }
    )
    pq.write_table(table, output)

    metadata = {
        "scenario": scenario,
        "rows": len(user_ids),
        "output": str(output),
        "seed": seed,
        "distinct_keys": len(set(user_ids)),
    }
    output.with_suffix(output.suffix + ".json").write_text(
        json.dumps(metadata, indent=2),
        encoding="utf-8",
    )
    return metadata


def stable_value(user_id: str, row_id: int, seed: int) -> int:
    value = seed ^ row_id
    for byte in user_id.encode("utf-8"):
        value = ((value * 131) ^ byte) & 0x7FFF_FFFF
    return value


def encode_utf8_key(column: str, value: str) -> str:
    return f"{len(column)}:{column}#utf8:{len(value)}:{value}"


def partition_id(encoded_key: str, partition_count: int, seed: int) -> int:
    return fnv1a64_seeded(seed, encoded_key.encode("utf-8")) % partition_count


def fnv1a64_seeded(seed: int, data: bytes) -> int:
    hash_value = 0xCBF29CE484222325 ^ seed
    for byte in data:
        hash_value ^= byte
        hash_value = (hash_value * 0x100000001B3) & 0xFFFF_FFFF_FFFF_FFFF
    return hash_value


def run_command(command: list[str], *, cwd: Path) -> None:
    completed = subprocess.run(command, cwd=cwd, text=True, capture_output=True, check=False)
    if completed.returncode != 0:
        raise RuntimeError(
            json.dumps(
                {
                    "command": command,
                    "returncode": completed.returncode,
                    "stdout": completed.stdout,
                    "stderr": completed.stderr,
                },
                indent=2,
            )
        )


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def elapsed(result: dict[str, Any] | None) -> float | None:
    if result is None:
        return None
    if result.get("skipped"):
        return None
    return result.get("elapsed_seconds")


def correctness(result: dict[str, Any] | None, key: str) -> bool | None:
    if result is None:
        return None
    if result.get("skipped"):
        return None
    return (result.get("correctness") or {}).get(key)


def add_optional(left: float | None, right: float | None) -> float | None:
    if left is None or right is None:
        return None
    return left + right


def parse_str_list(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def parse_int_list(value: str) -> list[int]:
    return [int(item) for item in parse_str_list(value)]


def parse_float_list(value: str) -> list[float]:
    return [float(item) for item in parse_str_list(value)]


def format_decimal(value: float) -> str:
    return str(value).replace(".", "p")


def timestamp_slug() -> str:
    return time.strftime("%Y%m%d-%H%M%S")


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


if __name__ == "__main__":
    main()
