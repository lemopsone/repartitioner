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
from typing import Any


DEFAULT_DISTRIBUTIONS = ["uniform", "single_heavy", "multi_heavy", "zipf"]
DEFAULT_ROWS = [10_000, 100_000, 1_000_000]
DEFAULT_HEAVY_FRACTIONS = [0.10, 0.25, 0.50, 0.75]
DEFAULT_MAX_PARTITIONS = [4, 8, 16, 32]
DEFAULT_TARGET_PARTITION_SIZE_MB = [16, 64, 128]
DEFAULT_ZIPF_EXPONENT = 1.20
DEFAULT_HEAVY_KEY_ALPHA = 2.0
SUMMARY_COLUMNS = [
    "scenario",
    "rows",
    "distinct_keys",
    "heavy_fraction",
    "zipf_exponent",
    "max_partitions",
    "target_partition_size_mb",
    "heavy_key_alpha",
    "preprocessing_total_seconds",
    "spark_baseline_seconds",
    "spark_physical_only_seconds",
    "spark_method_aware_seconds",
    "end_to_end_physical_only_seconds",
    "end_to_end_method_aware_seconds",
    "before_max_mean_ratio",
    "after_max_mean_ratio",
    "before_max_partition_size",
    "after_max_partition_size",
    "heavy_hitter_count",
    "output_partitions",
    "output_file_count",
    "target_partition_size_satisfied",
    "method_aware_row_count_matches_baseline",
    "method_aware_result_rows_match_baseline",
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
    parser.add_argument("--zipf-exponent", type=float, default=DEFAULT_ZIPF_EXPONENT)
    parser.add_argument("--heavy-key-alpha", type=float, default=DEFAULT_HEAVY_KEY_ALPHA)
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
    output_path = data_dir / "preprocessed"
    preprocessor_result_path = reports_dir / "preprocessor.json"
    spark_json_path = reports_dir / "spark.json"
    spark_csv_path = reports_dir / "spark.csv"

    generated_metadata = generate_dataset(case, input_path)
    preprocessor_result = run_preprocessor(case, input_path, output_path, preprocessor_result_path, args)
    spark_result = None
    if not args.skip_spark:
        spark_result = run_spark_benchmark(
            input_path,
            output_path,
            spark_json_path,
            spark_csv_path,
            args,
        )
        validate_method_aware_correctness(spark_result)

    summary = build_summary_row(case, generated_metadata, preprocessor_result, spark_result)
    summary["artifacts_dir"] = str(case_dir)
    summary["generated_metadata_path"] = str(input_path.with_suffix(input_path.suffix + ".json"))
    summary["preprocessor_result_path"] = str(preprocessor_result_path)
    summary["spark_result_path"] = str(spark_json_path) if spark_result is not None else None
    return summary


def generate_dataset(case: SuiteCase, input_path: Path) -> dict[str, Any]:
    input_path.parent.mkdir(parents=True, exist_ok=True)
    command = [
        sys.executable,
        str(repo_root() / "experiments" / generator_script(case.distribution)),
        "--output",
        str(input_path),
        "--rows",
        str(case.rows),
        "--seed",
        str(case.seed),
        "--key-cardinality",
        str(case.key_cardinality),
    ]
    if case.distribution == "single_heavy":
        command.extend(["--heavy-fraction", str(case.heavy_fraction)])
    elif case.distribution == "multi_heavy":
        command.extend(["--heavy-fraction", str(case.heavy_fraction), "--heavy-keys", "4"])
    elif case.distribution == "zipf":
        command.extend(["--zipf-exponent", str(case.zipf_exponent)])

    run_command(command, cwd=repo_root())
    return read_json(input_path.with_suffix(input_path.suffix + ".json"))


def run_preprocessor(
    case: SuiteCase,
    input_path: Path,
    output_path: Path,
    result_path: Path,
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
        "--target-partition-size-mb",
        str(case.target_partition_size_mb),
        "--max-partitions",
        str(case.max_partitions),
        "--heavy-key-alpha",
        str(case.heavy_key_alpha),
        "--seed",
        str(case.seed),
        "--input-metadata",
        str(input_path.with_suffix(input_path.suffix + ".json")),
    ]
    if args.release:
        command.append("--release")
    if args.force_rewrite:
        command.append("--force-rewrite")

    run_command(command, cwd=repo_root())
    return read_json(result_path)


def run_spark_benchmark(
    input_path: Path,
    output_path: Path,
    json_report: Path,
    csv_report: Path,
    args: argparse.Namespace,
) -> dict[str, Any]:
    command = [
        sys.executable,
        str(repo_root() / "spark_pipeline" / "run_groupby.py"),
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
        "--include-method-aware",
    ]
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
    spark_by_mode = spark_results_by_mode(spark_result)
    baseline = spark_by_mode.get("baseline")
    physical_only = spark_by_mode.get("physical_only")
    method_aware = spark_by_mode.get("method_aware")
    preprocessing_total = preprocessor_result.get("preprocessing_total_seconds")
    spark_physical = elapsed(physical_only)
    spark_method = elapsed(method_aware)
    feasibility = preprocessor_result.get("feasibility") or {}

    return {
        "scenario": case.distribution,
        "rows": case.rows,
        "distinct_keys": generated_metadata.get("distinct_keys") or preprocessor_result.get("distinct_keys"),
        "heavy_fraction": case.heavy_fraction,
        "zipf_exponent": case.zipf_exponent if case.distribution == "zipf" else None,
        "max_partitions": case.max_partitions,
        "target_partition_size_mb": case.target_partition_size_mb,
        "heavy_key_alpha": case.heavy_key_alpha,
        "preprocessing_total_seconds": preprocessing_total,
        "spark_baseline_seconds": elapsed(baseline),
        "spark_physical_only_seconds": spark_physical,
        "spark_method_aware_seconds": spark_method,
        "end_to_end_physical_only_seconds": add_optional(preprocessing_total, spark_physical),
        "end_to_end_method_aware_seconds": add_optional(preprocessing_total, spark_method),
        "before_max_mean_ratio": (preprocessor_result.get("before") or {}).get("max_mean_ratio"),
        "after_max_mean_ratio": (preprocessor_result.get("after") or {}).get("max_mean_ratio"),
        "before_max_partition_size": (preprocessor_result.get("before") or {}).get("max"),
        "after_max_partition_size": (preprocessor_result.get("after") or {}).get("max"),
        "heavy_hitter_count": preprocessor_result.get("heavy_hitter_count"),
        "output_partitions": preprocessor_result.get("output_partitions"),
        "output_file_count": preprocessor_result.get("output_file_count"),
        "target_partition_size_satisfied": feasibility.get("target_partition_size_satisfied"),
        "method_aware_row_count_matches_baseline": correctness(method_aware, "row_count_matches_baseline"),
        "method_aware_result_rows_match_baseline": correctness(method_aware, "result_rows_match_baseline"),
    }


def spark_results_by_mode(spark_result: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    if spark_result is None:
        return {}
    return {
        result["mode"]: result
        for result in spark_result.get("results", [])
        if result.get("workload") == "group_by"
    }


def validate_method_aware_correctness(spark_result: dict[str, Any]) -> None:
    method_aware = spark_results_by_mode(spark_result).get("method_aware")
    if method_aware is None:
        return

    correctness_payload = method_aware.get("correctness") or {}
    if not correctness_payload.get("row_count_matches_baseline"):
        raise RuntimeError("method-aware row count differs from baseline")
    if not correctness_payload.get("result_rows_match_baseline"):
        raise RuntimeError("method-aware result row count differs from baseline")


def write_summary(
    run_dir: Path,
    summary_rows: list[dict[str, Any]],
    failures: list[dict[str, Any]],
) -> None:
    run_dir.mkdir(parents=True, exist_ok=True)
    summary_csv = run_dir / "summary.csv"
    with summary_csv.open("w", encoding="utf-8", newline="") as file:
        fieldnames = SUMMARY_COLUMNS + ["artifacts_dir", "generated_metadata_path", "preprocessor_result_path", "spark_result_path"]
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(summary_rows)

    (run_dir / "summary.json").write_text(
        json.dumps({"results": summary_rows, "failures": failures}, indent=2),
        encoding="utf-8",
    )


def generator_script(distribution: str) -> str:
    mapping = {
        "uniform": "generate_uniform.py",
        "single_heavy": "generate_heavy_key.py",
        "multi_heavy": "generate_multi_heavy_key.py",
        "zipf": "generate_zipf.py",
    }
    try:
        return mapping[distribution]
    except KeyError as exc:
        raise ValueError(f"unsupported distribution: {distribution}") from exc


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
    return result.get("elapsed_seconds")


def correctness(result: dict[str, Any] | None, key: str) -> bool | None:
    if result is None:
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
