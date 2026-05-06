#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Collect Rust preprocessor metadata into one JSON result."
    )
    parser.add_argument("--output-dataset", required=True, type=Path)
    parser.add_argument("--input-metadata", type=Path)
    parser.add_argument("--elapsed-seconds", type=float)
    parser.add_argument("--result", type=Path, help="Optional JSON output path.")
    args = parser.parse_args()

    result = collect_results(
        args.output_dataset,
        input_metadata_path=args.input_metadata,
        elapsed_seconds=args.elapsed_seconds,
    )
    payload = json.dumps(result, indent=2)
    if args.result:
        args.result.parent.mkdir(parents=True, exist_ok=True)
        args.result.write_text(payload, encoding="utf-8")
    print(payload)


def collect_results(
    output_dataset: Path,
    input_metadata_path: Path | None = None,
    elapsed_seconds: float | None = None,
) -> dict:
    partition_plan = read_json(output_dataset / "_partition_plan.json")
    stats = read_json(output_dataset / "_stats.json")
    manifest = read_json(output_dataset / "_manifest.json")
    input_metadata = read_json(input_metadata_path) if input_metadata_path else None
    stats_input = stats.get("input", {})
    stats_estimates = stats.get("estimates", {})
    partition_plan_version = partition_plan.get("version")
    stats_version = stats.get("version")
    manifest_version = manifest.get("version")
    timing = stats.get("timing") or {}
    before_sizes = stats_estimates.get("before_partition_sizes", [])
    input_reused = bool(manifest.get("input_reused", False))
    manifest_partitions = manifest.get("partitions", [])
    after_sizes = [partition.get("row_count", 0) for partition in manifest_partitions]
    if input_reused and not after_sizes:
        after_sizes = before_sizes

    result = {
        "metadata_versions": {
            "partition_plan": partition_plan_version,
            "stats": stats_version,
            "manifest": manifest_version,
        },
        "input": input_metadata,
        "output_dataset": str(output_dataset),
        "input_reused": input_reused,
        "dataset_location": manifest.get("dataset_location"),
        "elapsed_seconds": elapsed_seconds,
        "preprocessing_timing": timing,
        "preprocessing_read_seconds": timing.get("read_seconds"),
        "preprocessing_statistics_seconds": timing.get("statistics_seconds"),
        "preprocessing_planning_seconds": timing.get("planning_seconds"),
        "preprocessing_assignment_seconds": timing.get("assignment_seconds"),
        "preprocessing_writing_seconds": timing.get("writing_seconds"),
        "preprocessing_total_seconds": timing.get("total_seconds"),
        "rows": stats_input.get("total_rows"),
        "input_file_count": stats_input.get("input_file_count"),
        "small_file_count": stats_input.get("small_file_count"),
        "oversized_file_count": stats_input.get("oversized_file_count"),
        "distinct_keys": stats_input.get("distinct_keys"),
        "mean_key_frequency": stats_input.get("mean_key_frequency"),
        "max_key_frequency": stats_input.get("max_key_frequency"),
        "heavy_hitter_count": len(stats_input.get("heavy_hitters", [])),
        "output_partitions": partition_plan.get("output_partitions"),
        "target_partition_rows": partition_plan.get("target_partition_rows"),
        "job_type": partition_plan.get("job_type"),
        "downstream_engine": partition_plan.get("downstream_engine"),
        "min_partitions": partition_plan.get("min_partitions"),
        "max_partitions": partition_plan.get("max_partitions"),
        "required_partitions_by_size": partition_plan.get("required_partitions_by_size"),
        "feasibility": partition_plan.get("feasibility"),
        "technical_columns": partition_plan.get("technical_columns"),
        "recommended_downstream_plan": partition_plan.get("recommended_downstream_plan"),
        "join_plan": partition_plan.get("join_plan"),
        "rewrite_required": partition_plan.get("rewrite_required"),
        "action": partition_plan.get("action"),
        "skip_reason": partition_plan.get("skip_reason"),
        "cost_estimate": partition_plan.get("cost_estimate"),
        "heavy_hitter_detection": stats.get("heavy_hitter_detection"),
        "storage": stats.get("storage"),
        "resources": stats.get("resources"),
        "output_file_count": len(manifest.get("output_files", [])),
        "before": partition_summary(before_sizes),
        "after": partition_summary(after_sizes),
        "partition_plan_path": str(output_dataset / "_partition_plan.json"),
        "stats_path": str(output_dataset / "_stats.json"),
        "manifest_path": str(output_dataset / "_manifest.json"),
    }
    return result


def partition_summary(sizes: list[int]) -> dict:
    if not sizes:
        return {
            "partition_sizes": [],
            "max": 0,
            "mean": 0.0,
            "max_mean_ratio": 0.0,
        }

    total = sum(sizes)
    mean = total / len(sizes)
    max_size = max(sizes)
    return {
        "partition_sizes": sizes,
        "max": max_size,
        "mean": mean,
        "max_mean_ratio": max_size / mean if mean > 0 else 0.0,
    }


def read_json(path: Path | None) -> dict | None:
    if path is None:
        return None
    return json.loads(path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    main()
