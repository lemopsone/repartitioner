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
    before_sizes = stats["estimates"]["before_partition_sizes"]
    after_sizes = [partition["row_count"] for partition in manifest["partitions"]]

    result = {
        "input": input_metadata,
        "output_dataset": str(output_dataset),
        "elapsed_seconds": elapsed_seconds,
        "rows": stats["input"]["total_rows"],
        "distinct_keys": stats["input"]["distinct_keys"],
        "mean_key_frequency": stats["input"]["mean_key_frequency"],
        "max_key_frequency": stats["input"]["max_key_frequency"],
        "heavy_hitter_count": len(stats["input"]["heavy_hitters"]),
        "output_partitions": partition_plan["output_partitions"],
        "target_partition_rows": partition_plan["target_partition_rows"],
        "output_file_count": len(manifest["output_files"]),
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
