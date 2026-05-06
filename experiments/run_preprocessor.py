#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import time
from pathlib import Path

from collect_results import collect_results


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the Rust repartitioner on a generated dataset and collect JSON results."
    )
    parser.add_argument("--input", required=True, type=Path, help="Input Parquet file or directory.")
    parser.add_argument("--output", required=True, type=Path, help="Output dataset directory.")
    parser.add_argument("--result", required=True, type=Path, help="Collected result JSON path.")
    parser.add_argument("--config", type=Path, help="Optional config path to write/use.")
    parser.add_argument("--key-column", default="user_id")
    parser.add_argument("--target-partition-size-mb", type=int, default=128)
    parser.add_argument("--target-file-size-mb", type=int, default=128)
    parser.add_argument("--min-file-size-mb", type=int, default=16)
    parser.add_argument("--max-partitions", type=int, default=16)
    parser.add_argument("--heavy-key-alpha", type=float, default=2.0)
    parser.add_argument("--heavy-hitter-mode", choices=["exact", "approximate"], default="exact")
    parser.add_argument("--approximate-capacity", type=int, default=10000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--force-rewrite",
        action="store_true",
        help="Force materialized rewrite even when the planner would choose no-op.",
    )
    parser.add_argument(
        "--release",
        action="store_true",
        help="Run cargo with --release for timing-oriented experiments.",
    )
    parser.add_argument(
        "--input-metadata",
        type=Path,
        help="Optional generator metadata JSON. Defaults to <input>.json for files.",
    )
    args = parser.parse_args()

    config_path = args.config or args.result.with_suffix(".config.yaml")
    write_config(
        config_path,
        input_path=args.input,
        output_path=args.output,
        key_column=args.key_column,
        target_partition_size_mb=args.target_partition_size_mb,
        target_file_size_mb=args.target_file_size_mb,
        min_file_size_mb=args.min_file_size_mb,
        max_partitions=args.max_partitions,
        heavy_key_alpha=args.heavy_key_alpha,
        heavy_hitter_mode=args.heavy_hitter_mode,
        approximate_capacity=args.approximate_capacity,
        seed=args.seed,
        force_rewrite=args.force_rewrite,
    )

    command = ["cargo", "run", "-p", "repartitioner"]
    if args.release:
        command.append("--release")
    command.extend(["--", "--config", str(config_path)])

    started = time.perf_counter()
    completed = subprocess.run(
        command,
        cwd=repo_root(),
        check=False,
        text=True,
        capture_output=True,
    )
    elapsed = time.perf_counter() - started

    if completed.returncode != 0:
        raise SystemExit(
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

    input_metadata = args.input_metadata
    if input_metadata is None and args.input.is_file():
        candidate = args.input.with_suffix(args.input.suffix + ".json")
        input_metadata = candidate if candidate.exists() else None

    result = collect_results(
        args.output,
        input_metadata_path=input_metadata,
        elapsed_seconds=elapsed,
    )
    result.update(
        {
            "command": command,
            "config_path": str(config_path),
            "stdout": completed.stdout,
            "stderr": completed.stderr,
        }
    )
    args.result.parent.mkdir(parents=True, exist_ok=True)
    args.result.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result, indent=2))


def write_config(
    path: Path,
    *,
    input_path: Path,
    output_path: Path,
    key_column: str,
    target_partition_size_mb: int,
    max_partitions: int,
    heavy_key_alpha: float,
    seed: int,
    target_file_size_mb: int = 128,
    min_file_size_mb: int = 16,
    heavy_hitter_mode: str = "exact",
    approximate_capacity: int = 10_000,
    force_rewrite: bool = False,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    force_rewrite_line = "  force_rewrite: true\n" if force_rewrite else ""
    path.write_text(
        f"""dataset:
  input: "{input_path}"
  output: "{output_path}"
  format: "parquet"

partitioning:
  key_columns: ["{key_column}"]
  target_partition_size_mb: {target_partition_size_mb}
  max_partitions: {max_partitions}
  strategy: "adaptive_hash_salt"
  heavy_key_alpha: {heavy_key_alpha}
  seed: {seed}
{force_rewrite_line}

statistics:
  heavy_hitter_mode: "{heavy_hitter_mode}"
  approximate_capacity: {approximate_capacity}

storage:
  target_file_size_mb: {target_file_size_mb}
  min_file_size_mb: {min_file_size_mb}

job:
  type: "group_by"
  downstream_engine: "spark"

resources:
  local_threads: 1
  memory_limit_mb: 1024
""",
        encoding="utf-8",
    )


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


if __name__ == "__main__":
    main()
