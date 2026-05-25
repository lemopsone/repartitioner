#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Run a small automatic research test and delete generated datasets "
            "after completion. Reports are kept."
        )
    )
    parser.add_argument("--reports-dir", type=Path, default=Path("reports/research-smoke"))
    parser.add_argument("--rows", default="100000")
    parser.add_argument("--skews", nargs="+", default=["uniform", "heavy_key", "zipf"])
    parser.add_argument("--workloads", nargs="+", default=["scan", "filter", "group_by", "join"])
    parser.add_argument("--part-rows", type=int, default=1_000_000)
    parser.add_argument("--payload-columns", type=int, default=8)
    parser.add_argument("--min-partitions", type=int, default=1)
    parser.add_argument("--max-partitions", type=int, default=16)
    parser.add_argument("--local-threads", type=int, default=os.cpu_count() or 1)
    parser.add_argument("--shuffle-partitions", type=int, default=16)
    parser.add_argument("--spark-driver-memory", default="8g")
    parser.add_argument("--spark-executor-memory", default="8g")
    parser.add_argument("--auto-broadcast-threshold-bytes", type=int, default=-1)
    parser.add_argument("--enable-aqe", action="store_true")
    parser.add_argument("--parquet-batch-size", type=int, default=1024)
    parser.add_argument("--enable-vectorized-parquet", action="store_true")
    parser.add_argument("--enable-vectored-io", action="store_true")
    parser.add_argument("--dataset-repetitions", type=int, default=2)
    parser.add_argument("--spark-repetitions", "--repetitions", dest="spark_repetitions", type=int, default=3)
    parser.add_argument("--trim-fraction", type=float, default=0.0)
    parser.add_argument(
        "--spark-mode",
        choices=["suite", "per_process"],
        default="suite",
        help="Spark execution mode passed to run_research.py.",
    )
    parser.add_argument(
        "--correctness-level",
        choices=["none", "basic", "full"],
        default="basic",
    )
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--release", action="store_true")
    parser.add_argument(
        "--no-plots",
        action="store_true",
        help="Do not build PNG plots after the smoke run.",
    )
    args = parser.parse_args()

    repo = repo_root()
    args.reports_dir.mkdir(parents=True, exist_ok=True)

    data_dir = Path(tempfile.mkdtemp(prefix="repartitioner-research-data-"))
    try:
        command = [
            sys.executable,
            str(repo / "experiments" / "run_research.py"),
            "--data-dir",
            str(data_dir),
            "--reports-dir",
            str(args.reports_dir),
            "--rows",
            args.rows,
            "--skews",
            *args.skews,
            "--workloads",
            *args.workloads,
            "--max-partitions",
            str(args.max_partitions),
            "--local-threads",
            str(args.local_threads),
            "--min-partitions",
            str(args.min_partitions),
            "--shuffle-partitions",
            str(args.shuffle_partitions),
            "--spark-driver-memory",
            args.spark_driver_memory,
            "--spark-executor-memory",
            args.spark_executor_memory,
            "--seed",
            str(args.seed),
            "--part-rows",
            str(args.part_rows),
            "--payload-columns",
            str(args.payload_columns),
            "--dataset-repetitions",
            str(args.dataset_repetitions),
            "--spark-repetitions",
            str(args.spark_repetitions),
            "--trim-fraction",
            str(args.trim_fraction),
            "--spark-mode",
            args.spark_mode,
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
        if args.release:
            command.append("--release")
        if args.no_plots:
            command.append("--no-plots")

        run(command, repo)
    finally:
        shutil.rmtree(data_dir, ignore_errors=True)
        print(f"Deleted temporary dataset directory: {data_dir}")

    print(f"Reports are available in: {args.reports_dir}")


def run(command: list[str], cwd: Path) -> None:
    print("+", " ".join(command), flush=True)
    subprocess.run(command, cwd=cwd, check=True)


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


if __name__ == "__main__":
    main()
