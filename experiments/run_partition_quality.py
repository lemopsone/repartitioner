#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import shutil
import subprocess
import sys
from pathlib import Path


DEFAULT_FRACTIONS = "0,0.05,0.10,0.20,0.30,0.40,0.50,0.60,0.70,0.80,0.90"


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Run a fixed-size partition-quality experiment by varying one-heavy-key share."
        )
    )
    parser.add_argument("--data-dir", type=Path, default=Path("data/partition-quality"))
    parser.add_argument("--reports-dir", type=Path, default=Path("reports/partition-quality"))
    parser.add_argument("--rows", type=int, default=10_000_000)
    parser.add_argument("--heavy-fractions", default=DEFAULT_FRACTIONS)
    parser.add_argument("--key-cardinality", type=int, default=10_000)
    parser.add_argument("--part-rows", type=int, default=1_000_000)
    parser.add_argument("--payload-columns", type=int, default=0)
    parser.add_argument("--partitions", type=int, default=16)
    parser.add_argument("--target-partition-size-mb", type=int, default=128)
    parser.add_argument("--local-threads", type=int, default=8)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--release", action="store_true")
    parser.add_argument(
        "--keep-data",
        action="store_true",
        help="Keep generated input and preprocessed Parquet datasets after each point.",
    )
    parser.add_argument("--no-plot", action="store_true")
    args = parser.parse_args()

    if args.rows <= 0:
        raise SystemExit("--rows must be greater than zero")
    if args.partitions <= 0:
        raise SystemExit("--partitions must be greater than zero")

    args.reports_dir.mkdir(parents=True, exist_ok=True)
    args.data_dir.mkdir(parents=True, exist_ok=True)
    rows = []
    for fraction in parse_fractions(args.heavy_fractions):
        rows.extend(run_point(args, fraction))
        write_summary(args.reports_dir / "summary.csv", rows)

    if not args.no_plot:
        plot_summary(args.reports_dir / "summary.csv", args.reports_dir / "quality.png")

    print(f"Wrote {args.reports_dir / 'summary.csv'}")


def parse_fractions(raw: str) -> list[float]:
    fractions = [float(value) for value in raw.split(",") if value.strip()]
    if not fractions:
        raise SystemExit("--heavy-fractions must contain at least one value")
    for fraction in fractions:
        if not 0.0 <= fraction <= 1.0:
            raise SystemExit("--heavy-fractions values must be in [0.0, 1.0]")
    return fractions


def run_point(args: argparse.Namespace, fraction: float) -> list[dict]:
    suffix = f"{int(round(fraction * 100)):03d}"
    dataset_path = args.data_dir / f"heavy_share_{suffix}.parquet"
    preprocessed_path = args.data_dir / f"heavy_share_{suffix}_partitioned"
    result_path = args.reports_dir / "preprocess" / f"heavy_share_{suffix}.json"

    try:
        generate_dataset(args, fraction, dataset_path)
        run_preprocessor(args, dataset_path, preprocessed_path, result_path)
        result = json.loads(result_path.read_text(encoding="utf-8"))
        return rows_from_result(args, fraction, result)
    finally:
        if not args.keep_data:
            remove_path(dataset_path)
            remove_path(dataset_path.with_suffix(dataset_path.suffix + ".json"))
            remove_path(preprocessed_path)


def generate_dataset(args: argparse.Namespace, fraction: float, output: Path) -> None:
    command = [
        sys.executable,
        str(repo_root() / "experiments" / "generate_heavy_key.py"),
        "--output",
        str(output),
        "--rows",
        str(args.rows),
        "--seed",
        str(args.seed),
        "--part-rows",
        str(args.part_rows),
        "--key-cardinality",
        str(args.key_cardinality),
        "--payload-columns",
        str(args.payload_columns),
        "--heavy-fraction",
        str(fraction),
    ]
    run(command)


def run_preprocessor(
    args: argparse.Namespace,
    input_path: Path,
    output_path: Path,
    result_path: Path,
) -> None:
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
        "group_by",
        "--min-partitions",
        str(args.partitions),
        "--max-partitions",
        str(args.partitions),
        "--target-partition-size-mb",
        str(args.target_partition_size_mb),
        "--local-threads",
        str(args.local_threads),
        "--seed",
        str(args.seed),
        "--input-metadata",
        str(input_path.with_suffix(input_path.suffix + ".json")),
    ]
    if args.release:
        command.append("--release")
    run(command)


def rows_from_result(args: argparse.Namespace, fraction: float, result: dict) -> list[dict]:
    row_count = int(result.get("rows") or args.rows)
    partition_count = int(result.get("output_partitions") or args.partitions)
    ideal_rows = row_count / partition_count
    before_skew = result.get("before_skew") or {}
    after_skew = result.get("after_skew") or before_skew
    return [
        metric_row(
            fraction,
            row_count,
            partition_count,
            ideal_rows,
            "baseline",
            "Распределение записей по значению хэш-функции",
            before_skew,
        ),
        metric_row(
            fraction,
            row_count,
            partition_count,
            ideal_rows,
            "repartitioner",
            "После обработки методом",
            after_skew,
        ),
    ]


def metric_row(
    fraction: float,
    rows: int,
    partition_count: int,
    ideal_rows: float,
    variant: str,
    variant_label: str,
    skew: dict,
) -> dict:
    max_rows = float(skew.get("max_partition_size") or 0.0)
    p95_rows = float(skew.get("p95_partition_size") or 0.0)
    return {
        "heavy_fraction": fraction,
        "heavy_percent": fraction * 100.0,
        "rows": rows,
        "partitions": partition_count,
        "variant": variant,
        "variant_label": variant_label,
        "ideal_partition_rows": ideal_rows,
        "max_partition_rows": max_rows,
        "p95_partition_rows": p95_rows,
        "max_over_ideal_rows": max_rows / ideal_rows if ideal_rows > 0 else 0.0,
        "p95_over_ideal_rows": p95_rows / ideal_rows if ideal_rows > 0 else 0.0,
        "coefficient_of_variation": skew.get("coefficient_of_variation"),
        "rho": skew.get("max_mean_imbalance_ratio"),
        "max_mean_ratio": skew.get("max_mean_imbalance_ratio"),
    }


def write_summary(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "heavy_fraction",
        "heavy_percent",
        "rows",
        "partitions",
        "variant",
        "variant_label",
        "ideal_partition_rows",
        "max_partition_rows",
        "p95_partition_rows",
        "max_over_ideal_rows",
        "p95_over_ideal_rows",
        "coefficient_of_variation",
        "rho",
        "max_mean_ratio",
    ]
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def plot_summary(summary_path: Path, output_path: Path) -> None:
    import matplotlib.pyplot as plt

    with summary_path.open("r", encoding="utf-8", newline="") as file:
        rows = list(csv.DictReader(file))

    styles = {
        "baseline": {
            "color": "#1f77b4",
            "linestyle": "-",
            "marker": "o",
            "markerfacecolor": "white",
            "markeredgecolor": "#1f77b4",
        },
        "repartitioner": {
            "color": "#d62728",
            "linestyle": "--",
            "marker": "s",
            "markerfacecolor": "white",
            "markeredgecolor": "#d62728",
        },
    }
    labels = {
        "baseline": "Распределение записей по значению хэш-функции",
        "repartitioner": "После обработки методом",
    }

    plt.figure(figsize=(8, 5))
    for variant in ["baseline", "repartitioner"]:
        points = sorted(
            (
                (float(row["heavy_percent"]), float(row.get("rho") or row["tau"]))
                for row in rows
                if row["variant"] == variant
            ),
            key=lambda item: item[0],
        )
        if not points:
            continue
        xs, ys = zip(*points)
        plt.plot(
            xs,
            ys,
            label=labels[variant],
            linewidth=2.0,
            markersize=5.5,
            markeredgewidth=1.2,
            **styles[variant],
        )

    plt.title(
        "Зависимость равномерности разбиения набора данных "
        "от количества записей с тяжёлым ключом",
        wrap=True,
    )
    plt.xlabel("Доля записей с тяжёлым ключом, %")
    plt.ylabel("ρ")
    plt.grid(True, color="0.75", linestyle=":", linewidth=0.8)
    plt.legend()
    plt.tight_layout(rect=(0, 0, 1, 0.94))
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path, dpi=160)
    plt.close()


def remove_path(path: Path) -> None:
    if path.is_dir():
        shutil.rmtree(path)
    elif path.exists():
        path.unlink()


def run(command: list[str]) -> None:
    print("+", " ".join(command), flush=True)
    subprocess.run(command, cwd=repo_root(), check=True)


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


if __name__ == "__main__":
    main()
