#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Plot research CSV into time and partition-skew graphs per skew/workload."
    )
    parser.add_argument("--summary", required=True, type=Path)
    parser.add_argument("--plots-dir", required=True, type=Path)
    parser.add_argument(
        "--time-column",
        choices=["spark_time_seconds", "total_with_preprocessing_seconds"],
        default="spark_time_seconds",
    )
    args = parser.parse_args()

    import matplotlib.pyplot as plt

    rows = read_rows(args.summary)
    grouped = defaultdict(list)
    for row in rows:
        grouped[(row["skew"], row["workload"])].append(row)

    args.plots_dir.mkdir(parents=True, exist_ok=True)
    for (skew, workload), group_rows in grouped.items():
        for spec in metric_specs(args.time_column):
            title = spec["title"].format(
                workload=workload_label(workload),
                skew=skew_label(skew),
            )
            plot_metric(
                plt,
                group_rows,
                metric=spec["column"],
                ylabel=spec["ylabel"],
                title=title,
                xlabel=spec.get("xlabel", "Rows"),
                x_divisor=float(spec.get("x_divisor", 1.0)),
                output=args.plots_dir / f"{skew}_{workload}_{spec['suffix']}.png",
            )


def metric_specs(time_column: str) -> list[dict[str, str | int]]:
    return [
        {
            "column": time_column,
            "suffix": "time",
            "title": (
                "Зависимость времени выполнения задания от объёма данных, "
                "\nоператор - {workload}, вид перекоса - {skew}"
            ),
            "ylabel": "Время выполнения задания, с.",
            "xlabel": "Объём набора данных, млн строк",
            "x_divisor": 1_000_000,
        },
        {
            "column": "max_partition_rows",
            "suffix": "max_partition_rows",
            "title": "max partition rows",
            "ylabel": "Max partition size, rows",
        },
        {
            "column": "max_partition_bytes_estimated",
            "suffix": "max_partition_bytes",
            "title": "max partition bytes",
            "ylabel": "Estimated max partition size, bytes",
        },
        {
            "column": "p95_partition_rows",
            "suffix": "p95_partition_rows",
            "title": "p95 partition rows",
            "ylabel": "P95 partition size, rows",
        },
        {
            "column": "p95_partition_bytes_estimated",
            "suffix": "p95_partition_bytes",
            "title": "p95 partition bytes",
            "ylabel": "Estimated P95 partition size, bytes",
        },
        {
            "column": "coefficient_of_variation",
            "suffix": "cv",
            "title": "coefficient of variation",
            "ylabel": "Coefficient of variation",
        },
        {
            "column": "skew_reduction_factor",
            "suffix": "skew_reduction_factor",
            "title": "skew reduction factor",
            "ylabel": "Before max / current max",
        },
        {
            "column": "skew_remaining_ratio",
            "suffix": "skew_remaining_ratio",
            "title": "skew remaining ratio",
            "ylabel": "Current max / baseline max",
        },
        {
            "column": "largest_partition_share",
            "suffix": "largest_partition_share",
            "title": "largest partition share",
            "ylabel": "Largest partition / total rows",
        },
        {
            "column": "max_minus_mean_partition_rows",
            "suffix": "max_minus_mean_rows",
            "title": "max minus mean partition rows",
            "ylabel": "Max - mean partition size, rows",
        },
        {
            "column": "max_over_target_partition_rows",
            "suffix": "max_over_target_rows",
            "title": "max over target rows",
            "ylabel": "Max partition / target partition rows",
        },
        {
            "column": "tau",
            "suffix": "tau",
            "title": "rho",
            "ylabel": "ρ",
        },
    ]


def workload_label(workload: str) -> str:
    return workload


def skew_label(skew: str) -> str:
    labels = {
        "uniform": "равномерное распределение",
        "heavy_key": "один тяжёлый ключ",
        "multi_heavy_key": "5 тяжёлых ключей",
        "zipf": "длинный хвост частот",
    }
    return labels.get(skew, skew)


def read_rows(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8", newline="") as file:
        return list(csv.DictReader(file))


def plot_metric(
    plt,
    rows: list[dict],
    *,
    metric: str,
    ylabel: str,
    title: str,
    xlabel: str,
    x_divisor: float,
    output: Path,
) -> None:
    series = []
    for variant in ["baseline", "repartitioner"]:
        points = sorted(
            (
                (int(row["rows"]) / x_divisor, float(row[metric]))
                for row in rows
                if row["variant"] == variant and parseable_float(row.get(metric))
            ),
            key=lambda item: item[0],
        )
        if not points:
            continue
        series.append((variant, points))

    if not series:
        return

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
        "baseline": "Без предварительной обработки",
        "repartitioner": "После обработки методом",
    }

    plt.figure(figsize=(8, 5))
    for variant, points in series:
        xs, ys = zip(*points)
        plt.plot(
            xs,
            ys,
            label=labels.get(variant, variant),
            linewidth=2.0,
            markersize=5.5,
            markeredgewidth=1.2,
            **styles.get(variant, {}),
        )

    plt.title(title, wrap=True)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.grid(True, color="0.75", linestyle=":", linewidth=0.8)
    plt.legend()
    plt.tight_layout(rect=(0, 0, 1, 0.94))
    plt.savefig(output, dpi=160)
    plt.close()


def parseable_float(value) -> bool:
    if value in {"", None}:
        return False
    try:
        float(value)
        return True
    except (TypeError, ValueError):
        return False


if __name__ == "__main__":
    main()
