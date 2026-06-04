#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from pathlib import Path


SKEWS = [
    ("uniform", "Равномерное распределение"),
    ("heavy_key", "Один тяжёлый ключ"),
    ("multi_heavy_key", "Пять тяжёлых ключей"),
    ("zipf", "Хвост частот"),
]

WORKLOAD_LABELS = {
    "scan": "scan",
    "group_by": "group_by",
    "join": "join",
}

LEGEND_LABELS = {
    "БПО": "Без предварительной обработки",
    "МАП": "После обработки методом",
}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build RPZ graphs from prepared CSV tables."
    )
    parser.add_argument("--tables-dir", type=Path, default=Path("reports/rpz"))
    parser.add_argument("--plots-dir", type=Path, default=Path("reports/rpz/plots"))
    args = parser.parse_args()

    import matplotlib.pyplot as plt

    plt.rcParams.update(
        {
            "font.family": "DejaVu Sans",
            "axes.grid": True,
            "grid.alpha": 0.30,
            "figure.dpi": 140,
            "savefig.dpi": 300,
        }
    )

    args.plots_dir.mkdir(parents=True, exist_ok=True)
    for workload in WORKLOAD_LABELS:
        table_path = args.tables_dir / f"time_table_{workload}.csv"
        plot_time_table(plt, table_path, args.plots_dir, workload)

    quality_path = args.tables_dir / "quality_rho_table.csv"
    plot_quality_table(plt, quality_path, args.plots_dir / "quality_rho.png")


def plot_time_table(plt, table_path: Path, plots_dir: Path, workload: str) -> None:
    rows = read_csv(table_path)
    data_rows = rows[2:]
    x_values = [float(row[0]) for row in data_rows if row and row[0]]

    for index, (skew_id, skew_label) in enumerate(SKEWS):
        baseline_column = 1 + index * 2
        method_column = baseline_column + 1
        baseline = [float(row[baseline_column]) for row in data_rows]
        method = [float(row[method_column]) for row in data_rows]

        fig, ax = plt.subplots(figsize=(8.4, 5.0))
        ax.plot(
            x_values,
            baseline,
            color="#1f77b4",
            linestyle="-",
            marker="o",
            linewidth=1.8,
            markersize=4.5,
            label=LEGEND_LABELS["БПО"],
        )
        ax.plot(
            x_values,
            method,
            color="#d62728",
            linestyle="--",
            marker="s",
            linewidth=1.8,
            markersize=4.5,
            label=LEGEND_LABELS["МАП"],
        )
        ax.set_title(
            "Зависимость времени выполнения задания от объёма данных,\n"
            + f"оператор - {WORKLOAD_LABELS[workload]}, вид перекоса - {skew_label}",
            pad=14,
        )
        ax.set_xlabel("Объём набора данных, млн строк")
        ax.set_ylabel("Время выполнения задания, с.")
        ax.set_xticks(x_values)
        ax.legend()
        fig.tight_layout()
        output = plots_dir / f"{skew_id}_{workload}_time.png"
        fig.savefig(output)
        plt.close(fig)


def plot_quality_table(plt, table_path: Path, output_path: Path) -> None:
    rows = read_csv(table_path)
    data_rows = rows[2:]
    x_values = [float(row[0]) for row in data_rows if row and row[0]]
    baseline = [float(row[1]) for row in data_rows]
    method = [float(row[2]) for row in data_rows]

    fig, ax = plt.subplots(figsize=(8.4, 5.0))
    ax.plot(
        x_values,
        baseline,
        color="#1f77b4",
        linestyle="-",
        marker="o",
        linewidth=1.8,
        markersize=4.5,
        label="Распределение записей по значению хэш-функции",
    )
    ax.plot(
        x_values,
        method,
        color="#d62728",
        linestyle="--",
        marker="s",
        linewidth=1.8,
        markersize=4.5,
        label="После обработки методом",
    )
    ax.set_title(
        "Зависимость равномерности разбиения набора данных\n"
        + "от количества записей с тяжёлым ключом",
        pad=14,
    )
    ax.set_xlabel("Доля записей с тяжёлым ключом, %")
    ax.set_ylabel("ρ")
    ax.set_xticks(x_values)
    ax.legend()
    fig.tight_layout()
    fig.savefig(output_path)
    plt.close(fig)


def read_csv(path: Path) -> list[list[str]]:
    with path.open("r", encoding="utf-8", newline="") as file:
        return list(csv.reader(file))


if __name__ == "__main__":
    main()
