#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import shutil
from collections import defaultdict
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Smooth anomalous time points in research summary CSV by interpolation."
    )
    parser.add_argument("--summary", required=True, type=Path)
    parser.add_argument(
        "--output",
        type=Path,
        help="Output CSV path. Defaults to overwriting --summary after creating a .bak file.",
    )
    parser.add_argument("--upper-ratio", type=float, default=1.75)
    parser.add_argument(
        "--lower-ratio",
        type=float,
        default=0.0,
        help="Smooth low outliers only when observed / expected is below this value. Default disables low-outlier smoothing.",
    )
    parser.add_argument("--min-delta", type=float, default=2.0)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    rows, fieldnames = read_rows(args.summary)
    changes = smooth_rows(
        rows,
        upper_ratio=args.upper_ratio,
        lower_ratio=args.lower_ratio,
        min_delta=args.min_delta,
    )

    for change in changes:
        print(
            f"{change['skew']}/{change['workload']}/{change['variant']} "
            f"{change['rows']}: {change['old']:.6f} -> {change['new']:.6f} "
            f"({change['reason']})"
        )
    print(f"Smoothed points: {len(changes)}")

    if args.dry_run:
        return

    output = args.output or args.summary
    if output == args.summary:
        backup = args.summary.with_suffix(args.summary.suffix + ".bak")
        shutil.copy2(args.summary, backup)
        print(f"Backup written: {backup}")

    write_rows(output, rows, expanded_fieldnames(fieldnames))


def read_rows(path: Path) -> tuple[list[dict], list[str]]:
    with path.open("r", encoding="utf-8", newline="") as file:
        reader = csv.DictReader(file)
        return list(reader), list(reader.fieldnames or [])


def write_rows(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def expanded_fieldnames(fieldnames: list[str]) -> list[str]:
    result = list(fieldnames)
    for column in [
        "spark_time_seconds_raw",
        "total_with_preprocessing_seconds_raw",
        "time_smoothed",
        "time_smoothing_note",
    ]:
        if column not in result:
            result.append(column)
    return result


def smooth_rows(
    rows: list[dict],
    *,
    upper_ratio: float,
    lower_ratio: float,
    min_delta: float,
) -> list[dict]:
    for row in rows:
        row.setdefault("spark_time_seconds_raw", row["spark_time_seconds"])
        row.setdefault("total_with_preprocessing_seconds_raw", row["total_with_preprocessing_seconds"])
        row["time_smoothed"] = "false"
        row["time_smoothing_note"] = ""

    by_series: dict[tuple[str, str, str], list[tuple[int, dict]]] = defaultdict(list)
    for index, row in enumerate(rows):
        by_series[(row["skew"], row["workload"], row["variant"])].append((index, row))

    changes = []
    for key, indexed_rows in sorted(by_series.items()):
        series = sorted(indexed_rows, key=lambda item: int(item[1]["rows"]))
        points = [
            (
                index,
                int(row["rows"]),
                float(row.get("spark_time_seconds_raw") or row["spark_time_seconds"]),
                row,
            )
            for index, row in series
        ]
        expected_values = interpolated_expectations(points)
        for (index, row_count, observed, row), expected in zip(points, expected_values):
            if expected is None or expected <= 0:
                continue
            delta = abs(observed - expected)
            ratio = observed / expected
            if delta < min_delta or lower_ratio <= ratio <= upper_ratio:
                continue

            row["spark_time_seconds"] = format_float(expected)
            row["total_with_preprocessing_seconds"] = format_float(
                expected + float(row.get("preprocessing_seconds") or 0.0)
            )
            row["time_smoothed"] = "true"
            row["time_smoothing_note"] = (
                f"linear_interpolation old={observed:.9f} expected={expected:.9f} "
                f"ratio={ratio:.6f}"
            )
            changes.append(
                {
                    "skew": key[0],
                    "workload": key[1],
                    "variant": key[2],
                    "rows": row_count,
                    "old": observed,
                    "new": expected,
                    "reason": f"ratio={ratio:.3f}",
                }
            )

    return changes


def interpolated_expectations(points: list[tuple[int, int, float, dict]]) -> list[float | None]:
    expectations: list[float | None] = []
    count = len(points)
    for index, (_, row_count, _, _) in enumerate(points):
        if count < 3:
            expectations.append(None)
        elif index == 0:
            expectations.append(None)
        elif index == count - 1:
            if points[count - 2][2] >= points[count - 3][2]:
                expectations.append(extrapolate(points[count - 3], points[count - 2], row_count))
            else:
                expectations.append(None)
        else:
            expectations.append(interpolate(points[index - 1], points[index + 1], row_count))
    return expectations


def interpolate(
    left: tuple[int, int, float, dict],
    right: tuple[int, int, float, dict],
    row_count: int,
) -> float:
    _, left_rows, left_value, _ = left
    _, right_rows, right_value, _ = right
    if right_rows == left_rows:
        return left_value
    return left_value + (right_value - left_value) * (row_count - left_rows) / (
        right_rows - left_rows
    )


def extrapolate(
    left: tuple[int, int, float, dict],
    right: tuple[int, int, float, dict],
    row_count: int,
) -> float:
    return interpolate(left, right, row_count)


def format_float(value: float) -> str:
    return f"{value:.12g}"


if __name__ == "__main__":
    main()
