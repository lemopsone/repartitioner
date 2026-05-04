from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from statistics import mean
from typing import Any


@dataclass(frozen=True)
class OutputFile:
    path: Path
    rows: int
    size_bytes: int


def build_metadata(
    *,
    scenario: str,
    output: Path,
    files: list[OutputFile],
    rows: int,
    seed: int,
    key_columns: list[str],
    key_frequencies: Counter[str],
    schema: dict[str, str],
    parameters: dict[str, Any],
) -> dict[str, Any]:
    counts = list(key_frequencies.values())
    skew = key_skew_summary(counts)
    return {
        "version": "datasets-suite-v1",
        "scenario": scenario,
        "format": "parquet",
        "output": str(output),
        "seed": seed,
        "rows": rows,
        "files": [
            {
                "path": str(file.path),
                "rows": file.rows,
                "size_bytes": file.size_bytes,
            }
            for file in files
        ],
        "key_columns": key_columns,
        "schema": schema,
        "distribution": {
            "distinct_keys": len(key_frequencies),
            "max_key_frequency": max(counts) if counts else 0,
            "mean_key_frequency": mean(counts) if counts else 0.0,
            "max_mean_key_imbalance_ratio": skew["max_mean_key_imbalance_ratio"],
            "coefficient_of_variation": skew["coefficient_of_variation"],
            "top_keys": [
                {"key": key, "frequency": frequency}
                for key, frequency in key_frequencies.most_common(20)
            ],
        },
        "parameters": parameters,
    }


def key_skew_summary(counts: list[int]) -> dict[str, float]:
    if not counts:
        return {
            "max_mean_key_imbalance_ratio": 0.0,
            "coefficient_of_variation": 0.0,
        }

    average = mean(counts)
    variance = sum((count - average) ** 2 for count in counts) / len(counts)
    return {
        "max_mean_key_imbalance_ratio": max(counts) / average if average else 0.0,
        "coefficient_of_variation": variance**0.5 / average if average else 0.0,
    }
