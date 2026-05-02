from __future__ import annotations

import argparse
import bisect
import json
import random
from pathlib import Path
from typing import Iterable, Sequence

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError as exc:
    raise SystemExit(
        "pyarrow is required for dataset generation. Install with: pip install pyarrow"
    ) from exc


def base_generator_parser(description: str) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--output", required=True, type=Path, help="Output Parquet file path.")
    parser.add_argument("--rows", type=int, default=100_000, help="Number of rows to generate.")
    parser.add_argument("--seed", type=int, default=42, help="Random seed.")
    parser.add_argument(
        "--key-cardinality",
        type=int,
        default=10_000,
        help="Number of distinct non-heavy keys.",
    )
    return parser


def write_dataset(output: Path, user_ids: Iterable[str], scenario: str, seed: int) -> dict:
    output.parent.mkdir(parents=True, exist_ok=True)
    user_ids = list(user_ids)
    row_ids = list(range(len(user_ids)))
    values = [stable_value(user_id, row_id, seed) for row_id, user_id in enumerate(user_ids)]

    table = pa.table(
        {
            "user_id": pa.array(user_ids, type=pa.string()),
            "row_id": pa.array(row_ids, type=pa.int64()),
            "value": pa.array(values, type=pa.int64()),
        }
    )
    pq.write_table(table, output)

    metadata = {
        "scenario": scenario,
        "rows": len(user_ids),
        "output": str(output),
        "seed": seed,
        "distinct_keys": len(set(user_ids)),
    }
    metadata_path = output.with_suffix(output.suffix + ".json")
    metadata_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    return metadata


def uniform_keys(rows: int, key_cardinality: int, seed: int) -> list[str]:
    validate_positive(rows=rows, key_cardinality=key_cardinality)
    rng = random.Random(seed)
    keys = [f"user_{index:08d}" for index in range(key_cardinality)]
    return [rng.choice(keys) for _ in range(rows)]


def heavy_key_distribution(
    rows: int,
    key_cardinality: int,
    seed: int,
    heavy_key: str,
    heavy_fraction: float,
) -> list[str]:
    validate_positive(rows=rows, key_cardinality=key_cardinality)
    validate_fraction("heavy_fraction", heavy_fraction)
    rng = random.Random(seed)
    normal_keys = [f"user_{index:08d}" for index in range(key_cardinality)]
    heavy_rows = round(rows * heavy_fraction)
    user_ids = [heavy_key] * heavy_rows
    user_ids.extend(rng.choice(normal_keys) for _ in range(rows - heavy_rows))
    rng.shuffle(user_ids)
    return user_ids


def multi_heavy_key_distribution(
    rows: int,
    key_cardinality: int,
    seed: int,
    heavy_keys: Sequence[str],
    heavy_fraction: float,
) -> list[str]:
    validate_positive(rows=rows, key_cardinality=key_cardinality)
    validate_fraction("heavy_fraction", heavy_fraction)
    if not heavy_keys:
        raise SystemExit("heavy_keys must not be empty")

    rng = random.Random(seed)
    normal_keys = [f"user_{index:08d}" for index in range(key_cardinality)]
    total_heavy_rows = round(rows * heavy_fraction)
    user_ids: list[str] = []
    for index in range(total_heavy_rows):
        user_ids.append(heavy_keys[index % len(heavy_keys)])
    user_ids.extend(rng.choice(normal_keys) for _ in range(rows - total_heavy_rows))
    rng.shuffle(user_ids)
    return user_ids


def zipf_keys(rows: int, key_cardinality: int, seed: int, exponent: float) -> list[str]:
    validate_positive(rows=rows, key_cardinality=key_cardinality)
    if exponent <= 0:
        raise SystemExit("zipf exponent must be greater than zero")

    rng = random.Random(seed)
    weights = [1.0 / (rank**exponent) for rank in range(1, key_cardinality + 1)]
    cumulative: list[float] = []
    total = 0.0
    for weight in weights:
        total += weight
        cumulative.append(total)

    keys = [f"user_{index:08d}" for index in range(key_cardinality)]
    user_ids = []
    for _ in range(rows):
        pick = rng.random() * total
        user_ids.append(keys[bisect.bisect_left(cumulative, pick)])
    return user_ids


def print_metadata(metadata: dict) -> None:
    print(json.dumps(metadata, indent=2))


def stable_value(user_id: str, row_id: int, seed: int) -> int:
    value = seed ^ row_id
    for byte in user_id.encode("utf-8"):
        value = ((value * 131) ^ byte) & 0x7FFF_FFFF
    return value


def validate_positive(**values: int) -> None:
    for name, value in values.items():
        if value <= 0:
            raise SystemExit(f"{name} must be greater than zero")


def validate_fraction(name: str, value: float) -> None:
    if not 0.0 <= value <= 1.0:
        raise SystemExit(f"{name} must be between 0.0 and 1.0")
