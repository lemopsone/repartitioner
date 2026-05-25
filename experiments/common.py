from __future__ import annotations

import argparse
import bisect
import json
import random
import shutil
from pathlib import Path
from typing import Iterable, Iterator, Sequence

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError as exc:
    raise SystemExit(
        "pyarrow is required for dataset generation. Install with: pip install pyarrow"
    ) from exc


def base_generator_parser(description: str) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--output", required=True, type=Path, help="Output Parquet dataset path.")
    parser.add_argument("--rows", type=int, default=100_000, help="Number of rows to generate.")
    parser.add_argument("--seed", type=int, default=42, help="Random seed.")
    parser.add_argument(
        "--part-rows",
        type=int,
        default=1_000_000,
        help="Rows per generated Parquet part file.",
    )
    parser.add_argument(
        "--key-cardinality",
        type=int,
        default=10_000,
        help="Number of distinct non-heavy keys.",
    )
    parser.add_argument(
        "--payload-columns",
        type=int,
        default=8,
        help="Number of additional int64 payload columns per row.",
    )
    return parser


def write_dataset(
    output: Path,
    user_ids: Iterable[str],
    scenario: str,
    seed: int,
    *,
    part_rows: int = 1_000_000,
    payload_columns: int = 8,
) -> dict:
    if part_rows <= 0:
        raise SystemExit("part_rows must be greater than zero")
    if payload_columns < 0:
        raise SystemExit("payload_columns must be greater than or equal to zero")

    if output.exists():
        if output.is_dir():
            shutil.rmtree(output)
        else:
            output.unlink()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.mkdir(parents=True, exist_ok=True)

    row_count = 0
    part_index = 0
    distinct_keys: set[str] = set()
    chunk: list[str] = []
    for user_id in user_ids:
        chunk.append(user_id)
        distinct_keys.add(user_id)
        if len(chunk) >= part_rows:
            write_part(output, part_index, row_count, chunk, seed, payload_columns)
            row_count += len(chunk)
            part_index += 1
            chunk = []

    if chunk:
        write_part(output, part_index, row_count, chunk, seed, payload_columns)
        row_count += len(chunk)

    metadata = {
        "scenario": scenario,
        "rows": row_count,
        "output": str(output),
        "seed": seed,
        "part_rows": part_rows,
        "part_files": part_index + (1 if chunk else 0),
        "distinct_keys": len(distinct_keys),
        "payload_columns": payload_columns,
    }
    metadata_path = output.with_suffix(output.suffix + ".json")
    metadata_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    return metadata


def write_part(
    output: Path,
    part_index: int,
    start_row_id: int,
    user_ids: Sequence[str],
    seed: int,
    payload_columns: int,
) -> None:
    row_ids = list(range(start_row_id, start_row_id + len(user_ids)))
    values = [stable_value(user_id, row_id, seed) for row_id, user_id in zip(row_ids, user_ids)]
    columns = {
        "user_id": pa.array(user_ids, type=pa.string()),
        "row_id": pa.array(row_ids, type=pa.int64()),
        "value": pa.array(values, type=pa.int64()),
    }
    for payload_index in range(payload_columns):
        columns[f"payload_{payload_index}"] = pa.array(
            [
                stable_payload_value(user_id, row_id, seed, payload_index)
                for row_id, user_id in zip(row_ids, user_ids)
            ],
            type=pa.int64(),
        )
    table = pa.table(columns)
    pq.write_table(table, output / f"part-{part_index:05}.parquet")


def uniform_keys(rows: int, key_cardinality: int, seed: int) -> Iterator[str]:
    validate_positive(rows=rows, key_cardinality=key_cardinality)
    rng = random.Random(seed)
    keys = [f"user_{index:08d}" for index in range(key_cardinality)]
    for _ in range(rows):
        yield rng.choice(keys)


def heavy_key_distribution(
    rows: int,
    key_cardinality: int,
    seed: int,
    heavy_key: str,
    heavy_fraction: float,
) -> Iterator[str]:
    validate_positive(rows=rows, key_cardinality=key_cardinality)
    validate_fraction("heavy_fraction", heavy_fraction)
    rng = random.Random(seed)
    normal_keys = [f"user_{index:08d}" for index in range(key_cardinality)]
    remaining_heavy = round(rows * heavy_fraction)
    for row_index in range(rows):
        remaining_rows = rows - row_index
        if remaining_heavy > 0 and rng.random() < remaining_heavy / remaining_rows:
            remaining_heavy -= 1
            yield heavy_key
        else:
            yield rng.choice(normal_keys)


def multi_heavy_key_distribution(
    rows: int,
    key_cardinality: int,
    seed: int,
    heavy_keys: Sequence[str],
    heavy_fraction: float,
) -> Iterator[str]:
    validate_positive(rows=rows, key_cardinality=key_cardinality)
    validate_fraction("heavy_fraction", heavy_fraction)
    if not heavy_keys:
        raise SystemExit("heavy_keys must not be empty")

    rng = random.Random(seed)
    normal_keys = [f"user_{index:08d}" for index in range(key_cardinality)]
    remaining_heavy = round(rows * heavy_fraction)
    heavy_index = 0
    for row_index in range(rows):
        remaining_rows = rows - row_index
        if remaining_heavy > 0 and rng.random() < remaining_heavy / remaining_rows:
            remaining_heavy -= 1
            heavy_key = heavy_keys[heavy_index % len(heavy_keys)]
            heavy_index += 1
            yield heavy_key
        else:
            yield rng.choice(normal_keys)


def zipf_keys(rows: int, key_cardinality: int, seed: int, exponent: float) -> Iterator[str]:
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
    for _ in range(rows):
        pick = rng.random() * total
        yield keys[bisect.bisect_left(cumulative, pick)]


def print_metadata(metadata: dict) -> None:
    print(json.dumps(metadata, indent=2))


def stable_value(user_id: str, row_id: int, seed: int) -> int:
    value = seed ^ row_id
    for byte in user_id.encode("utf-8"):
        value = ((value * 131) ^ byte) & 0x7FFF_FFFF
    return value


def stable_payload_value(user_id: str, row_id: int, seed: int, payload_index: int) -> int:
    value = stable_value(user_id, row_id, seed + payload_index * 1_000_003)
    value ^= (row_id * 1_099_511_627_761) & 0x7FFF_FFFF
    value ^= (payload_index + 1) * 2_654_435_761
    return value & 0x7FFF_FFFF


def validate_positive(**values: int) -> None:
    for name, value in values.items():
        if value <= 0:
            raise SystemExit(f"{name} must be greater than zero")


def validate_fraction(name: str, value: float) -> None:
    if not 0.0 <= value <= 1.0:
        raise SystemExit(f"{name} must be between 0.0 and 1.0")
