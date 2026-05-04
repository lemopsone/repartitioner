import hashlib
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError as exc:
    raise SystemExit(
        "pyarrow is required for dataset generation. Install with: pip install pyarrow"
    ) from exc

from .metadata import OutputFile, build_metadata


DEFAULT_METRIC_COLUMNS = ["value"]


def write_parquet_dataset(
    *,
    output: Path,
    logical_keys: list[str],
    scenario: str,
    seed: int,
    key_columns: list[str],
    metric_columns: list[str],
    categorical_columns: list[str],
    payload_bytes: int,
    files: int,
    compression: str,
    row_group_size: int | None,
    timestamp_column: str | None,
    parameters: dict[str, Any],
    validate: bool,
) -> dict[str, Any]:
    if files <= 0:
        raise ValueError("files must be greater than zero")
    if payload_bytes < 0:
        raise ValueError("payload bytes must not be negative")
    if not key_columns:
        raise ValueError("at least one key column must be configured")

    output_files = output_paths(output, files)
    written_files = []
    start_row = 0
    for file_index, file_path in enumerate(output_files):
        row_count = shard_size(len(logical_keys), files, file_index)
        shard_keys = logical_keys[start_row : start_row + row_count]
        table = build_table(
            logical_keys=shard_keys,
            global_row_offset=start_row,
            seed=seed,
            key_columns=key_columns,
            metric_columns=metric_columns,
            categorical_columns=categorical_columns,
            payload_bytes=payload_bytes,
            timestamp_column=timestamp_column,
        )
        file_path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            table,
            file_path,
            compression=compression,
            row_group_size=row_group_size,
        )
        written_files.append(
            OutputFile(
                path=file_path,
                rows=row_count,
                size_bytes=file_path.stat().st_size,
            )
        )
        start_row += row_count

    schema = schema_description(
        key_columns=key_columns,
        metric_columns=metric_columns,
        categorical_columns=categorical_columns,
        payload_bytes=payload_bytes,
        timestamp_column=timestamp_column,
    )
    metadata = build_metadata(
        scenario=scenario,
        output=output,
        files=written_files,
        rows=len(logical_keys),
        seed=seed,
        key_columns=key_columns,
        key_frequencies=Counter(logical_keys),
        schema=schema,
        parameters=parameters,
    )
    metadata_path = metadata_output_path(output, files)
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    metadata["metadata_path"] = str(metadata_path)

    if validate:
        validate_dataset(written_files, len(logical_keys), key_columns)

    return metadata


def build_table(
    *,
    logical_keys: list[str],
    global_row_offset: int,
    seed: int,
    key_columns: list[str],
    metric_columns: list[str],
    categorical_columns: list[str],
    payload_bytes: int,
    timestamp_column: str | None,
) -> pa.Table:
    row_ids = list(range(global_row_offset, global_row_offset + len(logical_keys)))
    columns: dict[str, pa.Array] = {}

    key_values = [expand_key(logical_key, key_columns) for logical_key in logical_keys]
    for column in key_columns:
        columns[column] = pa.array([values[column] for values in key_values], type=pa.string())

    columns["row_id"] = pa.array(row_ids, type=pa.int64())
    for metric in metric_columns:
        columns[metric] = pa.array(
            [
                stable_int(seed, row_id, logical_key, metric, modulo=1_000_000)
                for row_id, logical_key in zip(row_ids, logical_keys)
            ],
            type=pa.int64(),
        )

    for column in categorical_columns:
        columns[column] = pa.array(
            [
                f"{column}_{stable_int(seed, logical_key, column, modulo=128):03d}"
                for logical_key in logical_keys
            ],
            type=pa.string(),
        )

    if payload_bytes > 0:
        columns["payload"] = pa.array(
            [
                deterministic_payload(seed, row_id, logical_key, payload_bytes)
                for row_id, logical_key in zip(row_ids, logical_keys)
            ],
            type=pa.string(),
        )

    if timestamp_column:
        base_ms = int(datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
        columns[timestamp_column] = pa.array(
            [base_ms + row_id * 1000 for row_id in row_ids],
            type=pa.timestamp("ms"),
        )

    return pa.table(columns)


def expand_key(logical_key: str, key_columns: list[str]) -> dict[str, str]:
    if len(key_columns) == 1:
        return {key_columns[0]: logical_key}

    values = {}
    for index, column in enumerate(key_columns):
        if index == len(key_columns) - 1:
            values[column] = logical_key
        else:
            bucket = stable_int(logical_key, column, modulo=1024)
            values[column] = f"{column}_{bucket:04d}"
    return values


def output_paths(output: Path, files: int) -> list[Path]:
    if files == 1 and output.suffix == ".parquet":
        return [output]
    if output.exists() and output.is_file():
        raise ValueError("multi-file output requires an output directory, not a file")
    return [output / f"part-{index:05d}.parquet" for index in range(files)]


def metadata_output_path(output: Path, files: int) -> Path:
    if files == 1 and output.suffix == ".parquet":
        return output.with_suffix(output.suffix + ".json")
    return output / "_dataset_metadata.json"


def shard_size(total_rows: int, files: int, file_index: int) -> int:
    base = total_rows // files
    remainder = total_rows % files
    return base + (1 if file_index < remainder else 0)


def schema_description(
    *,
    key_columns: list[str],
    metric_columns: list[str],
    categorical_columns: list[str],
    payload_bytes: int,
    timestamp_column: str | None,
) -> dict[str, str]:
    schema = {column: "string" for column in key_columns}
    schema["row_id"] = "int64"
    for column in metric_columns:
        schema[column] = "int64"
    for column in categorical_columns:
        schema[column] = "string"
    if payload_bytes > 0:
        schema["payload"] = "string"
    if timestamp_column:
        schema[timestamp_column] = "timestamp_ms"
    return schema


def validate_dataset(files: list[OutputFile], expected_rows: int, key_columns: list[str]) -> None:
    observed_rows = 0
    for file in files:
        table = pq.read_table(file.path, columns=key_columns)
        observed_rows += table.num_rows
        for column in key_columns:
            field = table.schema.field(column)
            if not pa.types.is_string(field.type) and not pa.types.is_large_string(field.type):
                raise ValueError(f"key column {column} is not string-compatible")
    if observed_rows != expected_rows:
        raise ValueError(f"validated {observed_rows} rows, expected {expected_rows}")


def stable_int(*parts: object, modulo: int) -> int:
    digest = hashlib.blake2b(digest_size=8)
    for part in parts:
        digest.update(str(part).encode("utf-8"))
        digest.update(b"\0")
    return int.from_bytes(digest.digest(), "big") % modulo


def deterministic_payload(seed: int, row_id: int, logical_key: str, size: int) -> str:
    chunks = []
    counter = 0
    while sum(len(chunk) for chunk in chunks) < size:
        digest = hashlib.blake2b(
            f"{seed}:{row_id}:{logical_key}:{counter}".encode("utf-8"),
            digest_size=32,
        ).hexdigest()
        chunks.append(digest)
        counter += 1
    return "".join(chunks)[:size]
