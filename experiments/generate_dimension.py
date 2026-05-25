#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

try:
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq
except ImportError as exc:
    raise SystemExit(
        "pyarrow is required for dimension generation. Install with: pip install pyarrow"
    ) from exc


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate a one-row-per-key dimension dataset for Spark join benchmarks."
    )
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--key-column", default="user_id")
    args = parser.parse_args()

    table = pq.read_table(args.input, columns=[args.key_column])
    unique_keys = pc.unique(table[args.key_column].combine_chunks())
    output = pa.table(
        {
            args.key_column: unique_keys,
            "dimension_value": pa.array(
                [f"dim_{index:08d}" for index in range(len(unique_keys))],
                type=pa.string(),
            ),
        }
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(output, args.output)
    print(
        {
            "input": str(args.input),
            "output": str(args.output),
            "key_column": args.key_column,
            "rows": len(unique_keys),
        }
    )


if __name__ == "__main__":
    main()
