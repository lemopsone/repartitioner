#!/usr/bin/env python3
from common import (
    base_generator_parser,
    multi_heavy_key_distribution,
    print_metadata,
    write_dataset,
)


def main() -> None:
    parser = base_generator_parser("Generate a multi-heavy-key Parquet dataset.")
    parser.add_argument("--heavy-keys", type=int, default=4)
    parser.add_argument("--heavy-fraction", type=float, default=0.60)
    args = parser.parse_args()

    heavy_keys = [f"heavy_{index:08d}" for index in range(args.heavy_keys)]
    user_ids = multi_heavy_key_distribution(
        args.rows,
        args.key_cardinality,
        args.seed,
        heavy_keys,
        args.heavy_fraction,
    )
    metadata = write_dataset(
        args.output,
        user_ids,
        "multi_heavy_key",
        args.seed,
        part_rows=args.part_rows,
        payload_columns=args.payload_columns,
    )
    metadata.update(
        {
            "key_cardinality": args.key_cardinality,
            "heavy_keys": heavy_keys,
            "heavy_fraction": args.heavy_fraction,
        }
    )
    print_metadata(metadata)


if __name__ == "__main__":
    main()
