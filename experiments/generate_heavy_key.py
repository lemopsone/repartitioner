#!/usr/bin/env python3
from common import (
    base_generator_parser,
    heavy_key_distribution,
    print_metadata,
    write_dataset,
)


def main() -> None:
    parser = base_generator_parser("Generate a one-heavy-key Parquet dataset.")
    parser.add_argument("--heavy-key", default="heavy_00000000")
    parser.add_argument("--heavy-fraction", type=float, default=0.50)
    args = parser.parse_args()

    user_ids = heavy_key_distribution(
        args.rows,
        args.key_cardinality,
        args.seed,
        args.heavy_key,
        args.heavy_fraction,
    )
    metadata = write_dataset(args.output, user_ids, "heavy_key", args.seed)
    metadata.update(
        {
            "key_cardinality": args.key_cardinality,
            "heavy_key": args.heavy_key,
            "heavy_fraction": args.heavy_fraction,
        }
    )
    print_metadata(metadata)


if __name__ == "__main__":
    main()
