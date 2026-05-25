#!/usr/bin/env python3
from common import base_generator_parser, print_metadata, write_dataset, zipf_keys


def main() -> None:
    parser = base_generator_parser("Generate a Zipf-distributed Parquet dataset.")
    parser.add_argument("--zipf-exponent", type=float, default=1.2)
    args = parser.parse_args()

    user_ids = zipf_keys(args.rows, args.key_cardinality, args.seed, args.zipf_exponent)
    metadata = write_dataset(
        args.output,
        user_ids,
        "zipf",
        args.seed,
        part_rows=args.part_rows,
        payload_columns=args.payload_columns,
    )
    metadata.update(
        {
            "key_cardinality": args.key_cardinality,
            "zipf_exponent": args.zipf_exponent,
        }
    )
    print_metadata(metadata)


if __name__ == "__main__":
    main()
