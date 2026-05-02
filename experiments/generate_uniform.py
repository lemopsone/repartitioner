#!/usr/bin/env python3
from common import base_generator_parser, print_metadata, uniform_keys, write_dataset


def main() -> None:
    parser = base_generator_parser("Generate a uniform-key Parquet dataset.")
    args = parser.parse_args()

    user_ids = uniform_keys(args.rows, args.key_cardinality, args.seed)
    metadata = write_dataset(args.output, user_ids, "uniform", args.seed)
    metadata["key_cardinality"] = args.key_cardinality
    print_metadata(metadata)


if __name__ == "__main__":
    main()
