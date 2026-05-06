#!/usr/bin/env python3
import random

from common import base_generator_parser, print_metadata, uniform_keys, write_dataset


def main() -> None:
    parser = base_generator_parser("Generate a uniform-key Parquet dataset.")
    parser.add_argument(
        "--balanced",
        action="store_true",
        help="Generate exactly balanced key frequencies instead of random uniform sampling.",
    )
    parser.add_argument("--scenario-name", default="uniform")
    args = parser.parse_args()

    if args.balanced:
        user_ids = balanced_uniform_keys(args.rows, args.key_cardinality, args.seed)
    else:
        user_ids = uniform_keys(args.rows, args.key_cardinality, args.seed)
    metadata = write_dataset(args.output, user_ids, args.scenario_name, args.seed)
    metadata["key_cardinality"] = args.key_cardinality
    metadata["balanced"] = args.balanced
    print_metadata(metadata)


def balanced_uniform_keys(rows: int, key_cardinality: int, seed: int) -> list[str]:
    if rows <= 0:
        raise SystemExit("rows must be greater than zero")
    if key_cardinality <= 0:
        raise SystemExit("key_cardinality must be greater than zero")

    keys = [f"user_{index:08d}" for index in range(key_cardinality)]
    values = [keys[index % key_cardinality] for index in range(rows)]
    random.Random(seed).shuffle(values)
    return values


if __name__ == "__main__":
    main()
