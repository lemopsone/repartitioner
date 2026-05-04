import argparse
import json
from pathlib import Path

from . import distributions as dist


DEFAULT_METRIC_COLUMNS = ["value"]


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    try:
        metadata = run(args)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    print(json.dumps(metadata, indent=2))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate deterministic Parquet datasets for skew/repartitioning tests."
    )
    subparsers = parser.add_subparsers(dest="scenario", required=True)

    add_common_options(
        subparsers.add_parser("uniform", help="Balanced key-frequency dataset.")
    )

    single = subparsers.add_parser("single-heavy", help="Dataset with one dominant key.")
    add_common_options(single)
    single.add_argument("--heavy-key", default="heavy_00000000")
    single.add_argument("--heavy-fraction", type=float, default=0.50)
    add_tail_options(single)

    multi = subparsers.add_parser("multi-heavy", help="Dataset with several dominant keys.")
    add_common_options(multi)
    multi.add_argument("--heavy-keys", type=int, default=4)
    multi.add_argument("--heavy-fraction", type=float, default=0.60)
    multi.add_argument(
        "--heavy-weights",
        help="Comma-separated relative weights for heavy keys, for example 5,3,1,1.",
    )
    add_tail_options(multi)

    zipf = subparsers.add_parser("zipf", help="Power-law key-frequency dataset.")
    add_common_options(zipf)
    zipf.add_argument("--zipf-exponent", type=float, default=1.20)

    custom = subparsers.add_parser(
        "custom-heavy",
        help="Dataset with explicit KEY:FRACTION heavy-key specifications.",
    )
    add_common_options(custom)
    custom.add_argument(
        "--heavy-spec",
        required=True,
        help="Comma-separated KEY:FRACTION entries, for example hot_a:0.35,hot_b:0.15.",
    )
    add_tail_options(custom)

    group_by = subparsers.add_parser(
        "group-by",
        help="Group-by oriented dataset with aggregation payload columns.",
    )
    add_common_options(group_by, default_key_columns="group_key")
    group_by.add_argument(
        "--group-distribution",
        choices=["uniform", "single-heavy", "multi-heavy", "zipf"],
        default="zipf",
    )
    group_by.add_argument("--heavy-fraction", type=float, default=0.50)
    group_by.add_argument("--heavy-keys", type=int, default=4)
    group_by.add_argument("--zipf-exponent", type=float, default=1.20)

    join_pair = subparsers.add_parser(
        "join-pair",
        help="Join-oriented left/right Parquet files with matching skewed join keys.",
    )
    add_common_options(join_pair, default_key_columns="join_key")
    join_pair.add_argument("--right-rows", type=int)
    join_pair.add_argument(
        "--join-distribution",
        choices=["uniform", "single-heavy", "multi-heavy", "zipf"],
        default="single-heavy",
    )
    join_pair.add_argument("--heavy-fraction", type=float, default=0.45)
    join_pair.add_argument("--right-heavy-fraction", type=float)
    join_pair.add_argument("--heavy-keys", type=int, default=3)
    join_pair.add_argument("--zipf-exponent", type=float, default=1.20)

    return parser


def add_common_options(parser: argparse.ArgumentParser, default_key_columns: str = "user_id") -> None:
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--key-cardinality", type=int, default=10_000)
    parser.add_argument(
        "--key-columns",
        default=default_key_columns,
        help="Comma-separated string key columns. Rust core currently requires string keys.",
    )
    parser.add_argument("--files", type=int, default=1)
    parser.add_argument("--compression", default="snappy")
    parser.add_argument("--row-group-size", type=int)
    parser.add_argument(
        "--metric-columns",
        default=",".join(DEFAULT_METRIC_COLUMNS),
        help="Comma-separated int64 metric columns.",
    )
    parser.add_argument(
        "--categorical-columns",
        default="",
        help="Comma-separated deterministic string dimension columns.",
    )
    parser.add_argument("--payload-bytes", type=int, default=0)
    parser.add_argument(
        "--timestamp-column",
        default="event_time",
        help="Timestamp column name. Use an empty string to disable.",
    )
    parser.add_argument(
        "--no-shuffle",
        action="store_true",
        help="Keep deterministic grouped order instead of shuffling rows.",
    )
    parser.add_argument(
        "--no-validate",
        action="store_true",
        help="Skip read-back validation after writing Parquet.",
    )


def add_tail_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--tail-distribution",
        choices=["uniform", "zipf"],
        default="uniform",
        help="Distribution used for non-heavy keys.",
    )
    parser.add_argument("--zipf-exponent", type=float, default=1.20)


def run(args: argparse.Namespace) -> dict:
    if args.scenario == "join-pair":
        return generate_join_pair(args)

    logical_keys = logical_keys_for_args(args)
    return write_standard_dataset(args, logical_keys, scenario=args.scenario, extra_parameters={})


def logical_keys_for_args(args: argparse.Namespace) -> list[str]:
    shuffle = not args.no_shuffle
    if args.scenario == "uniform":
        return dist.uniform_keys(args.rows, args.key_cardinality, args.seed, shuffle)
    if args.scenario == "single-heavy":
        return dist.single_heavy_keys(
            args.rows,
            args.key_cardinality,
            args.seed,
            args.heavy_key,
            args.heavy_fraction,
            args.tail_distribution,
            args.zipf_exponent,
            shuffle,
        )
    if args.scenario == "multi-heavy":
        return dist.multi_heavy_keys(
            args.rows,
            args.key_cardinality,
            args.seed,
            args.heavy_keys,
            args.heavy_fraction,
            dist.parse_weights(args.heavy_weights),
            args.tail_distribution,
            args.zipf_exponent,
            shuffle,
        )
    if args.scenario == "zipf":
        return dist.zipf_keys(
            args.rows,
            args.key_cardinality,
            args.seed,
            args.zipf_exponent,
            shuffle,
        )
    if args.scenario == "custom-heavy":
        return dist.custom_heavy_keys(
            args.rows,
            args.key_cardinality,
            args.seed,
            dist.parse_heavy_specs(args.heavy_spec),
            args.tail_distribution,
            args.zipf_exponent,
            shuffle,
        )
    if args.scenario == "group-by":
        return dist.group_by_keys(
            args.rows,
            args.key_cardinality,
            args.seed,
            args.group_distribution,
            args.heavy_fraction,
            args.heavy_keys,
            args.zipf_exponent,
            shuffle,
        )
    raise ValueError(f"unsupported scenario: {args.scenario}")


def generate_join_pair(args: argparse.Namespace) -> dict:
    if args.output.suffix == ".parquet":
        raise ValueError("join-pair output must be a directory")

    right_rows = args.right_rows if args.right_rows is not None else max(1, args.rows // 4)
    right_heavy_fraction = (
        args.right_heavy_fraction
        if args.right_heavy_fraction is not None
        else min(0.95, args.heavy_fraction + 0.10)
    )
    shuffle = not args.no_shuffle
    left_keys = dist.join_side_keys(
        args.rows,
        args.key_cardinality,
        args.seed,
        args.join_distribution,
        args.heavy_fraction,
        args.heavy_keys,
        args.zipf_exponent,
        shuffle,
    )
    right_keys = dist.join_side_keys(
        right_rows,
        args.key_cardinality,
        args.seed + 101,
        args.join_distribution,
        right_heavy_fraction,
        args.heavy_keys,
        args.zipf_exponent,
        shuffle,
    )

    left_output = args.output / "left.parquet" if args.files == 1 else args.output / "left"
    right_output = args.output / "right.parquet" if args.files == 1 else args.output / "right"

    left_metadata = write_standard_dataset(
        args,
        left_keys,
        scenario="join-left",
        output=left_output,
        extra_parameters={"join_side": "left"},
    )
    right_metadata = write_standard_dataset(
        args,
        right_keys,
        scenario="join-right",
        output=right_output,
        seed=args.seed + 101,
        extra_parameters={
            "join_side": "right",
            "right_rows": right_rows,
            "right_heavy_fraction": right_heavy_fraction,
        },
    )

    pair_metadata = {
        "version": "datasets-suite-v1",
        "scenario": "join-pair",
        "output": str(args.output),
        "left": left_metadata,
        "right": right_metadata,
        "parameters": common_parameters(args)
        | {
            "join_distribution": args.join_distribution,
            "right_rows": right_rows,
            "right_heavy_fraction": right_heavy_fraction,
        },
    }
    metadata_path = args.output / "_join_pair_metadata.json"
    metadata_path.write_text(json.dumps(pair_metadata, indent=2), encoding="utf-8")
    pair_metadata["metadata_path"] = str(metadata_path)
    return pair_metadata


def write_standard_dataset(
    args: argparse.Namespace,
    logical_keys: list[str],
    *,
    scenario: str,
    output: Path | None = None,
    files: int | None = None,
    seed: int | None = None,
    extra_parameters: dict,
) -> dict:
    from .writer import write_parquet_dataset

    output_path = output if output is not None else args.output
    metric_columns = parse_csv(args.metric_columns) or DEFAULT_METRIC_COLUMNS
    categorical_columns = parse_csv(args.categorical_columns)
    timestamp_column = args.timestamp_column.strip() or None
    return write_parquet_dataset(
        output=output_path,
        logical_keys=logical_keys,
        scenario=scenario,
        seed=args.seed if seed is None else seed,
        key_columns=parse_csv(args.key_columns),
        metric_columns=metric_columns,
        categorical_columns=categorical_columns,
        payload_bytes=args.payload_bytes,
        files=args.files if files is None else files,
        compression=args.compression,
        row_group_size=args.row_group_size,
        timestamp_column=timestamp_column,
        parameters=common_parameters(args) | extra_parameters,
        validate=not args.no_validate,
    )


def common_parameters(args: argparse.Namespace) -> dict:
    result = {
        "rows": args.rows,
        "key_cardinality": args.key_cardinality,
        "files": args.files,
        "compression": args.compression,
        "key_columns": parse_csv(args.key_columns),
        "metric_columns": parse_csv(args.metric_columns) or DEFAULT_METRIC_COLUMNS,
        "categorical_columns": parse_csv(args.categorical_columns),
        "payload_bytes": args.payload_bytes,
        "timestamp_column": args.timestamp_column.strip() or None,
        "shuffle": not args.no_shuffle,
    }
    for name in [
        "heavy_key",
        "heavy_fraction",
        "heavy_keys",
        "heavy_weights",
        "tail_distribution",
        "zipf_exponent",
        "heavy_spec",
        "group_distribution",
        "join_distribution",
    ]:
        if hasattr(args, name):
            result[name] = getattr(args, name)
    return result


def parse_csv(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]
