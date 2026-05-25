#!/usr/bin/env python3
from benchmark import base_parser, run_from_args


def main() -> None:
    parser = base_parser("Run equivalent Spark scan workloads.")
    args = parser.parse_args()
    run_from_args(args, workload_override="scan")


if __name__ == "__main__":
    main()
