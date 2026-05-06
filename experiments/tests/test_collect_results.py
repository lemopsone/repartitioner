from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from experiments.collect_results import collect_results


class CollectResultsTests(unittest.TestCase):
    def test_collect_results_reads_after_skew_fields(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dataset = Path(temp_dir)
            write_json(
                output_dataset / "_partition_plan.json",
                {
                    "version": "0.2.0",
                    "strategy": "adaptive_hash_salt",
                    "output_partitions": 2,
                    "target_partition_rows": 10,
                    "job_type": "group_by",
                    "downstream_engine": "spark",
                    "min_partitions": 1,
                    "max_partitions": 2,
                    "required_partitions_by_size": 2,
                    "feasibility": {"target_partition_size_satisfied": True},
                    "technical_columns": {},
                    "recommended_downstream_plan": {},
                    "rewrite_required": True,
                    "action": "rewrite",
                },
            )
            write_json(
                output_dataset / "_stats.json",
                {
                    "version": "0.2.0",
                    "input": {
                        "total_rows": 20,
                        "input_file_count": 1,
                        "distinct_keys": 4,
                        "mean_key_frequency": 5.0,
                        "max_key_frequency": 5,
                        "heavy_hitters": [],
                    },
                    "estimates": {
                        "before_partition_sizes": [18, 2],
                        "after_partition_sizes": [10, 10],
                    },
                    "before_skew": {
                        "max_partition_size": 18,
                        "max_mean_imbalance_ratio": 1.8,
                        "coefficient_of_variation": 0.8,
                    },
                    "after_skew": {
                        "max_partition_size": 10,
                        "max_mean_imbalance_ratio": 1.0,
                        "coefficient_of_variation": 0.0,
                    },
                    "partition_bound": {
                        "target_rows_satisfied_after": True,
                    },
                },
            )
            write_json(
                output_dataset / "_manifest.json",
                {
                    "version": "0.2.0",
                    "input_reused": False,
                    "dataset_location": str(output_dataset),
                    "output_files": [],
                    "partitions": [
                        {"partition_id": 0, "row_count": 10},
                        {"partition_id": 1, "row_count": 10},
                    ],
                },
            )

            result = collect_results(output_dataset)

        self.assertEqual(result["before_max_partition_size"], 18)
        self.assertEqual(result["after_max_partition_size"], 10)
        self.assertEqual(result["before_cv"], 0.8)
        self.assertEqual(result["after_cv"], 0.0)
        self.assertEqual(result["partitioning_strategy"], "adaptive_hash_salt")
        self.assertTrue(result["target_rows_satisfied_after"])
        self.assertAlmostEqual(result["skew_reduction_ratio"], 1.0 / 1.8)


def write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


if __name__ == "__main__":
    unittest.main()
