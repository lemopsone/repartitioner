from __future__ import annotations

import unittest

from experiments.run_research import aggregate_summary_rows, partition_metric_values


class PartitionMetricTests(unittest.TestCase):
    def test_partition_metrics_include_rows_bytes_and_reduction_factor(self) -> None:
        metrics = partition_metric_values(
            {
                "rows": 100,
                "target_partition_rows": 25,
                "cost_estimated_rows_written": 100,
                "cost_estimated_bytes_written": 1000,
                "before_skew": {
                    "max_partition_size": 70,
                    "mean_partition_size": 25.0,
                    "p95_partition_size": 70,
                    "coefficient_of_variation": 1.1,
                    "max_mean_imbalance_ratio": 2.8,
                },
                "after_skew": {
                    "max_partition_size": 30,
                    "mean_partition_size": 25.0,
                    "p95_partition_size": 30,
                    "coefficient_of_variation": 0.2,
                    "max_mean_imbalance_ratio": 1.2,
                },
            }
        )

        self.assertEqual(metrics["before"]["max_partition_rows"], 70)
        self.assertEqual(metrics["after"]["max_partition_rows"], 30)
        self.assertEqual(metrics["before"]["max_partition_bytes_estimated"], 700)
        self.assertEqual(metrics["after"]["p95_partition_bytes_estimated"], 300)
        self.assertAlmostEqual(metrics["after"]["skew_reduction_factor"], 70 / 30)
        self.assertAlmostEqual(metrics["after"]["skew_remaining_ratio"], 30 / 70)
        self.assertAlmostEqual(metrics["after"]["largest_partition_share"], 0.3)
        self.assertAlmostEqual(metrics["after"]["max_over_target_partition_rows"], 1.2)

    def test_aggregate_summary_keeps_partition_metrics(self) -> None:
        rows = [
            {
                "skew": "heavy_key",
                "workload": "group_by",
                "rows": 100,
                "dataset_repetition": 1,
                "spark_repetition": 1,
                "variant": "repartitioner",
                "spark_time_seconds": 2.0,
                "tau": 1.2,
                "max_partition_rows": 30,
                "preprocessing_seconds": 10.0,
                "total_with_preprocessing_seconds": 12.0,
                "spark_mode": "physical_only",
                "correctness_json": "{}",
            }
        ]

        summary = aggregate_summary_rows(rows, 0.0)

        self.assertEqual(summary[0]["max_partition_rows"], 30)
        self.assertEqual(summary[0]["coefficient_of_variation"], 0.0)


if __name__ == "__main__":
    unittest.main()
