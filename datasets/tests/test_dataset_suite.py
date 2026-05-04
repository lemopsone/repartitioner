import tempfile
import unittest
from collections import Counter
from pathlib import Path

try:
    import pyarrow.parquet as pq
except ImportError:
    pq = None

from dataset_suite.distributions import (
    HeavyKeySpec,
    custom_heavy_keys,
    multi_heavy_keys,
    uniform_keys,
)


class DistributionTests(unittest.TestCase):
    def test_uniform_distribution_is_exactly_balanced_when_divisible(self) -> None:
        keys = uniform_keys(rows=12, key_cardinality=3, seed=7, shuffle=True)

        self.assertEqual(Counter(keys), {"key_00000000": 4, "key_00000001": 4, "key_00000002": 4})

    def test_custom_heavy_distribution_uses_requested_counts(self) -> None:
        keys = custom_heavy_keys(
            rows=100,
            key_cardinality=10,
            seed=7,
            heavy_specs=[HeavyKeySpec("hot_a", 0.30), HeavyKeySpec("hot_b", 0.20)],
            tail_distribution="uniform",
            zipf_exponent=1.2,
        )

        counts = Counter(keys)
        self.assertEqual(counts["hot_a"], 30)
        self.assertEqual(counts["hot_b"], 20)
        self.assertEqual(sum(counts.values()), 100)

    def test_multi_heavy_weights_are_respected(self) -> None:
        keys = multi_heavy_keys(
            rows=100,
            key_cardinality=10,
            seed=7,
            heavy_key_count=2,
            heavy_fraction=0.60,
            heavy_weights=[2, 1],
            tail_distribution="uniform",
            zipf_exponent=1.2,
        )

        counts = Counter(keys)
        self.assertEqual(counts["heavy_00000000"], 40)
        self.assertEqual(counts["heavy_00000001"], 20)


class WriterTests(unittest.TestCase):
    @unittest.skipIf(pq is None, "pyarrow is not installed")
    def test_writes_valid_parquet_and_metadata(self) -> None:
        from dataset_suite.writer import write_parquet_dataset

        with tempfile.TemporaryDirectory() as temp_dir:
            output = Path(temp_dir) / "sample.parquet"
            metadata = write_parquet_dataset(
                output=output,
                logical_keys=["hot", "hot", "cold"],
                scenario="test",
                seed=42,
                key_columns=["tenant_id", "user_id"],
                metric_columns=["value"],
                categorical_columns=["region"],
                payload_bytes=8,
                files=1,
                compression="snappy",
                row_group_size=None,
                timestamp_column="event_time",
                parameters={},
                validate=True,
            )

            table = pq.read_table(output)
            self.assertEqual(table.num_rows, 3)
            self.assertIn("tenant_id", table.column_names)
            self.assertIn("user_id", table.column_names)
            self.assertEqual(metadata["distribution"]["top_keys"][0]["key"], "hot")
            self.assertTrue(output.with_suffix(output.suffix + ".json").exists())


if __name__ == "__main__":
    unittest.main()
