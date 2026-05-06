from __future__ import annotations

import unittest

from spark_pipeline.benchmark import (
    resolve_method_aware_partial_group_keys,
    resolve_method_aware_partition_column,
    resolve_method_aware_salt_column,
    single_column_heavy_key_literals,
)


class FakeDataFrame:
    def __init__(self, columns: list[str]) -> None:
        self.columns = columns


def partition_plan() -> dict:
    return {
        "technical_columns": {
            "partition_column": "_rp_partition_id",
            "salt_column": "_rp_salt",
        },
        "recommended_downstream_plan": {
            "partial_group_keys": ["_rp_partition_id", "_rp_salt", "user_id"],
        },
    }


class MethodAwareGroupByTests(unittest.TestCase):
    def test_method_aware_group_by_uses_salt_column_when_available(self) -> None:
        dataframe = FakeDataFrame(["_rp_partition_id", "_rp_salt", "user_id"])
        plan = partition_plan()

        partition_column = resolve_method_aware_partition_column(dataframe, plan)
        salt_column = resolve_method_aware_salt_column(dataframe, plan)
        partial_keys, extra = resolve_method_aware_partial_group_keys(
            dataframe,
            plan,
            key_column="user_id",
            partition_column=partition_column,
            salt_column=salt_column,
        )

        self.assertEqual(partial_keys, ["_rp_partition_id", "_rp_salt", "user_id"])
        self.assertEqual(salt_column, "_rp_salt")
        self.assertTrue(extra["salt_column_used"])
        self.assertFalse(extra["method_aware_degraded"])

    def test_method_aware_group_by_reports_degraded_mode_when_salt_missing(self) -> None:
        dataframe = FakeDataFrame(["_rp_partition_id", "user_id"])
        plan = partition_plan()

        partition_column = resolve_method_aware_partition_column(dataframe, plan)
        salt_column = resolve_method_aware_salt_column(dataframe, plan)
        partial_keys, extra = resolve_method_aware_partial_group_keys(
            dataframe,
            plan,
            key_column="user_id",
            partition_column=partition_column,
            salt_column=salt_column,
        )

        self.assertEqual(partial_keys, ["_rp_partition_id", "user_id"])
        self.assertIsNone(salt_column)
        self.assertFalse(extra["salt_column_used"])
        self.assertTrue(extra["method_aware_degraded"])
        self.assertEqual(extra["degraded_reason"], "salt_column_missing")


class StructuredHeavyKeyTests(unittest.TestCase):
    def test_single_column_heavy_key_literals_use_structured_metadata(self) -> None:
        plan = {
            "join_plan": {
                "shared_heavy_key_values": [
                    {
                        "encoded": "7:user_id#utf8:5:heavy",
                        "parts": [
                            {
                                "column": "user_id",
                                "value_type": "utf8",
                                "value": "heavy",
                            }
                        ],
                    }
                ]
            }
        }

        literals = single_column_heavy_key_literals(
            plan,
            side="shared",
            key_column="user_id",
        )

        self.assertEqual(
            literals,
            [{"column": "user_id", "value_type": "utf8", "value": "heavy"}],
        )

    def test_composite_heavy_key_literals_are_explicitly_unsupported(self) -> None:
        plan = {
            "join_plan": {
                "shared_heavy_key_values": [
                    {
                        "encoded": "composite",
                        "parts": [
                            {"column": "user_id", "value_type": "utf8", "value": "heavy"},
                            {"column": "region", "value_type": "utf8", "value": "eu"},
                        ],
                    }
                ]
            }
        }

        with self.assertRaisesRegex(ValueError, "single-column heavy keys only"):
            single_column_heavy_key_literals(
                plan,
                side="shared",
                key_column="user_id",
            )


if __name__ == "__main__":
    unittest.main()
