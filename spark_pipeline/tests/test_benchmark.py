from __future__ import annotations

import unittest

from spark_pipeline.benchmark import (
    resolve_method_aware_partial_group_keys,
    resolve_method_aware_partition_column,
    resolve_method_aware_salt_column,
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


if __name__ == "__main__":
    unittest.main()
