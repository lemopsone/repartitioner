from __future__ import annotations

import unittest

from spark_pipeline.benchmark import (
    comparable_join_checksum_column_names,
    heavy_key_literals_for_join,
    logical_result_columns,
    method_aware_join_skip_reason,
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


def join_partition_plan(strategy: str = "salted_heavy_key_join") -> dict:
    return {
        "job_type": "join",
        "key_columns": ["user_id"],
        "technical_columns": {
            "included": True,
            "partition_column": "_rp_partition_id",
            "salt_column": "_rp_salt",
            "heavy_key_column": "_rp_is_heavy_key",
        },
        "recommended_downstream_plan": {
            "strategy": strategy,
            "join_keys": ["user_id"],
        },
        "heavy_keys": [
            {
                "key": "7:user_id#utf8:5:heavy",
                "structured_key": {
                    "encoded": "7:user_id#utf8:5:heavy",
                    "parts": [
                        {"column": "user_id", "value_type": "utf8", "value": "heavy"}
                    ],
                },
                "salt_count": 3,
            }
        ],
        "join_plan": {
            "right_side_size_mb": 2,
            "broadcast_threshold_mb": 10,
            "left_heavy_key_values": [
                {
                    "encoded": "7:user_id#utf8:5:heavy",
                    "parts": [
                        {"column": "user_id", "value_type": "utf8", "value": "heavy"}
                    ],
                }
            ],
            "right_heavy_key_values": [
                {
                    "encoded": "7:user_id#utf8:5:heavy",
                    "parts": [
                        {"column": "user_id", "value_type": "utf8", "value": "heavy"}
                    ],
                }
            ],
            "shared_heavy_key_values": [
                {
                    "encoded": "7:user_id#utf8:5:heavy",
                    "parts": [
                        {"column": "user_id", "value_type": "utf8", "value": "heavy"}
                    ],
                }
            ],
        },
    }


class MethodAwareJoinTests(unittest.TestCase):
    def test_method_aware_join_skips_composite_key_with_reason(self) -> None:
        plan = join_partition_plan()
        plan["recommended_downstream_plan"]["join_keys"] = ["user_id", "region"]
        dataframe = FakeDataFrame(["user_id", "_rp_partition_id", "_rp_salt", "_rp_is_heavy_key"])

        reason = method_aware_join_skip_reason(
            dataframe,
            plan,
            key_column="user_id",
            input_reused=False,
        )

        self.assertEqual(reason, "composite_join_key_unsupported")

    def test_method_aware_join_uses_broadcast_for_broadcast_strategy(self) -> None:
        plan = join_partition_plan("broadcast_join")
        dataframe = FakeDataFrame(["user_id", "_rp_partition_id", "_rp_salt", "_rp_is_heavy_key"])

        reason = method_aware_join_skip_reason(
            dataframe,
            plan,
            key_column="user_id",
            input_reused=False,
        )

        self.assertIsNone(reason)
        self.assertEqual(plan["recommended_downstream_plan"]["strategy"], "broadcast_join")

    def test_method_aware_join_uses_salt_column_for_salted_strategy(self) -> None:
        plan = join_partition_plan("salted_heavy_key_join")
        dataframe = FakeDataFrame(["user_id", "_rp_partition_id", "_rp_salt", "_rp_is_heavy_key"])

        reason = method_aware_join_skip_reason(
            dataframe,
            plan,
            key_column="user_id",
            input_reused=False,
        )
        heavy_literals = heavy_key_literals_for_join(
            plan,
            strategy="salted_heavy_key_join",
            key_column="user_id",
        )

        self.assertIsNone(reason)
        self.assertEqual(plan["technical_columns"]["salt_column"], "_rp_salt")
        self.assertEqual(heavy_literals[0]["encoded"], "7:user_id#utf8:5:heavy")

    def test_method_aware_join_reports_missing_technical_columns(self) -> None:
        plan = join_partition_plan("salted_heavy_key_join")
        dataframe = FakeDataFrame(["user_id"])

        reason = method_aware_join_skip_reason(
            dataframe,
            plan,
            key_column="user_id",
            input_reused=False,
        )

        self.assertEqual(reason, "missing_technical_columns")


class CorrectnessHelperTests(unittest.TestCase):
    def test_logical_result_columns_drop_technical_columns(self) -> None:
        columns = logical_result_columns(
            [
                "user_id",
                "_rp_salt",
                "_rp_partition_id",
                "payload",
                "rp_partition",
                "join_payload",
            ]
        )

        self.assertEqual(columns, ["user_id", "payload", "join_payload"])

    def test_join_checksum_columns_use_logical_intersection(self) -> None:
        columns = comparable_join_checksum_column_names(
            ["user_id", "payload", "join_payload"],
            ["user_id", "payload", "join_payload", "_rp_salt", "rp_partition"],
            "user_id",
        )

        self.assertEqual(columns, ["user_id", "join_payload", "payload"])


if __name__ == "__main__":
    unittest.main()
