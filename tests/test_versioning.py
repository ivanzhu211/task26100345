"""End-to-end tests for the dataset version repository."""

import unittest

from dv import (
    FLOAT64,
    INT64,
    STRING,
    Column,
    InvalidRecord,
    InvalidSchema,
    NotPinned,
    PublishRejected,
    PushRejected,
    Repository,
    Schema,
    UnknownDataset,
)
from dv.convert import INT_FLOAT_CAP


class RepoTestCase(unittest.TestCase):
    def setUp(self):
        self.repo = Repository()
        self.addCleanup(self.repo.close)


class TestBasics(RepoTestCase):
    def test_versions_increment_from_one(self):
        repo = self.repo
        assert repo.publish_version("ds", Schema([Column("a", INT64)])) == 1
        assert repo.publish_version("ds", Schema([Column("a", INT64)])) == 2
        assert repo.publish_version("ds", Schema([Column("a", INT64)])) == 3

    def test_push_requires_published_version(self):
        with self.assertRaises(UnknownDataset):
            self.repo.push_records("nope", [{"a": 1}])

    def test_read_requires_pin(self):
        self.repo.publish_version("ds", Schema([Column("a", INT64)]))
        with self.assertRaises(NotPinned):
            self.repo.read_snapshot("c", "ds")

    def test_pin_unknown_dataset(self):
        with self.assertRaises(UnknownDataset):
            self.repo.pin("c", "nope")

    def test_invalid_schema_rejected(self):
        with self.assertRaises(InvalidSchema):
            self.repo.publish_version("ds", Schema([Column("a", "int32")]))
        with self.assertRaises(InvalidSchema):
            self.repo.publish_version("ds", Schema([]))
        with self.assertRaises(InvalidSchema):
            self.repo.publish_version(
                "ds", Schema([Column("a", INT64), Column("a", INT64)])
            )
        with self.assertRaises(InvalidSchema):
            self.repo.publish_version(
                "ds", Schema([Column("a", STRING, min=0, max=1)])
            )

    def test_record_validation(self):
        self.repo.publish_version(
            "ds", Schema([Column("a", INT64, min=0, max=10)])
        )
        with self.assertRaises(InvalidRecord):
            self.repo.push_records("ds", [{"a": 1, "extra": 2}])
        with self.assertRaises(InvalidRecord):
            self.repo.push_records("ds", [{"a": None}])
        with self.assertRaises(InvalidRecord):
            self.repo.push_records("ds", [{"a": 11}])
        with self.assertRaises(InvalidRecord):
            self.repo.push_records("ds", [{"a": True}])
        with self.assertRaises(InvalidRecord):
            self.repo.push_records("ds", [{"a": "1"}])
        self.repo.push_records("ds", [{"a": 10}])

    def test_push_is_atomic(self):
        self.repo.publish_version("ds", Schema([Column("a", INT64, max=5)]))
        self.repo.pin("c", "ds")
        with self.assertRaises(InvalidRecord):
            self.repo.push_records("ds", [{"a": 1}, {"a": 99}])
        assert self.repo.read_snapshot("c", "ds") == []


class TestCrossVersionReads(RepoTestCase):
    def test_defaults_and_dropped_columns(self):
        repo = self.repo
        repo.publish_version("ds", Schema([Column("a", INT64)]))
        repo.push_records("ds", [{"a": 1}])
        repo.pin("old", "ds")
        repo.publish_version(
            "ds",
            Schema(
                [
                    Column("a", INT64),
                    Column("b", STRING, default="hi"),
                    Column("c", FLOAT64, nullable=True),
                ]
            ),
        )
        repo.push_records("ds", [{"a": 2, "b": "yo", "c": 1.5}])
        # old pin: extra columns of v2 are dropped
        assert repo.read_snapshot("old", "ds") == [{"a": 1}, {"a": 2}]
        repo.pin("old", "ds")
        # new pin: v1 record gets default / null for the added columns
        assert repo.read_snapshot("old", "ds") == [
            {"a": 1, "b": "hi", "c": None},
            {"a": 2, "b": "yo", "c": 1.5},
        ]

    def test_explicit_null_is_not_replaced_by_default(self):
        repo = self.repo
        repo.publish_version("ds", Schema([Column("a", INT64, nullable=True)]))
        repo.push_records("ds", [{"a": None}])
        # new version makes the column non-nullable with a default: the
        # stored explicit null cannot be read there, so publish is rejected
        with self.assertRaises(PublishRejected):
            repo.publish_version("ds", Schema([Column("a", INT64, default=7)]))
        # nullable with a default: null stays null, default is not used
        repo.publish_version(
            "ds", Schema([Column("a", INT64, nullable=True, default=7)])
        )
        repo.pin("c", "ds")
        assert repo.read_snapshot("c", "ds") == [{"a": None}]

    def test_missing_column_without_default_blocks_publish(self):
        repo = self.repo
        repo.publish_version("ds", Schema([Column("a", INT64)]))
        repo.push_records("ds", [{"a": 1}])
        with self.assertRaises(PublishRejected):
            repo.publish_version(
                "ds", Schema([Column("a", INT64), Column("b", INT64)])
            )
        # nullable addition is fine
        repo.publish_version(
            "ds",
            Schema([Column("a", INT64), Column("b", INT64, nullable=True)]),
        )

    def test_numeric_conversion_rules(self):
        repo = self.repo
        repo.publish_version("ds", Schema([Column("x", INT64)]))
        repo.push_records("ds", [{"x": 3}])
        repo.publish_version("ds", Schema([Column("x", FLOAT64)]))
        repo.push_records("ds", [{"x": 2.5}])
        repo.pin("floats", "ds")
        # int64 -> float64 for the old record, float stays float
        assert repo.read_snapshot("floats", "ds") == [{"x": 3.0}, {"x": 2.5}]

    def test_push_rejected_when_unreadable_at_pin(self):
        repo = self.repo
        repo.publish_version("ds", Schema([Column("x", INT64)]))
        repo.pin("c", "ds")
        repo.publish_version("ds", Schema([Column("x", FLOAT64)]))
        with self.assertRaises(PushRejected):
            repo.push_records("ds", [{"x": 2.5}])
        repo.push_records("ds", [{"x": 2.0}])
        assert repo.read_snapshot("c", "ds") == [{"x": 2}]

    def test_int_float_cap(self):
        repo = self.repo
        repo.publish_version("ds", Schema([Column("x", INT64)]))
        repo.push_records("ds", [{"x": INT_FLOAT_CAP}])
        repo.pin("c", "ds")
        repo.publish_version("ds", Schema([Column("x", FLOAT64)]))
        assert repo.read_snapshot("c", "ds") == [{"x": float(INT_FLOAT_CAP)}]

    def test_oversized_int_blocks_publish(self):
        repo = self.repo
        repo.publish_version("ds", Schema([Column("x", INT64)]))
        repo.push_records("ds", [{"x": INT_FLOAT_CAP + 1}])
        with self.assertRaises(PublishRejected):
            repo.publish_version("ds", Schema([Column("x", FLOAT64)]))

    def test_string_to_numeric_blocks_publish_when_pinned(self):
        repo = self.repo
        repo.publish_version("ds", Schema([Column("x", INT64)]))
        repo.pin("c", "ds")
        with self.assertRaises(PublishRejected):
            repo.publish_version("ds", Schema([Column("x", STRING)]))
        # without any pin the same publish is accepted
        repo2 = Repository()
        self.addCleanup(repo2.close)
        repo2.publish_version("ds", Schema([Column("x", INT64)]))
        assert repo2.publish_version("ds", Schema([Column("x", STRING)])) == 2

    def test_disjoint_ranges_block_publish_when_pinned(self):
        repo = self.repo
        repo.publish_version("ds", Schema([Column("x", INT64, min=0, max=10)]))
        repo.pin("c", "ds")
        with self.assertRaises(PublishRejected):
            repo.publish_version(
                "ds", Schema([Column("x", INT64, min=20, max=30)])
            )

    def test_narrowing_range_with_existing_data_blocked(self):
        repo = self.repo
        repo.publish_version("ds", Schema([Column("x", INT64, min=0, max=100)]))
        repo.push_records("ds", [{"x": 50}])
        with self.assertRaises(PublishRejected):
            repo.publish_version("ds", Schema([Column("x", INT64, min=0, max=10)]))

    def test_consumers_on_different_versions(self):
        repo = self.repo
        repo.publish_version("ds", Schema([Column("x", INT64)]))
        repo.push_records("ds", [{"x": 1}])
        repo.pin("c1", "ds")
        repo.publish_version("ds", Schema([Column("x", FLOAT64)]))
        repo.pin("c2", "ds")
        repo.push_records("ds", [{"x": 4.0}])
        assert repo.read_snapshot("c1", "ds") == [{"x": 1}, {"x": 4}]
        assert repo.read_snapshot("c2", "ds") == [{"x": 1.0}, {"x": 4.0}]


class TestRequiredScenario(RepoTestCase):
    """Pinned at v1; v2 widens an int64 range; v3 switches it to float64.

    A record written at v3 holding a value outside the v1 range must be
    rejected while the consumer is pinned at v1, and accepted after the
    consumer upgrades.
    """

    def test_widen_then_retype(self):
        repo = self.repo
        repo.publish_version("ds", Schema([Column("x", INT64, min=0, max=100)]))
        repo.push_records("ds", [{"x": 10}])
        assert repo.pin("consumer", "ds") == 1

        # v2 widens the int64 range
        assert (
            repo.publish_version(
                "ds", Schema([Column("x", INT64, min=0, max=1000)])
            )
            == 2
        )
        # v3 retypes the column to float64
        assert (
            repo.publish_version(
                "ds", Schema([Column("x", FLOAT64, min=0, max=1000)])
            )
            == 3
        )

        batch = [{"x": 500.0}]  # legal at v3, outside the v1 range
        with self.assertRaises(PushRejected):
            repo.push_records("ds", batch)
        # nothing was written by the rejected batch
        assert repo.read_snapshot("consumer", "ds") == [{"x": 10}]

        # explicit upgrade, then the very same batch goes through
        assert repo.pin("consumer", "ds") == 3
        repo.push_records("ds", batch)
        assert repo.read_snapshot("consumer", "ds") == [
            {"x": 10.0},
            {"x": 500.0},
        ]


if __name__ == "__main__":
    unittest.main()
