import unittest

from dataset_vcs import (
    DatasetRepo,
    NotPinned,
    PublishRejected,
    PushRejected,
    UnknownDataset,
)


def cols(*entries):
    return {"columns": [dict(e) for e in entries]}


class RepoTestCase(unittest.TestCase):
    def setUp(self):
        self.repo = DatasetRepo()

    def tearDown(self):
        self.repo.close()


class TestRequiredScenario(RepoTestCase):
    def test_upgrade_allows_out_of_v1_range_value(self):
        # v1: a int64 [0,10];消费者钉在 v1
        self.repo.publish_version("ds", cols({"name": "a", "type": "int64", "range": [0, 10]}))
        self.assertEqual(self.repo.pin("c1", "ds"), 1)
        # v2 放宽范围, v3 改 float64
        self.repo.publish_version("ds", cols({"name": "a", "type": "int64", "range": [0, 100]}))
        self.repo.publish_version("ds", cols({"name": "a", "type": "float64"}))
        # 升级前:超出 v1 范围的值必须被拒绝
        with self.assertRaises(PushRejected):
            self.repo.push_records("ds", [{"a": 50.0}])
        # pin 升级后:同一批记录必须写入成功
        self.assertEqual(self.repo.pin("c1", "ds"), 3)
        self.repo.push_records("ds", [{"a": 50.0}])
        self.assertEqual(self.repo.read_snapshot("c1", "ds"), [{"a": 50.0}])


class TestProjection(RepoTestCase):
    def test_defaults_nulls_and_dropped_columns(self):
        self.repo.publish_version("ds", cols({"name": "x", "type": "int64"}))
        self.repo.pin("c", "ds")
        self.repo.push_records("ds", [{"x": 1}])
        # v2: 去掉 x 不可能(x 不可空且无默认,存量记录读不出),改为加列
        self.repo.publish_version("ds", cols(
            {"name": "x", "type": "int64"},
            {"name": "s", "type": "string", "nullable": True, "default": "d"},
        ))
        self.repo.push_records("ds", [{"x": 2, "s": "hi"}])
        # 钉在 v1:s 被丢弃
        self.assertEqual(self.repo.read_snapshot("c", "ds"), [{"x": 1}, {"x": 2}])
        # 升级:v1 的记录补默认值
        self.repo.pin("c", "ds")
        self.assertEqual(
            self.repo.read_snapshot("c", "ds"),
            [{"x": 1, "s": "d"}, {"x": 2, "s": "hi"}],
        )

    def test_explicit_null_not_replaced_by_default(self):
        self.repo.publish_version("ds", cols({"name": "a", "type": "int64", "nullable": True}))
        self.repo.pin("c", "ds")
        self.repo.push_records("ds", [{"a": None}])
        # v2: a 带默认值但仍可空;显式 null 读出仍为 null
        self.repo.publish_version("ds", cols(
            {"name": "a", "type": "int64", "nullable": True, "default": 7}
        ))
        self.repo.pin("c", "ds")
        self.assertEqual(self.repo.read_snapshot("c", "ds"), [{"a": None}])

    def test_numeric_conversions(self):
        self.repo.publish_version("ds", cols({"name": "a", "type": "int64"}))
        self.repo.push_records("ds", [{"a": 3}])
        self.repo.publish_version("ds", cols({"name": "a", "type": "float64"}))
        self.repo.push_records("ds", [{"a": 4.0}])
        self.repo.pin("c", "ds")
        self.assertEqual(self.repo.read_snapshot("c", "ds"), [{"a": 3.0}, {"a": 4.0}])
        # 回到 int64:4.0 是整数可转,但存量 3.0 也可转,发布应成功
        self.repo.publish_version("ds", cols({"name": "a", "type": "int64"}))
        self.repo.pin("c", "ds")
        self.assertEqual(self.repo.read_snapshot("c", "ds"), [{"a": 3}, {"a": 4}])


class TestPublishRejection(RepoTestCase):
    def test_reject_when_existing_record_unreadable(self):
        self.repo.publish_version("ds", cols({"name": "a", "type": "int64"}))
        self.repo.push_records("ds", [{"a": 5}])
        with self.assertRaises(PublishRejected):
            self.repo.publish_version("ds", cols({"name": "a", "type": "string"}))
        with self.assertRaises(PublishRejected):
            self.repo.publish_version("ds", cols({"name": "a", "type": "int64", "range": [0, 3]}))
        # 合法收紧仍接受
        self.repo.publish_version("ds", cols({"name": "a", "type": "int64", "range": [0, 10]}))

    def test_reject_when_pinned_version_unsatisfiable(self):
        self.repo.publish_version("ds", cols({"name": "a", "type": "int64", "range": [0, 10]}))
        self.repo.pin("c", "ds")
        # 新版本 a 的范围与 v1 完全不相交 -> 拒绝
        with self.assertRaises(PublishRejected):
            self.repo.publish_version("ds", cols({"name": "a", "type": "int64", "range": [20, 30]}))
        # 部分相交 -> 接受
        self.repo.publish_version("ds", cols({"name": "a", "type": "int64", "range": [5, 50]}))


class TestPushAtomicity(RepoTestCase):
    def test_batch_all_or_nothing(self):
        self.repo.publish_version("ds", cols({"name": "a", "type": "int64"}))
        self.repo.pin("c", "ds")
        with self.assertRaises(PushRejected):
            self.repo.push_records("ds", [{"a": 1}, {"a": "bad"}])
        self.assertEqual(self.repo.read_snapshot("c", "ds"), [])

    def test_unknown_column_rejected(self):
        self.repo.publish_version("ds", cols({"name": "a", "type": "int64"}))
        with self.assertRaises(PushRejected):
            self.repo.push_records("ds", [{"a": 1, "b": 2}])


class TestErrors(RepoTestCase):
    def test_unknown_dataset_and_not_pinned(self):
        with self.assertRaises(UnknownDataset):
            self.repo.pin("c", "nope")
        self.repo.publish_version("ds", cols({"name": "a", "type": "int64"}))
        with self.assertRaises(NotPinned):
            self.repo.read_snapshot("c", "ds")


if __name__ == "__main__":
    unittest.main()
