"""数据集版本库:SQLite 存储,按版本写入/钉住/读出。"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from typing import Any, Optional

INT64 = "int64"
FLOAT64 = "float64"
STRING = "string"
COLUMN_TYPES = (INT64, FLOAT64, STRING)

_CONVERSION_LIMIT = 2 ** 53
_INT64_MIN = -(2 ** 63)
_INT64_MAX = 2 ** 63 - 1


class DatasetError(Exception):
    """所有领域错误的基类。"""


class UnknownDataset(DatasetError):
    """数据集不存在(尚未发布任何版本)。"""


class NotPinned(DatasetError):
    """消费者尚未钉住该数据集。"""


class InvalidSchema(DatasetError):
    """schema 本身不合法。"""


class PublishRejected(DatasetError):
    """publish_version 被拒绝。"""


class PushRejected(DatasetError):
    """push_records 被拒绝,整批不写。"""


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    nullable: bool = False
    default: Optional[Any] = None
    range: Optional[tuple] = None  # (lo, hi) 闭区间,仅数值列


@dataclass(frozen=True)
class Schema:
    columns: tuple  # tuple[Column, ...],按声明顺序

    @property
    def by_name(self) -> dict:
        return {c.name: c for c in self.columns}


def parse_schema(raw: Any) -> Schema:
    """把用户传入的 schema 描述解析并校验为 Schema。"""
    if not isinstance(raw, dict) or not isinstance(raw.get("columns"), list):
        raise InvalidSchema("schema 必须是含 columns 列表的 dict")
    columns = []
    seen = set()
    for entry in raw["columns"]:
        if not isinstance(entry, dict):
            raise InvalidSchema("每列必须是 dict")
        name = entry.get("name")
        ctype = entry.get("type")
        if not isinstance(name, str) or not name:
            raise InvalidSchema("列名必须是非空字符串")
        if name in seen:
            raise InvalidSchema(f"列名重复: {name}")
        seen.add(name)
        if ctype not in COLUMN_TYPES:
            raise InvalidSchema(f"列 {name} 类型非法: {ctype!r}")
        nullable = bool(entry.get("nullable", False))
        default = entry.get("default")
        rng = entry.get("range")
        if rng is not None:
            if ctype == STRING:
                raise InvalidSchema(f"string 列 {name} 不能带范围")
            if (
                not isinstance(rng, (list, tuple))
                or len(rng) != 2
                or any(isinstance(b, bool) or not isinstance(b, (int, float)) for b in rng)
                or rng[0] > rng[1]
            ):
                raise InvalidSchema(f"列 {name} 的范围必须是 [lo, hi] 且 lo <= hi")
            rng = (rng[0], rng[1])
        if default is not None:
            _check_value_type(name, ctype, default)
            if rng is not None and not (rng[0] <= default <= rng[1]):
                raise InvalidSchema(f"列 {name} 默认值超出范围")
        columns.append(Column(name=name, type=ctype, nullable=nullable,
                              default=default, range=rng))
    return Schema(columns=tuple(columns))


def _check_value_type(name: str, ctype: str, value: Any) -> None:
    if isinstance(value, bool):
        raise InvalidSchema(f"列 {name} 的值类型非法: {value!r}")
    ok = (
        (ctype == INT64 and isinstance(value, int))
        or (ctype == FLOAT64 and isinstance(value, (int, float)))
        or (ctype == STRING and isinstance(value, str))
    )
    if not ok:
        raise InvalidSchema(f"列 {name} 的值与类型 {ctype} 不符: {value!r}")


def project_record(data: dict, write_schema: Schema, read_schema: Schema) -> Optional[dict]:
    """把按 write_schema 写入的记录投影到 read_schema;不可读返回 None。"""
    write_cols = write_schema.by_name
    out = {}
    for col in read_schema.columns:
        wcol = write_cols.get(col.name)
        if wcol is None:
            if col.default is not None:
                out[col.name] = col.default
            elif col.nullable:
                out[col.name] = None
            else:
                return None
            continue
        value = data[col.name]
        if value is None:
            if col.nullable:
                out[col.name] = None
            else:
                return None
            continue
        converted = _convert_value(value, wcol.type, col.type)
        if converted is _UNREADABLE:
            return None
        if col.range is not None and not (col.range[0] <= converted <= col.range[1]):
            return None
        out[col.name] = converted
    return out


class _Unreadable:
    pass


_UNREADABLE = _Unreadable()


def _convert_value(value: Any, from_type: str, to_type: str) -> Any:
    if from_type == to_type:
        return value
    if from_type == INT64 and to_type == FLOAT64:
        if abs(value) <= _CONVERSION_LIMIT:
            return float(value)
        return _UNREADABLE
    if from_type == FLOAT64 and to_type == INT64:
        if isinstance(value, float) and value.is_integer() and abs(value) <= _CONVERSION_LIMIT:
            return int(value)
        return _UNREADABLE
    return _UNREADABLE


class DatasetRepo:
    """四个操作:publish_version / push_records / pin / read_snapshot。"""

    def __init__(self, db_path: str = ":memory:"):
        self._conn = sqlite3.connect(db_path)
        self._conn.execute("PRAGMA foreign_keys = ON")
        self._init_tables()

    def _init_tables(self) -> None:
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS versions (
                dataset TEXT NOT NULL,
                version INTEGER NOT NULL,
                schema  TEXT NOT NULL,
                PRIMARY KEY (dataset, version)
            );
            CREATE TABLE IF NOT EXISTS records (
                id      INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset TEXT NOT NULL,
                version INTEGER NOT NULL,
                data    TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS pins (
                consumer TEXT NOT NULL,
                dataset  TEXT NOT NULL,
                version  INTEGER NOT NULL,
                PRIMARY KEY (consumer, dataset)
            );
            """
        )
        self._conn.commit()

    def _latest_version(self, dataset: str) -> Optional[int]:
        row = self._conn.execute(
            "SELECT MAX(version) FROM versions WHERE dataset = ?", (dataset,)
        ).fetchone()
        return row[0]

    def _require_latest(self, dataset: str) -> int:
        latest = self._latest_version(dataset)
        if latest is None:
            raise UnknownDataset(f"数据集不存在: {dataset}")
        return latest

    def _load_schema(self, dataset: str, version: int) -> Schema:
        row = self._conn.execute(
            "SELECT schema FROM versions WHERE dataset = ? AND version = ?",
            (dataset, version),
        ).fetchone()
        if row is None:
            raise UnknownDataset(f"数据集 {dataset} 没有版本 {version}")
        return parse_schema(json.loads(row[0]))

    def _pinned_versions(self, dataset: str) -> list:
        rows = self._conn.execute(
            "SELECT DISTINCT version FROM pins WHERE dataset = ?", (dataset,)
        ).fetchall()
        return [r[0] for r in rows]

    def publish_version(self, dataset: str, schema: Any) -> int:
        new_schema = parse_schema(schema)
        latest = self._latest_version(dataset)
        new_version = 1 if latest is None else latest + 1

        # 拒绝条件一:存量记录里至少有一条不能按新版本读出
        rows = self._conn.execute(
            "SELECT version, data FROM records WHERE dataset = ?", (dataset,)
        ).fetchall()
        write_schemas = {}
        for wver, data_json in rows:
            if wver not in write_schemas:
                write_schemas[wver] = self._load_schema(dataset, wver)
            if project_record(json.loads(data_json), write_schemas[wver], new_schema) is None:
                raise PublishRejected(
                    f"存在无法按版本 {new_version} 读出的存量记录"
                )

        # 拒绝条件二:某个被钉住的版本,任何合法的新版本记录都读不出
        for pver in self._pinned_versions(dataset):
            pschema = self._load_schema(dataset, pver)
            if not _new_record_can_satisfy(new_schema, pschema):
                raise PublishRejected(
                    f"被钉住的版本 {pver} 无法读出任何版本 {new_version} 的合法记录"
                )

        self._conn.execute(
            "INSERT INTO versions (dataset, version, schema) VALUES (?, ?, ?)",
            (dataset, new_version, json.dumps(schema)),
        )
        self._conn.commit()
        return new_version

    def push_records(self, dataset: str, records: list) -> None:
        latest = self._require_latest(dataset)
        latest_schema = self._load_schema(dataset, latest)
        pinned = [
            (v, self._load_schema(dataset, v)) for v in self._pinned_versions(dataset)
        ]

        normalized = []
        for i, rec in enumerate(records):
            data = self._validate_record(rec, latest_schema, i)
            for pver, pschema in pinned:
                if project_record(data, latest_schema, pschema) is None:
                    raise PushRejected(
                        f"第 {i} 条记录无法按被钉住的版本 {pver} 读出,整批拒绝"
                    )
            normalized.append(data)

        with self._conn:  # 整批一个事务,要么全写要么全不写
            for data in normalized:
                self._conn.execute(
                    "INSERT INTO records (dataset, version, data) VALUES (?, ?, ?)",
                    (dataset, latest, json.dumps(data)),
                )

    @staticmethod
    def _validate_record(rec: Any, schema: Schema, index: int) -> dict:
        if not isinstance(rec, dict):
            raise PushRejected(f"第 {index} 条记录不是 dict")
        expected = {c.name for c in schema.columns}
        if set(rec.keys()) != expected:
            raise PushRejected(
                f"第 {index} 条记录的列与最新版本不符: "
                f"缺少 {sorted(expected - rec.keys())}, 多出 {sorted(rec.keys() - expected)}"
            )
        data = {}
        for col in schema.columns:
            value = rec[col.name]
            if value is None:
                if not col.nullable:
                    raise PushRejected(f"第 {index} 条记录列 {col.name} 不可为空")
                data[col.name] = None
                continue
            try:
                _check_value_type(col.name, col.type, value)
            except InvalidSchema as exc:
                raise PushRejected(f"第 {index} 条记录: {exc}") from exc
            if col.type == FLOAT64 and isinstance(value, int):
                value = float(value)
            if col.type == INT64 and not (_INT64_MIN <= value <= _INT64_MAX):
                raise PushRejected(f"第 {index} 条记录列 {col.name} 超出 int64 范围")
            if col.range is not None and not (col.range[0] <= value <= col.range[1]):
                raise PushRejected(f"第 {index} 条记录列 {col.name} 超出范围 {col.range}")
            data[col.name] = value
        return data

    def pin(self, consumer: str, dataset: str) -> int:
        latest = self._require_latest(dataset)
        with self._conn:
            self._conn.execute(
                "INSERT INTO pins (consumer, dataset, version) VALUES (?, ?, ?) "
                "ON CONFLICT (consumer, dataset) DO UPDATE SET version = excluded.version",
                (consumer, dataset, latest),
            )
        return latest

    def read_snapshot(self, consumer: str, dataset: str) -> list:
        row = self._conn.execute(
            "SELECT version FROM pins WHERE consumer = ? AND dataset = ?",
            (consumer, dataset),
        ).fetchone()
        if row is None:
            raise NotPinned(f"消费者 {consumer} 尚未钉住数据集 {dataset}")
        read_schema = self._load_schema(dataset, row[0])
        rows = self._conn.execute(
            "SELECT version, data FROM records WHERE dataset = ? ORDER BY id",
            (dataset,),
        ).fetchall()
        out = []
        for wver, data_json in rows:
            write_schema = self._load_schema(dataset, wver)
            projected = project_record(json.loads(data_json), write_schema, read_schema)
            if projected is None:
                raise DatasetError(  # 不变量被破坏,不应发生
                    f"记录无法按钉住版本 {row[0]} 读出"
                )
            out.append(projected)
        return out

    def close(self) -> None:
        self._conn.close()


def _new_record_can_satisfy(new_schema: Schema, pinned_schema: Schema) -> bool:
    """是否存在一条按 new_schema 合法的记录,能按 pinned_schema 读出。

    各列相互独立,逐列检查可满足性即可。
    """
    new_cols = new_schema.by_name
    for pcol in pinned_schema.columns:
        ncol = new_cols.get(pcol.name)
        if ncol is None:
            # 读出值与记录无关:有默认值取默认值,可空取 null,否则不可读
            if pcol.default is None and not pcol.nullable:
                return False
            continue
        if ncol.nullable and pcol.nullable:
            continue  # 写 null -> 读 null
        if not _domains_compatible(ncol, pcol):
            return False
    return True


def _domains_compatible(ncol: Column, pcol: Column) -> bool:
    """ncol 的合法值域中,是否存在能转换并落入 pcol 域的值(均非 null)。"""
    if ncol.type == STRING or pcol.type == STRING:
        return ncol.type == pcol.type
    lo, hi = ncol.range if ncol.range is not None else (float("-inf"), float("inf"))
    plo, phi = pcol.range if pcol.range is not None else (float("-inf"), float("inf"))
    if ncol.type == INT64:
        lo, hi = max(lo, _INT64_MIN), min(hi, _INT64_MAX)
    if pcol.type == INT64:
        plo, phi = max(plo, _INT64_MIN), min(phi, _INT64_MAX)
    if (ncol.type, pcol.type) in ((INT64, FLOAT64), (FLOAT64, INT64)):
        lo = max(lo, -_CONVERSION_LIMIT)
        hi = min(hi, _CONVERSION_LIMIT)
    lo, hi = max(lo, plo), min(hi, phi)
    if lo > hi:
        return False
    if ncol.type == INT64 or pcol.type == INT64:
        # 源或目标为整数时,交集中必须存在整数
        import math

        return math.ceil(lo) <= math.floor(hi)
    return True
