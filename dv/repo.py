"""SQLite-backed dataset version repository."""

from __future__ import annotations

import sqlite3
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

from .convert import convert_record, writable_record_exists
from .errors import (
    DVError,
    InvalidRecord,
    NotPinned,
    PublishRejected,
    PushRejected,
    UnknownDataset,
)
from .schema import FLOAT64, INT64, NO_DEFAULT, Column, Schema, check_value

_DDL = """
CREATE TABLE IF NOT EXISTS datasets (
    name TEXT PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS schema_versions (
    dataset TEXT NOT NULL,
    version INTEGER NOT NULL,
    PRIMARY KEY (dataset, version)
);
CREATE TABLE IF NOT EXISTS columns (
    dataset TEXT NOT NULL,
    version INTEGER NOT NULL,
    pos INTEGER NOT NULL,
    name TEXT NOT NULL,
    type TEXT NOT NULL,
    nullable INTEGER NOT NULL,
    has_default INTEGER NOT NULL,
    default_kind TEXT,
    default_ival INTEGER,
    default_rval REAL,
    default_sval TEXT,
    lo REAL,
    hi REAL,
    PRIMARY KEY (dataset, version, name)
);
CREATE TABLE IF NOT EXISTS records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dataset TEXT NOT NULL,
    version INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS record_values (
    record_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    kind TEXT NOT NULL,
    ival INTEGER,
    rval REAL,
    sval TEXT,
    PRIMARY KEY (record_id, name)
);
CREATE TABLE IF NOT EXISTS pins (
    consumer TEXT NOT NULL,
    dataset TEXT NOT NULL,
    version INTEGER NOT NULL,
    PRIMARY KEY (consumer, dataset)
);
"""


def _encode_value(col_type: str, value: Any) -> Tuple[str, Any, Any, Any]:
    if value is None:
        return ("null", None, None, None)
    if col_type == INT64:
        return ("int", value, None, None)
    if col_type == FLOAT64:
        return ("float", None, float(value), None)
    return ("str", None, None, value)


def _decode_value(kind: str, ival: Any, rval: Any, sval: Any) -> Any:
    if kind == "null":
        return None
    if kind == "int":
        return ival
    if kind == "float":
        return rval
    return sval


class Repository:
    def __init__(self, path: str = ":memory:") -> None:
        self._conn = sqlite3.connect(path)
        self._conn.executescript(_DDL)
        self._schemas: Dict[Tuple[str, int], Schema] = {}

    def close(self) -> None:
        self._conn.close()

    # -- schema storage ---------------------------------------------------

    def _latest_version(self, dataset: str) -> Optional[int]:
        row = self._conn.execute(
            "SELECT MAX(version) FROM schema_versions WHERE dataset = ?",
            (dataset,),
        ).fetchone()
        return row[0]

    def _load_schema(self, dataset: str, version: int) -> Schema:
        key = (dataset, version)
        if key in self._schemas:
            return self._schemas[key]
        rows = self._conn.execute(
            "SELECT name, type, nullable, has_default, default_kind,"
            " default_ival, default_rval, default_sval, lo, hi"
            " FROM columns WHERE dataset = ? AND version = ? ORDER BY pos",
            (dataset, version),
        ).fetchall()
        cols = []
        for row in rows:
            default = NO_DEFAULT
            if row[3]:
                default = _decode_value(row[4], row[5], row[6], row[7])
            cols.append(
                Column(
                    name=row[0],
                    type=row[1],
                    nullable=bool(row[2]),
                    default=default,
                    min=row[8],
                    max=row[9],
                )
            )
        schema = Schema(cols)
        self._schemas[key] = schema
        return schema

    def _store_schema(self, dataset: str, version: int, schema: Schema) -> None:
        self._conn.execute(
            "INSERT INTO schema_versions (dataset, version) VALUES (?, ?)",
            (dataset, version),
        )
        for pos, col in enumerate(schema.columns):
            if col.has_default:
                kind, ival, rval, sval = _encode_value(col.type, col.default)
                has_default = 1
            else:
                kind, ival, rval, sval = None, None, None, None
                has_default = 0
            self._conn.execute(
                "INSERT INTO columns (dataset, version, pos, name, type,"
                " nullable, has_default, default_kind, default_ival,"
                " default_rval, default_sval, lo, hi)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    dataset,
                    version,
                    pos,
                    col.name,
                    col.type,
                    1 if col.nullable else 0,
                    has_default,
                    kind,
                    ival,
                    rval,
                    sval,
                    col.min,
                    col.max,
                ),
            )

    # -- record storage ---------------------------------------------------

    def _iter_records(
        self, dataset: str
    ) -> Iterator[Tuple[int, Dict[str, Any]]]:
        rows = self._conn.execute(
            "SELECT id, version FROM records WHERE dataset = ? ORDER BY id",
            (dataset,),
        ).fetchall()
        for rec_id, version in rows:
            values = {
                name: _decode_value(kind, ival, rval, sval)
                for name, kind, ival, rval, sval in self._conn.execute(
                    "SELECT name, kind, ival, rval, sval"
                    " FROM record_values WHERE record_id = ?",
                    (rec_id,),
                )
            }
            yield version, values

    def _pinned_versions(self, dataset: str) -> List[int]:
        rows = self._conn.execute(
            "SELECT DISTINCT version FROM pins WHERE dataset = ?", (dataset,)
        ).fetchall()
        return [row[0] for row in rows]

    # -- operations -------------------------------------------------------

    def publish_version(self, dataset: str, schema: Schema) -> int:
        if not isinstance(schema, Schema):
            schema = Schema(schema)
        latest = self._latest_version(dataset)
        if latest is None:
            with self._conn:
                self._conn.execute(
                    "INSERT OR IGNORE INTO datasets (name) VALUES (?)", (dataset,)
                )
                self._store_schema(dataset, 1, schema)
            return 1
        new_version = latest + 1
        for version, values in self._iter_records(dataset):
            written = self._load_schema(dataset, version)
            if convert_record(values, written, schema) is None:
                raise PublishRejected(
                    f"an existing record written at v{version} is unreadable"
                    f" at the new schema"
                )
        for pinned in self._pinned_versions(dataset):
            pinned_schema = self._load_schema(dataset, pinned)
            if not writable_record_exists(schema, pinned_schema):
                raise PublishRejected(
                    f"no legal record written at the new schema could be read"
                    f" at pinned v{pinned}"
                )
        with self._conn:
            self._store_schema(dataset, new_version, schema)
        return new_version

    def push_records(self, dataset: str, records: Iterable[Dict[str, Any]]) -> None:
        records = list(records)
        latest = self._latest_version(dataset)
        if latest is None:
            raise UnknownDataset(f"dataset {dataset!r} has no published version")
        schema = self._load_schema(dataset, latest)
        names = {col.name for col in schema.columns}
        normalized: List[Dict[str, Any]] = []
        for rec in records:
            if set(rec.keys()) != names:
                raise InvalidRecord(
                    f"record keys {sorted(rec.keys())} do not match the latest"
                    f" schema columns {sorted(names)}"
                )
            values = {}
            for col in schema.columns:
                ok, nval = check_value(col, rec[col.name])
                if not ok:
                    raise InvalidRecord(
                        f"value {rec[col.name]!r} is not legal for column"
                        f" {col.name!r}"
                    )
                values[col.name] = nval
            normalized.append(values)
        pinned_schemas = [
            self._load_schema(dataset, p) for p in self._pinned_versions(dataset)
        ]
        for values in normalized:
            for pinned_schema in pinned_schemas:
                if convert_record(values, schema, pinned_schema) is None:
                    raise PushRejected(
                        "a record in the batch is unreadable at a pinned"
                        " version; nothing was written"
                    )
        with self._conn:
            for values in normalized:
                cur = self._conn.execute(
                    "INSERT INTO records (dataset, version) VALUES (?, ?)",
                    (dataset, latest),
                )
                rec_id = cur.lastrowid
                for col in schema.columns:
                    kind, ival, rval, sval = _encode_value(
                        col.type, values[col.name]
                    )
                    self._conn.execute(
                        "INSERT INTO record_values"
                        " (record_id, name, kind, ival, rval, sval)"
                        " VALUES (?, ?, ?, ?, ?, ?)",
                        (rec_id, col.name, kind, ival, rval, sval),
                    )

    def pin(self, consumer: str, dataset: str) -> int:
        latest = self._latest_version(dataset)
        if latest is None:
            raise UnknownDataset(f"dataset {dataset!r} has no published version")
        with self._conn:
            self._conn.execute(
                "INSERT INTO pins (consumer, dataset, version) VALUES (?, ?, ?)"
                " ON CONFLICT(consumer, dataset)"
                " DO UPDATE SET version = excluded.version",
                (consumer, dataset, latest),
            )
        return latest

    def read_snapshot(self, consumer: str, dataset: str) -> List[Dict[str, Any]]:
        row = self._conn.execute(
            "SELECT version FROM pins WHERE consumer = ? AND dataset = ?",
            (consumer, dataset),
        ).fetchone()
        if row is None:
            raise NotPinned(
                f"consumer {consumer!r} has not pinned dataset {dataset!r}"
            )
        schema = self._load_schema(dataset, row[0])
        out: List[Dict[str, Any]] = []
        for version, values in self._iter_records(dataset):
            written = self._load_schema(dataset, version)
            converted = convert_record(values, written, schema)
            if converted is None:  # pragma: no cover - hard requirement
                raise DVError(
                    "hard requirement violated: a stored record is unreadable"
                    " at a pinned version"
                )
            out.append(converted)
        return out
