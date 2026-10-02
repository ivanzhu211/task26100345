"""Schema and column type definitions."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Optional, Tuple

from .errors import InvalidSchema

INT64 = "int64"
FLOAT64 = "float64"
STRING = "string"
COLUMN_TYPES = (INT64, FLOAT64, STRING)
NUMERIC_TYPES = (INT64, FLOAT64)


class _NoDefault:
    def __repr__(self) -> str:  # pragma: no cover - cosmetic
        return "NO_DEFAULT"


NO_DEFAULT = _NoDefault()


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    nullable: bool = False
    default: Any = NO_DEFAULT
    min: Optional[float] = None
    max: Optional[float] = None

    @property
    def has_default(self) -> bool:
        return self.default is not NO_DEFAULT

    @property
    def numeric(self) -> bool:
        return self.type in NUMERIC_TYPES

    def validate(self) -> None:
        if not self.name:
            raise InvalidSchema("column name must be non-empty")
        if self.type not in COLUMN_TYPES:
            raise InvalidSchema(f"column {self.name!r}: unknown type {self.type!r}")
        if (self.min is not None or self.max is not None) and not self.numeric:
            raise InvalidSchema(
                f"column {self.name!r}: ranges only apply to numeric columns"
            )
        if (
            self.min is not None
            and self.max is not None
            and not self.min <= self.max
        ):
            raise InvalidSchema(f"column {self.name!r}: min exceeds max")
        if self.has_default:
            ok, _ = check_value(self, self.default)
            if not ok:
                raise InvalidSchema(
                    f"column {self.name!r}: default violates the column definition"
                )

    def in_range(self, value: float) -> bool:
        if self.min is not None and value < self.min:
            return False
        if self.max is not None and value > self.max:
            return False
        return True


def check_value(col: Column, value: Any) -> Tuple[bool, Any]:
    """Validate a raw value against a column; return (ok, normalized_value).

    Normalization stores float64 values as Python floats so the stored kind
    always matches the column type.
    """
    if value is None:
        return (col.nullable, None)
    if isinstance(value, bool):
        return (False, None)
    if col.type == INT64:
        if not isinstance(value, int):
            return (False, None)
        return (col.in_range(value), value)
    if col.type == FLOAT64:
        if not isinstance(value, (int, float)):
            return (False, None)
        fval = float(value)
        if math.isnan(fval):
            return (False, None)
        return (col.in_range(fval), fval)
    if isinstance(value, str):
        return (True, value)
    return (False, None)


class Schema:
    """An ordered set of columns keyed by name."""

    def __init__(self, columns) -> None:
        self.columns: Tuple[Column, ...] = tuple(columns)
        seen = set()
        for col in self.columns:
            col.validate()
            if col.name in seen:
                raise InvalidSchema(f"duplicate column {col.name!r}")
            seen.add(col.name)
        if not self.columns:
            raise InvalidSchema("schema must contain at least one column")
        self._by_name = {c.name: c for c in self.columns}

    def column(self, name: str) -> Optional[Column]:
        return self._by_name.get(name)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Schema) and self.columns == other.columns

    def __repr__(self) -> str:  # pragma: no cover - cosmetic
        return f"Schema({self.columns!r})"
