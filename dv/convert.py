"""Cross-version record conversion and feasibility checks."""

from __future__ import annotations

import math
from typing import Any, Dict, Optional, Tuple

from .schema import FLOAT64, INT64, STRING, Column, Schema

#: Largest absolute integer that converts losslessly between int64/float64.
INT_FLOAT_CAP = 2**53


def _convert_value(value: Any, src: Column, dst: Column) -> Tuple[bool, Any]:
    """Convert one non-null value from the src column to the dst column."""
    if src.type == dst.type:
        converted = value
    elif src.type == INT64 and dst.type == FLOAT64:
        if abs(value) > INT_FLOAT_CAP:
            return (False, None)
        converted = float(value)
    elif src.type == FLOAT64 and dst.type == INT64:
        if not value.is_integer() or abs(value) > INT_FLOAT_CAP:
            return (False, None)
        converted = int(value)
    else:
        return (False, None)
    if dst.numeric and not dst.in_range(converted):
        return (False, None)
    return (True, converted)


def convert_record(
    values: Dict[str, Any], src: Schema, dst: Schema
) -> Optional[Dict[str, Any]]:
    """Convert a record written at ``src`` so it reads at ``dst``.

    Returns None when the record is unreadable at ``dst``.
    """
    out: Dict[str, Any] = {}
    for dst_col in dst.columns:
        src_col = src.column(dst_col.name)
        if src_col is None:
            if dst_col.has_default:
                out[dst_col.name] = dst_col.default
            elif dst_col.nullable:
                out[dst_col.name] = None
            else:
                return None
            continue
        value = values[dst_col.name]
        if value is None:
            if dst_col.nullable:
                out[dst_col.name] = None
            else:
                return None
            continue
        ok, converted = _convert_value(value, src_col, dst_col)
        if not ok:
            return None
        out[dst_col.name] = converted
    return out


def _range_witness(
    a_lo: Optional[float],
    a_hi: Optional[float],
    b_lo: Optional[float],
    b_hi: Optional[float],
    need_int: bool,
    cap: Optional[float] = None,
) -> bool:
    """True iff the closed-interval intersection contains a usable witness."""
    los = [x for x in (a_lo, b_lo) if x is not None]
    his = [x for x in (a_hi, b_hi) if x is not None]
    lo = max(los) if los else None
    hi = min(his) if his else None
    if cap is not None:
        lo = -cap if lo is None else max(lo, -cap)
        hi = cap if hi is None else min(hi, cap)
    if need_int:
        lo = None if lo is None else math.ceil(lo)
        hi = None if hi is None else math.floor(hi)
    if lo is not None and hi is not None and lo > hi:
        return False
    return True


def _column_pair_feasible(new_col: Column, pinned_col: Column) -> bool:
    """Can a writer pick a value for ``new_col`` that reads at ``pinned_col``?"""
    if new_col.nullable and pinned_col.nullable:
        return True  # the null path always works
    if new_col.type == pinned_col.type:
        if new_col.type == STRING:
            return True
        return _range_witness(
            new_col.min,
            new_col.max,
            pinned_col.min,
            pinned_col.max,
            need_int=new_col.type == INT64,
        )
    if new_col.type == INT64 and pinned_col.type == FLOAT64:
        return _range_witness(
            new_col.min,
            new_col.max,
            pinned_col.min,
            pinned_col.max,
            need_int=True,
            cap=INT_FLOAT_CAP,
        )
    if new_col.type == FLOAT64 and pinned_col.type == INT64:
        return _range_witness(
            new_col.min,
            new_col.max,
            pinned_col.min,
            pinned_col.max,
            need_int=True,
            cap=INT_FLOAT_CAP,
        )
    return False


def writable_record_exists(new: Schema, pinned: Schema) -> bool:
    """True iff some legal record written at ``new`` reads at ``pinned``."""
    for pinned_col in pinned.columns:
        new_col = new.column(pinned_col.name)
        if new_col is None:
            if not pinned_col.has_default and not pinned_col.nullable:
                return False
            continue
        if not _column_pair_feasible(new_col, pinned_col):
            return False
    return True
