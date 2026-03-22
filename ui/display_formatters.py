from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any
import math
import re

import pandas as pd

from ui.display_labels import STATUS_COLUMNS, format_status_text


EMPTY_DISPLAY = "—"

PERCENT_COLUMNS = {
    "suggested_position_pct",
    "market_change_pct",
    "unrealized_pnl_pct",
    "stop_loss_gap_pct",
    "target_gap_pct",
    "core_realtime_coverage_ratio",
    "target_total_position_pct",
    "actual_total_position_pct",
    "cash_pct",
}

AMOUNT_COLUMNS = {
    "order_price",
    "avg_fill_price",
    "entry_price",
    "stop_loss",
    "target_price",
    "expected_loss_amt",
    "expected_profit_amt",
    "close_price",
    "open_price",
    "high_price",
    "low_price",
    "prev_close",
    "unrealized_pnl_amt",
    "cost_price",
    "capital",
    "planned_position_amt",
    "actual_total_position_amt",
    "cash_amt",
}

INTEGER_COLUMNS = {
    "stage_no",
    "portfolio_rank",
    "execution_rank",
    "order_shares",
    "suggested_shares",
    "filled_shares",
    "filled_qty",
    "hold_qty",
    "available_qty",
    "hold_days",
    "reports_file_count",
    "key_files_present",
    "key_files_total",
    "selected_count",
    "review_count",
    "candidate_count",
    "hit_count",
    "miss_count",
}

DATE_ONLY_COLUMNS = {
    "trading_date",
    "trade_date",
    "date",
}

DATETIME_COLUMNS = {
    "last_refresh",
    "started_at",
    "ended_at",
    "review_time",
    "risk_review_time",
}

DATE_ONLY_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")
DATETIME_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(:\d{2})?$")


def format_display_value(value: Any, column_name: str = "", context: str = "table") -> str:
    """Format UI display text without mutating raw values."""

    if is_empty_value(value):
        return EMPTY_DISPLAY

    normalized_column = column_name.strip().lower()

    if _is_bool_like(value):
        return "是" if value else "否"

    if normalized_column in STATUS_COLUMNS:
        return format_status_text(str(value))

    if normalized_column in DATE_ONLY_COLUMNS:
        formatted = _format_datetime_candidate(value, date_only=True)
        return formatted if formatted is not None else str(value)

    if normalized_column in DATETIME_COLUMNS:
        formatted = _format_datetime_candidate(value, date_only=False)
        return formatted if formatted is not None else str(value)

    if normalized_column in PERCENT_COLUMNS or normalized_column.endswith("_pct") or normalized_column.endswith("_ratio"):
        formatted = _format_percent(value)
        return formatted if formatted is not None else str(value)

    if normalized_column in AMOUNT_COLUMNS or normalized_column.endswith("_amt") or normalized_column.endswith("_price"):
        formatted = _format_amount(value)
        return formatted if formatted is not None else str(value)

    if normalized_column in INTEGER_COLUMNS or normalized_column.endswith("_count") or normalized_column.endswith("_qty") or normalized_column.endswith("_shares"):
        formatted = _format_integer(value)
        return formatted if formatted is not None else str(value)

    if _looks_like_datetime_string(value):
        formatted = _format_datetime_candidate(value, date_only=False)
        return formatted if formatted is not None else str(value)

    return str(value)


def format_status_panel_value(value: Any, field_name: str = "") -> str:
    return format_display_value(value, column_name=field_name, context="status")


def is_empty_value(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        stripped = value.strip()
        return stripped == "" or stripped.lower() in {"nan", "none", "null", "nat"}
    try:
        return bool(pd.isna(value))
    except Exception:
        return False


def _format_percent(value: Any) -> str | None:
    number = _to_decimal(value)
    if number is None:
        return None

    display_value = number * Decimal("100") if abs(number) <= Decimal("1") else number
    return f"{display_value:,.2f}%"


def _format_amount(value: Any) -> str | None:
    number = _to_decimal(value)
    if number is None:
        return None
    return f"{number:,.2f}"


def _format_integer(value: Any) -> str | None:
    number = _to_decimal(value)
    if number is None:
        return None
    quantized = int(number)
    return f"{quantized:,}"


def _format_datetime_candidate(value: Any, date_only: bool) -> str | None:
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d" if date_only else "%Y-%m-%d %H:%M:%S")
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")

    text = str(value).strip()
    if not text:
        return None

    try:
        parsed = pd.to_datetime(text, errors="coerce")
    except Exception:
        return None

    if pd.isna(parsed):
        return None

    if date_only or (text and DATE_ONLY_PATTERN.match(text)):
        return parsed.strftime("%Y-%m-%d")
    return parsed.strftime("%Y-%m-%d %H:%M:%S")


def _looks_like_datetime_string(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    stripped = value.strip()
    return bool(DATE_ONLY_PATTERN.match(stripped) or DATETIME_PATTERN.match(stripped))


def _to_decimal(value: Any) -> Decimal | None:
    if isinstance(value, bool):
        return Decimal(1 if value else 0)
    if isinstance(value, Decimal):
        return value
    if isinstance(value, int):
        return Decimal(value)
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return Decimal(str(value))

    text = str(value).strip().replace(",", "")
    if not text:
        return None
    try:
        return Decimal(text)
    except (InvalidOperation, ValueError):
        return None


def _is_bool_like(value: Any) -> bool:
    return isinstance(value, bool) or type(value).__name__ in {"bool_", "bool"}
