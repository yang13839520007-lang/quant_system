from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any
import math

from PySide6.QtGui import QColor, QBrush

from ui.display_formatters import is_empty_value


@dataclass(frozen=True)
class ColorRule:
    foreground: str
    background: str = ""


@dataclass(frozen=True)
class DisplayThresholds:
    stop_loss_near_pct: float = 2.0
    target_near_pct: float = 2.0
    pnl_near_zero_abs: float = 0.05


THRESHOLDS = DisplayThresholds()

COLOR_RULES = {
    "success": ColorRule(foreground="#166534", background="#dcfce7"),
    "warning": ColorRule(foreground="#b45309", background="#ffedd5"),
    "danger": ColorRule(foreground="#b91c1c", background="#fee2e2"),
    "buy": ColorRule(foreground="#15803d", background="#ecfdf5"),
    "sell": ColorRule(foreground="#b91c1c", background="#fef2f2"),
    "neutral": ColorRule(foreground="#475569", background=""),
    "positive": ColorRule(foreground="#166534", background=""),
    "negative": ColorRule(foreground="#b91c1c", background=""),
}

STATUS_COLUMNS = {
    "status",
    "stage_status",
    "overall_status",
    "acceptance_status",
    "run_mode_label",
    "reuse_audit_status",
    "production_mode_label",
}

ACTION_COLUMNS = {
    "action",
    "next_day_action",
    "management_action",
    "position_status",
}

PNL_COLUMNS = {
    "market_change_pct",
    "unrealized_pnl_pct",
    "unrealized_pnl_amt",
}

RISK_COLUMNS = {
    "stop_loss_gap_pct",
    "target_gap_pct",
}

STATUS_FIELDS = {
    "orchestrator_status",
    "reports_status",
    "error_status",
    "alert_status",
    "current_stage",
    "summary_status",
}

SUCCESS_TOKENS = {"SUCCESS", "PASS", "通过", "成功"}
WARNING_TOKENS = {"SKIPPED", "WARNING", "已跳过", "警告", "提醒"}
DANGER_TOKENS = {"FAILED", "REJECT", "ERROR", "失败", "拒绝", "错误", "异常"}
BUY_TOKENS = {"BUY", "买", "建仓", "加仓"}
SELL_TOKENS = {"SELL", "卖", "减仓", "清仓"}
HOLD_TOKENS = {"HOLD", "WATCH", "持有", "观察", "正常跟踪", "正常持有"}


def get_foreground_brush(value: Any, column_name: str = "") -> QBrush | None:
    rule = _resolve_rule(value, column_name)
    if rule is None:
        return None
    return QBrush(QColor(rule.foreground))


def get_background_brush(value: Any, column_name: str = "") -> QBrush | None:
    normalized_column = column_name.strip().lower()
    if normalized_column not in STATUS_COLUMNS and normalized_column not in ACTION_COLUMNS:
        return None

    rule = _resolve_rule(value, normalized_column)
    if rule is None or not rule.background:
        return None
    return QBrush(QColor(rule.background))


def get_label_style(value: Any, field_name: str = "") -> str:
    rule = _resolve_rule(value, field_name)
    if rule is None:
        return "color: #0f172a;"
    return f"color: {rule.foreground}; font-weight: 600;"


def _resolve_rule(value: Any, column_name: str = "") -> ColorRule | None:
    if is_empty_value(value):
        return None

    normalized_column = column_name.strip().lower()
    text = str(value).strip()

    if normalized_column in STATUS_COLUMNS or normalized_column in STATUS_FIELDS:
        return _resolve_status_rule(text)

    if normalized_column in ACTION_COLUMNS:
        return _resolve_action_rule(text)

    if normalized_column in RISK_COLUMNS:
        return _resolve_risk_rule(value, normalized_column)

    if normalized_column in PNL_COLUMNS or "pnl" in normalized_column or normalized_column.endswith("change_pct"):
        return _resolve_pnl_rule(value)

    return None


def _resolve_status_rule(text: str) -> ColorRule | None:
    upper = text.upper()
    if any(token in upper for token in DANGER_TOKENS):
        return COLOR_RULES["danger"]
    if any(token in upper for token in WARNING_TOKENS):
        return COLOR_RULES["warning"]
    if any(token in upper for token in SUCCESS_TOKENS):
        return COLOR_RULES["success"]
    return None


def _resolve_action_rule(text: str) -> ColorRule | None:
    upper = text.upper()
    if any(token in upper for token in BUY_TOKENS):
        return COLOR_RULES["buy"]
    if any(token in upper for token in SELL_TOKENS):
        return COLOR_RULES["sell"]
    if any(token in upper for token in HOLD_TOKENS):
        return COLOR_RULES["neutral"]
    return None


def _resolve_risk_rule(value: Any, column_name: str) -> ColorRule | None:
    number = _to_float(value)
    if number is None:
        return None

    if column_name == "stop_loss_gap_pct" and number <= THRESHOLDS.stop_loss_near_pct:
        return COLOR_RULES["danger"]
    if column_name == "target_gap_pct" and number <= THRESHOLDS.target_near_pct:
        return COLOR_RULES["success"]
    return None


def _resolve_pnl_rule(value: Any) -> ColorRule | None:
    number = _to_float(value)
    if number is None:
        return None

    if number > THRESHOLDS.pnl_near_zero_abs:
        return COLOR_RULES["positive"]
    if number < -THRESHOLDS.pnl_near_zero_abs:
        return COLOR_RULES["negative"]
    return None


def _to_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    if isinstance(value, int):
        return float(value)
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value

    text = str(value).strip().replace(",", "")
    if not text:
        return None
    try:
        return float(Decimal(text))
    except (InvalidOperation, ValueError):
        return None
