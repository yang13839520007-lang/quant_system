from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd


def _load_optional_settings():
    try:
        from config import settings  # type: ignore
        return settings
    except Exception:
        return None


def _get_setting(name: str, default: Any) -> Any:
    settings = _load_optional_settings()
    if settings is not None and hasattr(settings, name):
        return getattr(settings, name)
    return default


def _pick_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    lower_map = {str(col).strip().lower(): col for col in df.columns}
    for name in candidates:
        hit = lower_map.get(name.strip().lower())
        if hit is not None:
            return hit
    return None


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
        if pd.isna(out):
            return default
        return out
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        out = int(float(value))
        return out
    except (TypeError, ValueError):
        return default


def _normalize_code(code: Any) -> str:
    text = str(code).strip().lower()
    if "." in text:
        left, right = text.split(".", 1)
        if left in {"sh", "sz", "bj"}:
            return f"{left}.{right}"
        if right in {"sh", "sz", "bj"}:
            return f"{right}.{left}"
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) != 6:
        return text
    if digits.startswith(("600", "601", "603", "605", "688", "689")):
        return f"sh.{digits}"
    if digits.startswith(("430", "831", "832", "833", "834", "835", "836", "837", "838", "839", "870", "871", "872", "873", "874", "875", "876", "877", "878", "879", "920")):
        return f"bj.{digits}"
    return f"sz.{digits}"


def _contains_st(name: Any) -> bool:
    text = str(name or "").upper().replace(" ", "")
    return "ST" in text


def _is_near(value: float, target: float, tolerance: float = 0.01) -> bool:
    return abs(value - target) <= tolerance


def _is_truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    return text in {"1", "true", "yes", "y"}


@dataclass(frozen=True)
class ExecutionCostModel:
    buy_slippage_bps: float = 8.0
    sell_slippage_bps: float = 8.0
    commission_rate: float = 0.0003
    min_commission: float = 5.0
    stamp_tax_rate: float = 0.001
    transfer_fee_rate: float = 0.0


@dataclass(frozen=True)
class ExecutionAuditConfig:
    board_lot: int = 100
    block_st_buy: bool = True
    require_execution_ready_snapshot: bool = True
    cost_model: ExecutionCostModel = ExecutionCostModel()


class AShareExecutionAuditor:
    """Production-grade A-share execution authenticity audit."""

    def __init__(self, config: ExecutionAuditConfig | None = None) -> None:
        self.config = config or self._build_default_config()

    def audit_orders(
        self,
        trading_date: str,
        orders_df: pd.DataFrame,
        snapshot_df: pd.DataFrame,
        positions_df: pd.DataFrame | None = None,
    ) -> tuple[pd.DataFrame, dict[str, Any]]:
        prepared_orders = self._prepare_orders(orders_df)
        prepared_snapshot = self._prepare_snapshot(snapshot_df)
        prepared_positions = self._prepare_positions(positions_df)

        if prepared_orders.empty:
            empty_df = pd.DataFrame(columns=self._output_columns())
            return empty_df, self._build_summary(trading_date=trading_date, audit_df=empty_df)

        merged = prepared_orders.merge(prepared_snapshot, on="code", how="left", suffixes=("", "_snapshot"))
        merged = merged.merge(prepared_positions, on="code", how="left")
        merged["available_qty"] = pd.to_numeric(merged.get("available_qty", 0), errors="coerce").fillna(0).astype(int)

        records: list[dict[str, Any]] = []
        for _, row in merged.iterrows():
            records.append(self._audit_single_row(trading_date=trading_date, row=row))

        audit_df = pd.DataFrame(records)
        return audit_df, self._build_summary(trading_date=trading_date, audit_df=audit_df)

    def _audit_single_row(self, trading_date: str, row: pd.Series) -> dict[str, Any]:
        action = str(row.get("action", "")).strip().upper()
        code = _normalize_code(row.get("code", ""))
        order_shares = max(_safe_int(row.get("order_shares", 0)), 0)
        requested_price = _safe_float(row.get("order_price", 0.0), default=0.0)
        order_type = str(row.get("order_type", "")).strip().upper()
        name = str(row.get("name_snapshot", "") or row.get("name", "")).strip()

        violations: list[str] = []
        messages: list[str] = []

        if self.config.block_st_buy and action == "BUY" and _contains_st(name):
            violations.append("ST_SECURITY_BUY_BLOCKED")
            messages.append("ST/*ST 标的默认不进入买入执行。")

        if _is_truthy(row.get("paused", False)):
            violations.append("TRADE_SUSPENDED")
            messages.append("标的处于停牌状态，不允许下单。")

        if self.config.require_execution_ready_snapshot:
            snapshot_quality = str(row.get("snapshot_quality", "")).strip().upper()
            source_trade_date = str(row.get("source_trade_date", "")).strip()
            if snapshot_quality != "EXECUTION_READY" or (source_trade_date and source_trade_date != trading_date):
                violations.append("ADJUSTED_OR_STALE_PRICE_DATA")
                messages.append("行情并非当日可执行快照，疑似前复权/回放代理数据，不允许直接执行。")

        if action == "SELL":
            available_qty = max(_safe_int(row.get("available_qty", 0)), 0)
            if order_shares > available_qty:
                violations.append("T1_SELL_LOCKED")
                messages.append("卖出数量超过可用仓位，命中 A 股 T+1 锁仓约束。")

        open_price = _safe_float(row.get("open_price", 0.0), default=0.0)
        high_price = _safe_float(row.get("high_price", open_price), default=open_price)
        low_price = _safe_float(row.get("low_price", open_price), default=open_price)
        high_limit = _safe_float(row.get("high_limit", 0.0), default=0.0)
        low_limit = _safe_float(row.get("low_limit", 0.0), default=0.0)
        if action == "BUY" and high_limit > 0 and open_price > 0:
            if _is_near(open_price, high_limit) and _is_near(low_price, high_limit):
                violations.append("ONE_WORD_LIMIT_UP_BUY_BLOCKED")
                messages.append("一字涨停买不到，买入单阻断。")
        if action == "SELL" and low_limit > 0 and open_price > 0:
            if _is_near(open_price, low_limit) and _is_near(high_price, low_limit):
                violations.append("ONE_WORD_LIMIT_DOWN_SELL_BLOCKED")
                messages.append("一字跌停卖不掉，卖出单阻断。")

        audited_fill_price = self._estimate_fill_price(action=action, requested_price=requested_price)
        gross_amount = round(audited_fill_price * order_shares, 2)
        commission = self._estimate_commission(gross_amount)
        stamp_tax = round(gross_amount * self.config.cost_model.stamp_tax_rate, 2) if action == "SELL" else 0.0
        transfer_fee = self._estimate_transfer_fee(code=code, gross_amount=gross_amount)
        total_fee = round(commission + stamp_tax + transfer_fee, 2)
        slippage_amt = round(abs(audited_fill_price - requested_price) * order_shares, 2)
        cash_impact = round(gross_amount + total_fee, 2) if action == "BUY" else round(gross_amount - total_fee, 2)

        audit_status = "BLOCKED" if violations else "PASS"
        return {
            "trading_date": trading_date,
            "code": code,
            "name": name,
            "action": action,
            "order_type": order_type,
            "requested_order_shares": order_shares,
            "requested_order_price": round(requested_price, 4),
            "audit_status": audit_status,
            "violation_count": len(violations),
            "violation_codes": "|".join(violations),
            "violation_message": " | ".join(messages),
            "available_qty": max(_safe_int(row.get("available_qty", 0)), 0),
            "snapshot_quality": str(row.get("snapshot_quality", "")).strip(),
            "source_trade_date": str(row.get("source_trade_date", "")).strip(),
            "paused": bool(_is_truthy(row.get("paused", False))),
            "audited_fill_price": round(audited_fill_price, 4),
            "gross_amount": gross_amount,
            "slippage_amount": slippage_amt,
            "commission": commission,
            "stamp_tax": stamp_tax,
            "transfer_fee": transfer_fee,
            "total_fee": total_fee,
            "cash_impact": cash_impact,
            "order_shares": order_shares if not violations else 0,
            "order_price": round(requested_price, 4),
        }

    def _prepare_orders(self, orders_df: pd.DataFrame) -> pd.DataFrame:
        if orders_df is None or orders_df.empty:
            return pd.DataFrame(columns=["code", "action", "order_shares", "order_price", "order_type"])
        out = orders_df.copy()
        out["code"] = out["code"].map(_normalize_code)
        return out

    def _prepare_snapshot(self, snapshot_df: pd.DataFrame) -> pd.DataFrame:
        if snapshot_df is None or snapshot_df.empty:
            return pd.DataFrame(columns=["code"])
        out = snapshot_df.copy()
        out["code"] = out["code"].map(_normalize_code)
        return out

    def _prepare_positions(self, positions_df: pd.DataFrame | None) -> pd.DataFrame:
        if positions_df is None or positions_df.empty:
            return pd.DataFrame(columns=["code", "available_qty"])
        work = positions_df.copy()
        code_col = _pick_col(work, ["code", "symbol", "ts_code"])
        available_col = _pick_col(work, ["available_qty", "sellable_qty", "tradable_qty"])
        if code_col is None:
            return pd.DataFrame(columns=["code", "available_qty"])
        if available_col is None:
            work["available_qty"] = 0
            available_col = "available_qty"
        grouped = (
            work.assign(
                code=work[code_col].map(_normalize_code),
                available_qty=pd.to_numeric(work[available_col], errors="coerce").fillna(0).astype(int),
            )
            .groupby("code", as_index=False)["available_qty"]
            .sum()
        )
        return grouped

    def _estimate_fill_price(self, action: str, requested_price: float) -> float:
        if requested_price <= 0:
            return 0.0
        if action == "SELL":
            return round(requested_price * (1 - self.config.cost_model.sell_slippage_bps / 10000.0), 4)
        return round(requested_price * (1 + self.config.cost_model.buy_slippage_bps / 10000.0), 4)

    def _estimate_commission(self, gross_amount: float) -> float:
        if gross_amount <= 0:
            return 0.0
        fee = round(gross_amount * self.config.cost_model.commission_rate, 2)
        return round(max(fee, self.config.cost_model.min_commission), 2)

    def _estimate_transfer_fee(self, code: str, gross_amount: float) -> float:
        if gross_amount <= 0 or self.config.cost_model.transfer_fee_rate <= 0:
            return 0.0
        if not code.startswith(("sh.", "bj.")):
            return 0.0
        return round(gross_amount * self.config.cost_model.transfer_fee_rate, 2)

    def _build_summary(self, trading_date: str, audit_df: pd.DataFrame) -> dict[str, Any]:
        blocked_df = audit_df[audit_df["audit_status"] == "BLOCKED"] if not audit_df.empty else audit_df
        return {
            "trading_date": trading_date,
            "total_orders": int(len(audit_df)),
            "passed_orders": int((audit_df["audit_status"] == "PASS").sum()) if not audit_df.empty else 0,
            "blocked_orders": int((audit_df["audit_status"] == "BLOCKED").sum()) if not audit_df.empty else 0,
            "blocked_by_code": blocked_df["violation_codes"].value_counts(dropna=False).to_dict() if not blocked_df.empty else {},
            "estimated_total_fee": float(audit_df["total_fee"].sum()) if not audit_df.empty else 0.0,
            "estimated_total_slippage": float(audit_df["slippage_amount"].sum()) if not audit_df.empty else 0.0,
        }

    def _output_columns(self) -> list[str]:
        return [
            "trading_date",
            "code",
            "name",
            "action",
            "order_type",
            "requested_order_shares",
            "requested_order_price",
            "audit_status",
            "violation_count",
            "violation_codes",
            "violation_message",
            "available_qty",
            "snapshot_quality",
            "source_trade_date",
            "paused",
            "audited_fill_price",
            "gross_amount",
            "slippage_amount",
            "commission",
            "stamp_tax",
            "transfer_fee",
            "total_fee",
            "cash_impact",
            "order_shares",
            "order_price",
        ]

    def _build_default_config(self) -> ExecutionAuditConfig:
        return ExecutionAuditConfig(
            board_lot=int(_get_setting("LOT_SIZE", 100)),
            block_st_buy=bool(_get_setting("EXECUTION_AUDIT_BLOCK_ST_BUY", True)),
            require_execution_ready_snapshot=bool(_get_setting("EXECUTION_AUDIT_REQUIRE_EXECUTION_READY", True)),
            cost_model=ExecutionCostModel(
                buy_slippage_bps=float(_get_setting("EXECUTION_AUDIT_BUY_SLIPPAGE_BPS", 8.0)),
                sell_slippage_bps=float(_get_setting("EXECUTION_AUDIT_SELL_SLIPPAGE_BPS", 8.0)),
                commission_rate=float(_get_setting("COMMISSION_RATE", 0.0003)),
                min_commission=float(_get_setting("EXECUTION_AUDIT_MIN_COMMISSION", 5.0)),
                stamp_tax_rate=float(_get_setting("STAMP_TAX_RATE", 0.001)),
                transfer_fee_rate=float(_get_setting("EXECUTION_AUDIT_TRANSFER_FEE_RATE", 0.0)),
            ),
        )
