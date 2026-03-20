# -*- coding: utf-8 -*-
"""
Created on Thu Mar 19 12:36:58 2026

@author: DELL
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


BOARD_LOT = 100


@dataclass(frozen=True)
class IntradayRecheckConfig:
    recheck_chase_limit_pct: float = 0.045
    recheck_reduce_chase_pct: float = 0.06
    recheck_break_stop_buffer_pct: float = 0.0
    reduced_execute_ratio: float = 0.5


def _norm_col(name: str) -> str:
    return "".join(ch for ch in str(name).strip().lower() if ch.isalnum())


def _resolve_column(df: pd.DataFrame, aliases: list[str], required: bool = False) -> str | None:
    mapping = {_norm_col(col): col for col in df.columns}
    for alias in aliases:
        hit = mapping.get(_norm_col(alias))
        if hit is not None:
            return hit
    if required:
        raise KeyError(f"缺少必要字段，候选别名: {aliases}；当前字段: {list(df.columns)}")
    return None


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"文件不存在: {path}")
    return pd.read_csv(path)


def _num(series: pd.Series | None, default: float | None = None, index=None) -> pd.Series:
    if series is None:
        if index is None:
            result = pd.Series(dtype=float)
        else:
            result = pd.Series(np.nan, index=index, dtype=float)
    else:
        result = pd.to_numeric(series, errors="coerce")
    if default is not None:
        result = result.fillna(default)
    return result


def _std_code(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().str.lower()


def _blank_series(index) -> pd.Series:
    return pd.Series([""] * len(index), index=index, dtype=object)


def _safe_int(value: Any, default: int = 0) -> int:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return default
    return int(float(numeric))


def _round_lot(shares: Any) -> int:
    numeric = pd.to_numeric(pd.Series([shares]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return 0
    numeric = float(numeric)
    if numeric <= 0:
        return 0
    value = int(numeric)
    lots = (value // BOARD_LOT) * BOARD_LOT
    if lots <= 0 and value > 0:
        return BOARD_LOT
    return lots


def _pick_trading_date(df: pd.DataFrame, explicit_date: str | None) -> str | None:
    if explicit_date:
        return explicit_date
    for col in ("trading_date", "trade_date"):
        if col in df.columns:
            series = df[col].dropna()
            if not series.empty:
                return str(series.iloc[0])
    return None


def _derive_prev_close(snapshot_df: pd.DataFrame, latest_series: pd.Series, index) -> tuple[pd.Series, str]:
    pct_col = _resolve_column(
        snapshot_df,
        ["pct_chg", "pctchg", "change_pct", "chg_pct", "涨跌幅", "涨跌幅%", "percent_change"],
    )
    chg_col = _resolve_column(
        snapshot_df,
        ["change", "chg", "price_change", "涨跌额", "diff"],
    )

    if pct_col:
        pct = _num(snapshot_df[pct_col], index=index)
        pct_ratio = pd.Series(np.where(np.abs(pct) > 2, pct / 100.0, pct), index=index, dtype=float)
        derived = pd.Series(np.nan, index=index, dtype=float)
        valid = latest_series.notna() & pct_ratio.notna() & ((1.0 + pct_ratio) != 0)
        derived.loc[valid] = latest_series.loc[valid] / (1.0 + pct_ratio.loc[valid])
        return derived, f"derived_from_{pct_col}"

    if chg_col:
        chg = _num(snapshot_df[chg_col], index=index)
        derived = pd.Series(np.nan, index=index, dtype=float)
        valid = latest_series.notna() & chg.notna()
        derived.loc[valid] = latest_series.loc[valid] - chg.loc[valid]
        return derived, f"derived_from_{chg_col}"

    return pd.Series(np.nan, index=index, dtype=float), "unavailable"


def _build_snapshot_view(snapshot_df: pd.DataFrame) -> pd.DataFrame:
    code_col = _resolve_column(snapshot_df, ["code", "ts_code", "stock_code"], required=True)
    name_col = _resolve_column(snapshot_df, ["name", "stock_name", "security_name"])
    open_col = _resolve_column(
        snapshot_df,
        ["open", "open_price", "open_px", "today_open", "今开", "开盘价"],
    )
    prev_close_col = _resolve_column(
        snapshot_df,
        ["prev_close", "pre_close", "preclose", "last_close", "昨收", "昨收价"],
    )
    latest_col = _resolve_column(
        snapshot_df,
        ["latest_price", "latest", "price", "close", "close_price", "current_price", "最新价", "现价"],
    )
    high_limit_col = _resolve_column(snapshot_df, ["high_limit", "up_limit", "limit_up", "涨停价"])
    low_limit_col = _resolve_column(snapshot_df, ["low_limit", "down_limit", "limit_down", "跌停价"])
    volume_col = _resolve_column(snapshot_df, ["volume", "vol", "成交量"])
    amount_col = _resolve_column(snapshot_df, ["amount", "turnover", "turnover_amount", "成交额"])
    paused_col = _resolve_column(snapshot_df, ["paused", "suspended", "is_paused", "停牌"])

    index = snapshot_df.index
    view = pd.DataFrame(index=index)
    view["code"] = _std_code(snapshot_df[code_col])

    if name_col:
        name_series = snapshot_df[name_col].where(pd.notna(snapshot_df[name_col]), "")
        view["snap_name"] = name_series.astype(str)
    else:
        view["snap_name"] = _blank_series(index)

    latest_series = _num(snapshot_df[latest_col] if latest_col else None, index=index)

    if open_col:
        open_series = _num(snapshot_df[open_col], index=index)
        open_source = open_col
    elif latest_col:
        open_series = latest_series.copy()
        open_source = f"proxy_from_{latest_col}"
    else:
        raise KeyError(
            f"缺少价格字段。当前字段: {list(snapshot_df.columns)}；"
            f"至少需要其一: open/open_price/open_px/latest_price/price/close/close_price/current_price"
        )

    if prev_close_col:
        prev_close_series = _num(snapshot_df[prev_close_col], index=index)
        prev_close_source = prev_close_col
    else:
        prev_close_series, prev_close_source = _derive_prev_close(snapshot_df, latest_series, index)

    if prev_close_series.isna().all():
        prev_close_series = open_series.copy()
        prev_close_source = "fallback_equal_open_price"

    amount_series = _num(snapshot_df[amount_col] if amount_col else None, default=0, index=index)

    if volume_col:
        volume_series = _num(snapshot_df[volume_col], default=0, index=index)
        volume_source = volume_col
    else:
        volume_series = pd.Series(np.where(amount_series > 0, 1.0, 0.0), index=index, dtype=float)
        volume_source = "proxy_from_turnover_amount" if amount_col else "missing"

    view["open_price"] = open_series
    view["prev_close"] = prev_close_series
    view["latest_price"] = latest_series.where(latest_series.notna(), open_series)
    view["high_limit"] = _num(snapshot_df[high_limit_col] if high_limit_col else None, index=index)
    view["low_limit"] = _num(snapshot_df[low_limit_col] if low_limit_col else None, index=index)
    view["volume"] = volume_series
    view["amount"] = amount_series

    if paused_col:
        raw = snapshot_df[paused_col]
        if pd.api.types.is_bool_dtype(raw):
            view["paused"] = raw.fillna(False)
        else:
            view["paused"] = raw.astype(str).str.lower().isin(["1", "true", "y", "yes"])
    else:
        view["paused"] = False

    view["open_price_source"] = open_source
    view["prev_close_source"] = prev_close_source
    view["volume_source"] = volume_source
    return view.drop_duplicates(subset=["code"], keep="last").reset_index(drop=True)


def _build_open_decision_view(open_df: pd.DataFrame, trading_date: str | None) -> pd.DataFrame:
    code_col = _resolve_column(open_df, ["code"], required=True)
    rank_col = _resolve_column(open_df, ["execution_rank", "rank", "portfolio_rank"])
    name_col = _resolve_column(open_df, ["name"])
    priority_level_col = _resolve_column(open_df, ["priority_level", "priority_bucket", "priority"])
    priority_score_col = _resolve_column(open_df, ["priority_score", "score", "execution_priority_score"])
    entry_col = _resolve_column(open_df, ["plan_entry_price", "entry_price", "buy_price"])
    stop_col = _resolve_column(open_df, ["stop_loss", "stoploss"])
    target_col = _resolve_column(open_df, ["target_price", "take_profit_price"])
    planned_shares_col = _resolve_column(open_df, ["planned_shares", "suggested_shares", "shares"])
    order_action_col = _resolve_column(open_df, ["order_action"])
    order_shares_col = _resolve_column(open_df, ["order_shares"])
    order_price_col = _resolve_column(open_df, ["order_price"])
    stage06_decision_col = _resolve_column(open_df, ["decision"])
    stage06_reason_col = _resolve_column(open_df, ["decision_reason"])
    trade_date = _pick_trading_date(open_df, trading_date)

    index = open_df.index
    view = pd.DataFrame(index=index)
    view["trading_date"] = trade_date
    if rank_col:
        rank = pd.to_numeric(open_df[rank_col], errors="coerce")
        fallback = pd.Series(np.arange(1, len(open_df) + 1), index=index, dtype=int)
        rank = rank.where(rank.notna(), fallback.astype(float)).astype(int)
        view["execution_rank"] = rank
    else:
        view["execution_rank"] = pd.Series(np.arange(1, len(open_df) + 1), index=index, dtype=int)

    view["code"] = _std_code(open_df[code_col])

    if name_col:
        view["name"] = open_df[name_col].where(pd.notna(open_df[name_col]), "").astype(str)
    else:
        view["name"] = _blank_series(index)

    if priority_level_col:
        view["priority_level"] = open_df[priority_level_col].where(pd.notna(open_df[priority_level_col]), "").astype(str)
    else:
        view["priority_level"] = _blank_series(index)

    view["priority_score"] = _num(open_df[priority_score_col] if priority_score_col else None, index=index)
    view["plan_entry_price"] = _num(open_df[entry_col] if entry_col else None, index=index)
    view["stop_loss"] = _num(open_df[stop_col] if stop_col else None, index=index)
    view["target_price"] = _num(open_df[target_col] if target_col else None, index=index)
    view["planned_shares"] = _num(open_df[planned_shares_col] if planned_shares_col else None, default=0, index=index)

    if order_action_col:
        view["stage06_order_action"] = open_df[order_action_col].where(pd.notna(open_df[order_action_col]), "").astype(str)
    else:
        view["stage06_order_action"] = _blank_series(index)

    view["stage06_order_shares"] = _num(open_df[order_shares_col] if order_shares_col else None, default=0, index=index)
    view["stage06_order_price"] = _num(open_df[order_price_col] if order_price_col else None, index=index)

    if stage06_decision_col:
        view["stage06_decision"] = open_df[stage06_decision_col].where(pd.notna(open_df[stage06_decision_col]), "").astype(str)
    else:
        view["stage06_decision"] = _blank_series(index)

    if stage06_reason_col:
        view["stage06_reason"] = open_df[stage06_reason_col].where(pd.notna(open_df[stage06_reason_col]), "").astype(str)
    else:
        view["stage06_reason"] = _blank_series(index)

    return view.reset_index(drop=True)


class IntradayRecheckManager:
    def __init__(self, base_dir: str | Path | None = None, output_dir: str | Path | None = None) -> None:
        self.base_dir = Path(base_dir) if base_dir else Path(__file__).resolve().parents[1]
        self.output_dir = Path(output_dir) if output_dir else self.base_dir / "reports"
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def run(
        self,
        trading_date: str | None = None,
        target_trading_date: str | None = None,
        trade_date: str | None = None,
        open_execution_decision_path: str | Path | None = None,
        open_execution_path: str | Path | None = None,
        decision_path: str | Path | None = None,
        market_snapshot_path: str | Path | None = None,
        market_snapshot_file: str | Path | None = None,
        output_dir: str | Path | None = None,
        reports_dir: str | Path | None = None,
        project_dir: str | Path | None = None,
        root_dir: str | Path | None = None,
        recheck_chase_limit_pct: float = 0.045,
        recheck_reduce_chase_pct: float = 0.06,
        recheck_break_stop_buffer_pct: float = 0.0,
        reduced_execute_ratio: float = 0.5,
        **_: Any,
    ) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
        resolved_base_dir = Path(project_dir or root_dir or self.base_dir)
        resolved_output_dir = Path(output_dir or reports_dir or self.output_dir)
        resolved_output_dir.mkdir(parents=True, exist_ok=True)

        effective_trade_date = trading_date or trade_date or target_trading_date
        config = IntradayRecheckConfig(
            recheck_chase_limit_pct=recheck_chase_limit_pct,
            recheck_reduce_chase_pct=recheck_reduce_chase_pct,
            recheck_break_stop_buffer_pct=recheck_break_stop_buffer_pct,
            reduced_execute_ratio=reduced_execute_ratio,
        )

        open_decision_file = Path(
            open_execution_decision_path
            or open_execution_path
            or decision_path
            or (resolved_output_dir / "daily_open_execution_decision.csv")
        )
        snapshot_file = Path(market_snapshot_path or market_snapshot_file or (resolved_output_dir / "market_signal_snapshot.csv"))

        open_df = _read_csv(open_decision_file)
        snapshot_df = _read_csv(snapshot_file)

        open_view = _build_open_decision_view(open_df, effective_trade_date)
        snapshot_view = _build_snapshot_view(snapshot_df)
        effective_trade_date = _pick_trading_date(open_view, effective_trade_date)
        open_view["trading_date"] = effective_trade_date

        merged = open_view.merge(snapshot_view, on="code", how="left")

        merged["name"] = merged["name"].fillna("").astype(str)
        merged["snap_name"] = merged["snap_name"].fillna("").astype(str)
        merged.loc[merged["name"].isin(["", "nan", "None"]), "name"] = merged["snap_name"]

        merged["recheck_price"] = merged["latest_price"].where(merged["latest_price"].notna(), merged["open_price"])
        merged["recheck_gap_pct"] = np.where(
            merged["recheck_price"].notna() & merged["prev_close"].notna() & (merged["prev_close"] != 0),
            merged["recheck_price"] / merged["prev_close"] - 1.0,
            np.nan,
        )
        merged["recheck_slippage_vs_entry_pct"] = np.where(
            merged["recheck_price"].notna() & merged["plan_entry_price"].notna() & (merged["plan_entry_price"] != 0),
            merged["recheck_price"] / merged["plan_entry_price"] - 1.0,
            np.nan,
        )

        decisions: list[dict[str, Any]] = []
        generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        for row in merged.to_dict(orient="records"):
            stage06_action = str(row.get("stage06_order_action", "") or "").upper()
            planned_shares = _round_lot(row.get("planned_shares"))
            recheck_price = row.get("recheck_price")
            stop_loss = row.get("stop_loss")
            high_limit = row.get("high_limit")
            low_limit = row.get("low_limit")
            gap_pct = row.get("recheck_gap_pct")
            slippage_pct = row.get("recheck_slippage_vs_entry_pct")
            volume = float(row.get("volume", 0) or 0)
            paused = bool(row.get("paused", False))

            decision = "NO_ACTION"
            decision_reason = ""
            order_action = "NONE"
            execute_ratio = 0.0

            if paused or pd.isna(recheck_price) or float(recheck_price) <= 0:
                decision = "KEEP_WATCH_NO_PRICE"
                decision_reason = "无有效盘中价格，维持观察"
            elif pd.notna(low_limit) and float(recheck_price) <= float(low_limit) * 1.001:
                decision = "CANCEL_RECHECK_LIMIT_DOWN"
                decision_reason = "盘中接近跌停，取消执行"
            elif pd.notna(stop_loss) and float(recheck_price) <= float(stop_loss) * (1.0 + config.recheck_break_stop_buffer_pct):
                decision = "CANCEL_RECHECK_BROKEN_STOP"
                decision_reason = "盘中价格跌破止损，取消执行"
            elif pd.notna(high_limit) and float(recheck_price) >= float(high_limit) * 0.999:
                decision = "DEFER_RECHECK_LIMIT_UP"
                decision_reason = "盘中接近涨停，避免追价"
            elif stage06_action == "BUY":
                if pd.notna(slippage_pct) and float(slippage_pct) >= config.recheck_reduce_chase_pct:
                    decision = "REDUCE_BUY_RECHECK"
                    decision_reason = "盘中追价偏离扩大，降为半仓执行"
                    order_action = "BUY"
                    execute_ratio = config.reduced_execute_ratio
                else:
                    decision = "CONFIRM_BUY_RECHECK"
                    decision_reason = "盘中价格仍在容忍区间内，确认执行"
                    order_action = "BUY"
                    execute_ratio = 1.0
            else:
                if pd.notna(slippage_pct) and float(slippage_pct) <= config.recheck_chase_limit_pct:
                    decision = "EXECUTE_BUY_RECHECK"
                    decision_reason = "盘中价格回到可执行区间，触发二次确认买入"
                    order_action = "BUY"
                    execute_ratio = 1.0
                elif pd.notna(slippage_pct) and float(slippage_pct) <= config.recheck_reduce_chase_pct:
                    decision = "EXECUTE_BUY_RECHECK_REDUCED"
                    decision_reason = "盘中价格略高，半仓执行"
                    order_action = "BUY"
                    execute_ratio = config.reduced_execute_ratio
                else:
                    decision = "KEEP_WATCH_RECHECK"
                    decision_reason = "盘中价格仍不理想，继续观察"

            if order_action == "BUY" and volume <= 0:
                decision = "KEEP_WATCH_NO_VOLUME"
                decision_reason = "无有效成交存在信号，维持观察"
                order_action = "NONE"
                execute_ratio = 0.0

            order_shares = _round_lot(planned_shares * execute_ratio) if order_action == "BUY" else 0
            if order_action == "BUY" and order_shares == 0 and planned_shares >= BOARD_LOT:
                order_shares = BOARD_LOT

            order_price = float(recheck_price) if order_action == "BUY" and pd.notna(recheck_price) else np.nan
            order_amt = round(float(order_price) * order_shares, 2) if order_shares > 0 and pd.notna(order_price) else 0.0

            decisions.append(
                {
                    "trading_date": effective_trade_date,
                    "execution_rank": _safe_int(row.get("execution_rank")),
                    "code": row.get("code"),
                    "name": row.get("name"),
                    "priority_level": row.get("priority_level"),
                    "priority_score": row.get("priority_score"),
                    "plan_entry_price": row.get("plan_entry_price"),
                    "recheck_price": recheck_price,
                    "open_price": row.get("open_price"),
                    "latest_price": row.get("latest_price"),
                    "prev_close": row.get("prev_close"),
                    "open_price_source": row.get("open_price_source"),
                    "prev_close_source": row.get("prev_close_source"),
                    "volume_source": row.get("volume_source"),
                    "recheck_gap_pct": row.get("recheck_gap_pct"),
                    "recheck_slippage_vs_entry_pct": row.get("recheck_slippage_vs_entry_pct"),
                    "stop_loss": row.get("stop_loss"),
                    "target_price": row.get("target_price"),
                    "planned_shares": planned_shares,
                    "stage06_order_action": row.get("stage06_order_action"),
                    "stage06_decision": row.get("stage06_decision"),
                    "stage06_reason": row.get("stage06_reason"),
                    "order_action": order_action,
                    "execute_ratio": round(float(execute_ratio), 4),
                    "order_shares": int(order_shares),
                    "order_price": order_price,
                    "order_amt": order_amt,
                    "decision": decision,
                    "decision_reason": decision_reason,
                    "generated_at": generated_at,
                }
            )

        decision_df = pd.DataFrame(decisions).sort_values(by=["execution_rank", "code"], kind="stable").reset_index(drop=True)
        orders_df = decision_df.loc[
            decision_df["order_action"].astype(str).eq("BUY") & decision_df["order_shares"].gt(0),
            [
                "trading_date",
                "execution_rank",
                "code",
                "name",
                "order_action",
                "order_price",
                "order_shares",
                "order_amt",
                "decision",
                "decision_reason",
                "generated_at",
            ],
        ].reset_index(drop=True)

        decision_path_out = resolved_output_dir / "daily_intraday_recheck_decision.csv"
        orders_path_out = resolved_output_dir / "daily_intraday_recheck_orders.csv"
        summary_path_out = resolved_output_dir / "daily_intraday_recheck_summary.txt"

        decision_df.to_csv(decision_path_out, index=False, encoding="utf-8-sig")
        orders_df.to_csv(orders_path_out, index=False, encoding="utf-8-sig")

        summary = {
            "trading_date": effective_trade_date,
            "decision_count": int(len(decision_df)),
            "buy_order_count": int(len(orders_df)),
            "buy_order_amt": round(float(orders_df["order_amt"].sum()) if not orders_df.empty else 0.0, 2),
            "confirm_count": int(decision_df["decision"].astype(str).str.startswith("CONFIRM").sum()) if not decision_df.empty else 0,
            "execute_count": int(decision_df["decision"].astype(str).str.startswith("EXECUTE").sum()) if not decision_df.empty else 0,
            "cancel_count": int(decision_df["decision"].astype(str).str.startswith("CANCEL").sum()) if not decision_df.empty else 0,
            "watch_count": int(
                decision_df["decision"].astype(str).str.startswith("KEEP").sum()
                + decision_df["decision"].astype(str).str.startswith("DEFER").sum()
            ) if not decision_df.empty else 0,
            "generated_at": generated_at,
            "decision_path": str(decision_path_out),
            "orders_path": str(orders_path_out),
            "summary_path": str(summary_path_out),
            "open_execution_decision_path": str(open_decision_file),
            "market_snapshot_path": str(snapshot_file),
            "base_dir": str(resolved_base_dir),
        }

        lines = [
            "============================================================",
            "盘中二次确认完成",
            f"目标交易日: {effective_trade_date}",
            f"决策标的数: {summary['decision_count']}",
            f"买入委托数: {summary['buy_order_count']}",
            f"买入总金额: {summary['buy_order_amt']}",
            f"确认执行数: {summary['confirm_count']}",
            f"触发买入数: {summary['execute_count']}",
            f"取消执行数: {summary['cancel_count']}",
            f"观察延后数: {summary['watch_count']}",
            f"决策文件  : {decision_path_out}",
            f"委托文件  : {orders_path_out}",
            "============================================================",
        ]
        summary_path_out.write_text("\n".join(lines), encoding="utf-8")
        return decision_df, orders_df, summary


def _merge_positional_args(args: tuple[Any, ...], kwargs: dict[str, Any]) -> dict[str, Any]:
    merged = dict(kwargs)
    positional_names = [
        "trading_date",
        "open_execution_decision_path",
        "market_snapshot_path",
        "output_dir",
        "base_dir",
    ]
    for idx, value in enumerate(args):
        if idx < len(positional_names) and positional_names[idx] not in merged:
            merged[positional_names[idx]] = value
    return merged


def main(*args: Any, **kwargs: Any) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    merged_kwargs = _merge_positional_args(args, kwargs)
    manager = IntradayRecheckManager(
        base_dir=merged_kwargs.get("base_dir") or merged_kwargs.get("project_dir") or merged_kwargs.get("root_dir"),
        output_dir=merged_kwargs.get("output_dir") or merged_kwargs.get("reports_dir"),
    )
    return manager.run(**merged_kwargs)


def run(*args: Any, **kwargs: Any) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    return main(*args, **kwargs)


def generate_intraday_recheck(*args: Any, **kwargs: Any) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    return main(*args, **kwargs)


def execute_intraday_recheck(*args: Any, **kwargs: Any) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    return main(*args, **kwargs)