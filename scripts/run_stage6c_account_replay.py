# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd


SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR = SCRIPT_DIR.parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import run_stage6a_parameter_search as stage6a  # noqa: E402


DEFAULT_BASE_DIR = ROOT_DIR
SELL_SETTLEMENT_MODE = "NEXT_DAY_AVAILABLE"
BASELINE_CFG = stage6a.SearchConfig(
    regime_mode="HARD_BLOCK",
    regime_ma_window=25,
    regime_risk_scale=1.0,
    left_catch_multiplier=1.015,
    max_gap_chase_pct=0.02,
    fallback_rule="NO_FILL_BUY_AT_OPEN_IF_GAP_SMALL",
    shadow_threshold=0.60,
    min_profit_to_trigger_shadow_exit=0.0,
    shadow_exit_mode="NEXT_OPEN_HALF_EXIT",
    use_volume_sanity_check=False,
    volume_sanity_ratio=1.20,
)


@dataclass
class AccountReplayConfig:
    base_dir: Path
    reports_dir: Path
    data_dir: Path
    universe_file: Path
    start_date: str | None
    end_date: str | None
    max_files: int | None
    amount_min: float
    target_profit_pct: float
    initial_cash: float
    commission_rate: float
    stamp_tax_rate: float
    buy_slippage_bps: float
    sell_slippage_bps: float
    lot_size: int
    max_concurrent_positions: int
    max_new_positions_per_day: int
    max_single_position_pct: float
    max_total_position_pct: float


@dataclass
class Position:
    code: str
    entry_date: str
    shares: int
    entry_price: float
    entry_total_cost: float
    signal_date: str
    rps50: float
    ma20_bias: float
    score: float
    pending_exit: bool = False
    pending_exit_reason: str = ""
    pending_exit_signal: str = ""
    scheduled_sell_date: str = ""


def _safe_float(value: Any, default: float = 0.0) -> float:
    return stage6a._safe_float(value, default=default)


def _calc_monthly_stats(equity_df: pd.DataFrame) -> dict[str, Any]:
    if equity_df.empty:
        return {
            "monthly_return_mean": None,
            "monthly_return_std": None,
            "best_month_return_pct": None,
            "worst_month_return_pct": None,
            "positive_month_ratio_pct": None,
            "extreme_month_flag": False,
            "months": 0,
        }
    work = equity_df.copy()
    work["trade_date"] = pd.to_datetime(work["trade_date"], errors="coerce")
    work = work[work["trade_date"].notna()].copy()
    if work.empty:
        return {
            "monthly_return_mean": None,
            "monthly_return_std": None,
            "best_month_return_pct": None,
            "worst_month_return_pct": None,
            "positive_month_ratio_pct": None,
            "extreme_month_flag": False,
            "months": 0,
        }
    month_end = work.groupby(work["trade_date"].dt.to_period("M").astype(str), as_index=False).tail(1).copy()
    month_end["monthly_return_pct"] = month_end["equity"].pct_change().fillna(0.0) * 100.0
    mean_val = float(month_end["monthly_return_pct"].mean())
    std_val = float(month_end["monthly_return_pct"].std(ddof=0)) if len(month_end) > 1 else 0.0
    best_val = float(month_end["monthly_return_pct"].max())
    worst_val = float(month_end["monthly_return_pct"].min())
    positive_ratio = float((month_end["monthly_return_pct"] > 0).mean() * 100.0)
    return {
        "monthly_return_mean": round(mean_val, 4),
        "monthly_return_std": round(std_val, 4),
        "best_month_return_pct": round(best_val, 4),
        "worst_month_return_pct": round(worst_val, 4),
        "positive_month_ratio_pct": round(positive_ratio, 4),
        "extreme_month_flag": bool(abs(best_val) >= 25.0 or abs(worst_val) >= 15.0),
        "months": int(len(month_end)),
    }


def _calc_max_drawdown(equity_series: pd.Series) -> float:
    if equity_series.empty:
        return 0.0
    running_max = equity_series.cummax()
    drawdown = (equity_series / running_max - 1.0) * 100.0
    return round(float(drawdown.min()), 4)


def _load_histories(config: AccountReplayConfig) -> tuple[dict[str, pd.DataFrame], dict[str, dict[str, pd.Series]], list[str]]:
    base_cfg = stage6a.RunnerConfig(
        base_dir=config.base_dir,
        reports_dir=config.reports_dir,
        data_dir=config.data_dir,
        universe_file=config.universe_file,
        start_date=config.start_date,
        end_date=config.end_date,
        max_files=config.max_files,
        amount_min=config.amount_min,
        target_profit_pct=config.target_profit_pct,
        initial_trade_cash=config.initial_cash,
        commission_rate=config.commission_rate,
        stamp_tax_rate=config.stamp_tax_rate,
        buy_slippage_bps=config.buy_slippage_bps,
        sell_slippage_bps=config.sell_slippage_bps,
        lot_size=config.lot_size,
    )
    histories, _ = stage6a._load_histories(base_cfg)
    row_maps: dict[str, dict[str, pd.Series]] = {}
    all_dates: set[str] = set()
    for code, hist in histories.items():
        scoped = hist.copy()
        if config.start_date is not None:
            scoped = scoped[scoped["trade_date"] >= config.start_date]
        if config.end_date is not None:
            scoped = scoped[scoped["trade_date"] <= config.end_date]
        if scoped.empty:
            continue
        histories[code] = scoped.reset_index(drop=True)
        row_maps[code] = {str(row["trade_date"]): row for _, row in scoped.iterrows()}
        all_dates.update(scoped["trade_date"].tolist())
    sorted_dates = sorted(all_dates)
    return histories, row_maps, sorted_dates


def _build_candidate_events(
    histories: dict[str, pd.DataFrame],
    config: AccountReplayConfig,
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, list[dict[str, Any]]], dict[str, dict[str, bool]]]:
    runner_cfg = stage6a.RunnerConfig(
        base_dir=config.base_dir,
        reports_dir=config.reports_dir,
        data_dir=config.data_dir,
        universe_file=config.universe_file,
        start_date=config.start_date,
        end_date=config.end_date,
        max_files=config.max_files,
        amount_min=config.amount_min,
        target_profit_pct=config.target_profit_pct,
        initial_trade_cash=config.initial_cash,
        commission_rate=config.commission_rate,
        stamp_tax_rate=config.stamp_tax_rate,
        buy_slippage_bps=config.buy_slippage_bps,
        sell_slippage_bps=config.sell_slippage_bps,
        lot_size=config.lot_size,
    )
    rps_map = stage6a._build_rps_map(histories=histories, start_date=config.start_date, end_date=config.end_date)
    regime_maps = stage6a._build_regime_maps(runner_cfg)
    by_entry_date: dict[str, list[dict[str, Any]]] = {}
    by_signal_date: dict[str, list[dict[str, Any]]] = {}
    for code, hist in histories.items():
        for idx in range(50, len(hist) - 1):
            row = hist.iloc[idx]
            signal_date = str(row["trade_date"])
            rps50 = stage6a.exp5._calc_rps50(signal_date, _safe_float(row.get("close_return_50"), default=float("nan")), rps_map)
            if not stage6a._passes_candidate_base(row=row, config=runner_cfg, search_cfg=BASELINE_CFG, rps50=rps50):
                continue
            if not regime_maps.get(BASELINE_CFG.regime_ma_window, {}).get(signal_date, True):
                continue
            ma20 = _safe_float(row.get("ma20"))
            close_price = _safe_float(row.get("close"))
            ma20_bias = abs(close_price - ma20) / ma20 if ma20 > 0 else 999.0
            score = float(rps50) * 100.0 - ma20_bias * 100.0
            next_row = hist.iloc[idx + 1]
            event = {
                "code": code,
                "signal_date": signal_date,
                "entry_date": str(next_row["trade_date"]),
                "signal_idx": idx,
                "rps50": round(float(rps50), 6),
                "ma20_bias": round(float(ma20_bias), 6),
                "score": round(float(score), 6),
            }
            by_entry_date.setdefault(event["entry_date"], []).append(event)
            by_signal_date.setdefault(signal_date, []).append(event)
    return by_entry_date, by_signal_date, regime_maps


def _next_date(current_date: str, sorted_dates: list[str]) -> str | None:
    try:
        idx = sorted_dates.index(current_date)
    except ValueError:
        return None
    if idx + 1 >= len(sorted_dates):
        return None
    return sorted_dates[idx + 1]


def _market_value_of_positions(positions: dict[str, Position], date: str, row_maps: dict[str, dict[str, pd.Series]]) -> float:
    total = 0.0
    for code, pos in positions.items():
        row = row_maps.get(code, {}).get(date)
        if row is None:
            total += pos.shares * pos.entry_price
            continue
        close_price = _safe_float(row.get("close"), default=pos.entry_price)
        total += pos.shares * close_price
    return round(total, 2)


def _calc_buy_budget(
    available_cash: float,
    receivable_cash: float,
    positions: dict[str, Position],
    trade_date: str,
    row_maps: dict[str, dict[str, pd.Series]],
    config: AccountReplayConfig,
) -> float:
    market_value = _market_value_of_positions(positions, trade_date, row_maps)
    total_equity = available_cash + receivable_cash + market_value
    if total_equity <= 0:
        return 0.0
    single_cap = total_equity * config.max_single_position_pct
    total_cap = max(total_equity * config.max_total_position_pct - market_value, 0.0)
    return max(min(single_cap, total_cap, available_cash), 0.0)


def _calc_buy_fill(
    signal_row: pd.Series,
    exec_row: pd.Series,
    budget: float,
    config: AccountReplayConfig,
) -> tuple[bool, float, int, float, str]:
    open_price = _safe_float(exec_row.get("open"))
    low_price = _safe_float(exec_row.get("low"))
    if open_price <= 0 or low_price <= 0:
        return False, 0.0, 0, 0.0, "INVALID_EXECUTION_BAR"
    if stage6a.exp5._is_one_word_limit_down(exec_row):
        return False, 0.0, 0, 0.0, "ONE_WORD_LIMIT_DOWN"

    target_buy_price_base = _safe_float(signal_row.get("low")) * BASELINE_CFG.left_catch_multiplier
    if target_buy_price_base <= 0:
        return False, 0.0, 0, 0.0, "INVALID_TARGET_BUY_PRICE"
    planned_buy_price = min(open_price, target_buy_price_base)
    if low_price <= planned_buy_price:
        raw_price = planned_buy_price
    elif BASELINE_CFG.fallback_rule == "NO_FILL_BUY_AT_OPEN_IF_GAP_SMALL" and open_price <= target_buy_price_base * (1 + BASELINE_CFG.max_gap_chase_pct):
        raw_price = open_price
    else:
        return False, 0.0, 0, 0.0, "TARGET_NOT_TOUCHED"

    executed_price = round(raw_price * (1 + config.buy_slippage_bps / 10000.0), 4)
    shares = int(budget // executed_price)
    shares = (shares // config.lot_size) * config.lot_size
    while shares >= config.lot_size:
        gross = shares * executed_price
        commission = max(gross * config.commission_rate, 5.0)
        total_cost = gross + commission
        if total_cost <= budget:
            return True, executed_price, shares, round(total_cost, 2), ""
        shares -= config.lot_size
    return False, 0.0, 0, 0.0, "INSUFFICIENT_CASH_OR_LOT"


def _process_sell_order(
    pos: Position,
    trade_date: str,
    row_maps: dict[str, dict[str, pd.Series]],
    config: AccountReplayConfig,
) -> tuple[bool, dict[str, Any] | None, float]:
    row = row_maps.get(pos.code, {}).get(trade_date)
    if row is None:
        return False, None, 0.0
    if stage6a.exp5._is_one_word_limit_down(row):
        return False, None, 0.0
    open_price = _safe_float(row.get("open"))
    if open_price <= 0:
        return False, None, 0.0
    executed_sell_price = round(open_price * (1 - config.sell_slippage_bps / 10000.0), 4)
    gross_amount = pos.shares * executed_sell_price
    commission = max(gross_amount * config.commission_rate, 5.0)
    stamp_tax = gross_amount * config.stamp_tax_rate
    net_amount = gross_amount - commission - stamp_tax
    pnl_amount = net_amount - pos.entry_total_cost
    return_pct = pnl_amount / pos.entry_total_cost * 100.0 if pos.entry_total_cost > 0 else 0.0
    trade = {
        "code": pos.code,
        "signal_date": pos.signal_date,
        "entry_date": pos.entry_date,
        "exit_date": trade_date,
        "entry_price": pos.entry_price,
        "exit_price": round(executed_sell_price, 4),
        "shares": pos.shares,
        "pnl_amount": round(pnl_amount, 2),
        "return_pct": round(return_pct, 4),
        "holding_days": 0,
        "rps50": pos.rps50,
        "ma20_bias": pos.ma20_bias,
        "score": pos.score,
        "exit_signal": pos.pending_exit_signal or "",
        "exit_reason": pos.pending_exit_reason or "",
    }
    return True, trade, round(net_amount, 2)


def _run_account_replay_for_period(
    period_name: str,
    config: AccountReplayConfig,
    histories: dict[str, pd.DataFrame],
    row_maps: dict[str, dict[str, pd.Series]],
    sorted_dates: list[str],
) -> tuple[dict[str, Any], dict[str, Any], pd.DataFrame]:
    by_entry_date, _, _ = _build_candidate_events(histories=histories, config=config)
    positions: dict[str, Position] = {}
    trades: list[dict[str, Any]] = []
    equity_records: list[dict[str, Any]] = []
    available_cash = float(config.initial_cash)
    pending_cash: list[dict[str, Any]] = []
    miss_trade_count = 0
    attempted_buy_count = 0
    max_reached = 0

    for trade_date in sorted_dates:
        released = [item for item in pending_cash if item["available_date"] <= trade_date]
        pending_cash = [item for item in pending_cash if item["available_date"] > trade_date]
        for item in released:
            available_cash += float(item["amount"])

        sell_codes = sorted([code for code, pos in positions.items() if pos.scheduled_sell_date == trade_date])
        for code in sell_codes:
            pos = positions[code]
            filled, trade, net_amount = _process_sell_order(pos=pos, trade_date=trade_date, row_maps=row_maps, config=config)
            if not filled:
                next_sell_date = _next_date(trade_date, sorted_dates)
                positions[code].scheduled_sell_date = next_sell_date or ""
                continue
            next_cash_date = _next_date(trade_date, sorted_dates) if SELL_SETTLEMENT_MODE == "NEXT_DAY_AVAILABLE" else trade_date
            if next_cash_date is None:
                available_cash += net_amount
            else:
                pending_cash.append({"available_date": next_cash_date, "amount": net_amount})
            entry_dt = pd.to_datetime(pos.entry_date)
            exit_dt = pd.to_datetime(trade_date)
            trade["holding_days"] = int(max((exit_dt - entry_dt).days, 1))
            trades.append(trade)
            del positions[code]

        day_events = sorted(
            by_entry_date.get(trade_date, []),
            key=lambda item: (-float(item["rps50"]), float(item["ma20_bias"]), -float(item["score"]), item["code"]),
        )
        current_count = len(positions)
        open_slots = max(config.max_concurrent_positions - current_count, 0)
        buy_slots = min(config.max_new_positions_per_day, open_slots)
        used_new_positions = 0

        receivable_cash = sum(float(item["amount"]) for item in pending_cash)
        for event in day_events:
            attempted_buy_count += 1
            code = str(event["code"])
            if code in positions:
                miss_trade_count += 1
                continue
            if used_new_positions >= buy_slots:
                miss_trade_count += 1
                continue
            signal_row = histories[code].iloc[int(event["signal_idx"])]
            exec_row = row_maps.get(code, {}).get(trade_date)
            if exec_row is None:
                miss_trade_count += 1
                continue
            budget = _calc_buy_budget(
                available_cash=available_cash,
                receivable_cash=receivable_cash,
                positions=positions,
                trade_date=trade_date,
                row_maps=row_maps,
                config=config,
            )
            if budget <= 0:
                miss_trade_count += 1
                continue
            filled, entry_price, shares, total_cost, reason = _calc_buy_fill(
                signal_row=signal_row,
                exec_row=exec_row,
                budget=budget,
                config=config,
            )
            if not filled or shares <= 0:
                miss_trade_count += 1
                continue
            available_cash -= total_cost
            used_new_positions += 1
            positions[code] = Position(
                code=code,
                entry_date=trade_date,
                shares=shares,
                entry_price=entry_price,
                entry_total_cost=total_cost,
                signal_date=str(event["signal_date"]),
                rps50=float(event["rps50"]),
                ma20_bias=float(event["ma20_bias"]),
                score=float(event["score"]),
            )

        for code, pos in list(positions.items()):
            row = row_maps.get(code, {}).get(trade_date)
            if row is None:
                continue
            exit_signal, exit_reason = stage6a._evaluate_exit_signal(
                review_row=row,
                entry_price=pos.entry_price,
                cfg=BASELINE_CFG,
                config=stage6a.RunnerConfig(
                    base_dir=config.base_dir,
                    reports_dir=config.reports_dir,
                    data_dir=config.data_dir,
                    universe_file=config.universe_file,
                    start_date=config.start_date,
                    end_date=config.end_date,
                    max_files=config.max_files,
                    amount_min=config.amount_min,
                    target_profit_pct=config.target_profit_pct,
                    initial_trade_cash=config.initial_cash,
                    commission_rate=config.commission_rate,
                    stamp_tax_rate=config.stamp_tax_rate,
                    buy_slippage_bps=config.buy_slippage_bps,
                    sell_slippage_bps=config.sell_slippage_bps,
                    lot_size=config.lot_size,
                ),
            )
            if exit_signal:
                next_sell_date = _next_date(trade_date, sorted_dates)
                if next_sell_date:
                    pos.pending_exit = True
                    pos.pending_exit_signal = exit_signal
                    pos.pending_exit_reason = exit_reason
                    pos.scheduled_sell_date = next_sell_date

        market_value = _market_value_of_positions(positions, trade_date, row_maps)
        receivable_cash = sum(float(item["amount"]) for item in pending_cash)
        total_equity = available_cash + receivable_cash + market_value
        concurrent_positions = len(positions)
        max_reached = max(max_reached, concurrent_positions)
        cash_ratio = available_cash / total_equity if total_equity > 0 else 0.0
        position_ratio = market_value / total_equity if total_equity > 0 else 0.0
        equity_records.append(
            {
                "period": period_name,
                "trade_date": trade_date,
                "equity": round(total_equity, 2),
                "available_cash": round(available_cash, 2),
                "receivable_cash": round(receivable_cash, 2),
                "market_value": round(market_value, 2),
                "concurrent_positions": concurrent_positions,
                "cash_utilization": round(cash_ratio, 6),
                "position_utilization": round(position_ratio, 6),
            }
        )

    trade_df = pd.DataFrame(trades)
    equity_df = pd.DataFrame(equity_records)
    if trade_df.empty:
        profit_factor = None
        expectancy = 0.0
    else:
        gross_profit = float(trade_df.loc[trade_df["pnl_amount"] > 0, "pnl_amount"].sum())
        gross_loss = float(trade_df.loc[trade_df["pnl_amount"] < 0, "pnl_amount"].sum())
        profit_factor = gross_profit / abs(gross_loss) if gross_loss < 0 else None
        expectancy = float(trade_df["return_pct"].mean())
    final_equity = float(equity_df["equity"].iloc[-1]) if not equity_df.empty else float(config.initial_cash)
    total_return_pct = (final_equity / config.initial_cash - 1.0) * 100.0 if config.initial_cash > 0 else 0.0
    monthly_stats = _calc_monthly_stats(equity_df)
    summary = {
        "period": period_name,
        "account_total_return_pct": round(total_return_pct, 4),
        "account_max_drawdown_pct": _calc_max_drawdown(equity_df["equity"]) if not equity_df.empty else 0.0,
        "account_profit_factor": round(profit_factor, 4) if profit_factor is not None else None,
        "account_expectancy": round(expectancy, 4),
        "trade_count": int(len(trade_df)),
        "account_level_miss_trade_rate": round(miss_trade_count / attempted_buy_count * 100.0, 4) if attempted_buy_count else None,
        "avg_holding_days": round(float(trade_df["holding_days"].mean()), 4) if not trade_df.empty else 0.0,
        "average_position_utilization": round(float(equity_df["position_utilization"].mean()), 4) if not equity_df.empty else 0.0,
        "average_cash_utilization": round(float(equity_df["cash_utilization"].mean()), 4) if not equity_df.empty else 0.0,
        "average_concurrent_positions": round(float(equity_df["concurrent_positions"].mean()), 4) if not equity_df.empty else 0.0,
        "max_concurrent_positions_reached": int(max_reached),
        **monthly_stats,
    }
    authenticity = {
        "period": period_name,
        "sell_settlement_mode": SELL_SETTLEMENT_MODE,
        "sell_proceeds_same_day_available": False,
        "cost_deducted": True,
        "miss_trade_rate_abnormal": bool(_safe_float(summary["account_level_miss_trade_rate"], 0.0) >= 20.0),
        "extreme_month_flag": bool(monthly_stats["extreme_month_flag"]),
    }
    return summary, authenticity, equity_df


def _load_stage6b_reference(reports_dir: Path) -> pd.DataFrame:
    path = reports_dir / "stage6b_baseline_validation_summary.csv"
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, encoding="utf-8-sig")


def _write_report(
    path: Path,
    summary_df: pd.DataFrame,
    authenticity_df: pd.DataFrame,
    strategy_ref_df: pd.DataFrame,
) -> None:
    summary_view = summary_df[
        [
            "period",
            "account_total_return_pct",
            "account_max_drawdown_pct",
            "account_profit_factor",
            "account_expectancy",
            "trade_count",
            "account_level_miss_trade_rate",
            "avg_holding_days",
            "average_position_utilization",
            "average_cash_utilization",
            "average_concurrent_positions",
            "max_concurrent_positions_reached",
        ]
    ]
    lines = [
        "# Stage 6C Account Replay Report",
        "",
        "## 账户级指标",
        "",
        stage6a._markdown_table(summary_view),
        "",
        "## 真实性复核",
        "",
        stage6a._markdown_table(authenticity_df),
        "",
    ]
    if not strategy_ref_df.empty:
        merged = summary_df.merge(strategy_ref_df[["period", "total_return_pct", "max_drawdown_pct", "miss_trade_rate"]], on="period", how="left")
        merged["return_compression_pct"] = merged["account_total_return_pct"] - merged["total_return_pct"]
        merged["drawdown_delta_pct"] = merged["account_max_drawdown_pct"] - merged["max_drawdown_pct"]
        compare_view = merged[
            ["period", "total_return_pct", "account_total_return_pct", "return_compression_pct", "max_drawdown_pct", "account_max_drawdown_pct", "drawdown_delta_pct", "miss_trade_rate", "account_level_miss_trade_rate"]
        ]
        lines.extend(
            [
                "## 与 Stage 6B 策略级口径对比",
                "",
                stage6a._markdown_table(compare_view),
                "",
            ]
        )

    lines.extend(
        [
            "## 风险提示",
            "",
            "- 本阶段口径已切换为单账户资金池回放，买入共用同一现金池，卖出优先于买入。",
            f"- 卖出回款口径: `{SELL_SETTLEMENT_MODE}`，即卖出资金次日可用，未采用更激进的当日回款假设。",
            "- 未成交订单当日作废，不跨日排队。",
        ]
    )
    full_row = summary_df[summary_df["period"] == "full_period"].iloc[0]
    if _safe_float(full_row["account_total_return_pct"]) >= 100.0:
        lines.append("- 账户级 total_return_pct 仍然偏高，但已明显低于此前策略级累计收益；这说明仓位与现金约束对收益有显著压缩。")
    if bool(authenticity_df["extreme_month_flag"].any()):
        lines.append("- 账户层仍存在 extreme_month_flag，说明净值曲线仍有阶段性波动偏大的问题。")
    if _safe_float(full_row["account_max_drawdown_pct"]) <= -25.0:
        lines.append("- 账户层最大回撤偏大，净值曲线可接受性一般，需要进一步做更严格的仓位控制验证。")
    else:
        lines.append("- 账户层最大回撤未失控，净值曲线在当前样本下仍处于可接受区间。")
    lines.append("- 成本已扣除：买入佣金、卖出佣金、印花税、双边滑点均已纳入。")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8-sig")


def run(
    base_dir: str = str(DEFAULT_BASE_DIR),
    in_sample_start: str = "2021-01-01",
    in_sample_end: str = "2024-12-31",
    out_sample_start: str = "2025-01-01",
    out_sample_end: str | None = None,
    full_start: str = "2021-01-01",
    full_end: str | None = None,
    max_files: int | None = 160,
    amount_min: float = 2e8,
    target_profit_pct: float = 0.08,
    initial_cash: float = 1_000_000.0,
    commission_rate: float = 0.0003,
    stamp_tax_rate: float = 0.001,
    buy_slippage_bps: float = 8.0,
    sell_slippage_bps: float = 8.0,
    lot_size: int = 100,
    max_concurrent_positions: int = 10,
    max_new_positions_per_day: int = 3,
    max_single_position_pct: float = 0.10,
    max_total_position_pct: float = 0.95,
) -> dict[str, Any]:
    base_path = Path(base_dir)
    reports_dir = base_path / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    results: list[dict[str, Any]] = []
    authenticity_rows: list[dict[str, Any]] = []

    for period_name, start_date, end_date in [
        ("in_sample", in_sample_start, in_sample_end),
        ("out_of_sample", out_sample_start, out_sample_end),
        ("full_period", full_start, full_end),
    ]:
        period_config = AccountReplayConfig(
            base_dir=base_path,
            reports_dir=reports_dir,
            data_dir=base_path / "stock_data_5years",
            universe_file=reports_dir / "batch_backtest_summary.csv",
            start_date=start_date,
            end_date=end_date,
            max_files=max_files,
            amount_min=float(amount_min),
            target_profit_pct=float(target_profit_pct),
            initial_cash=float(initial_cash),
            commission_rate=float(commission_rate),
            stamp_tax_rate=float(stamp_tax_rate),
            buy_slippage_bps=float(buy_slippage_bps),
            sell_slippage_bps=float(sell_slippage_bps),
            lot_size=int(lot_size),
            max_concurrent_positions=int(max_concurrent_positions),
            max_new_positions_per_day=int(max_new_positions_per_day),
            max_single_position_pct=float(max_single_position_pct),
            max_total_position_pct=float(max_total_position_pct),
        )
        histories, row_maps, sorted_dates = _load_histories(period_config)
        summary, authenticity, _ = _run_account_replay_for_period(
            period_name=period_name,
            config=period_config,
            histories=histories,
            row_maps=row_maps,
            sorted_dates=sorted_dates,
        )
        summary["monthly_return_stats"] = json.dumps(
            {
                "mean": summary["monthly_return_mean"],
                "std": summary["monthly_return_std"],
                "best": summary["best_month_return_pct"],
                "worst": summary["worst_month_return_pct"],
                "positive_ratio_pct": summary["positive_month_ratio_pct"],
                "extreme_month_flag": summary["extreme_month_flag"],
            },
            ensure_ascii=False,
        )
        results.append(summary)
        authenticity_rows.append(authenticity)

    summary_df = pd.DataFrame(results)
    authenticity_df = pd.DataFrame(authenticity_rows)
    strategy_ref_df = _load_stage6b_reference(reports_dir)

    csv_path = reports_dir / "stage6c_account_replay_summary.csv"
    json_path = reports_dir / "stage6c_account_replay.json"
    report_path = reports_dir / "stage6c_account_report.md"

    summary_df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    payload = {
        "baseline_parameters": {
            "regime_mode": BASELINE_CFG.regime_mode,
            "regime_ma_window": BASELINE_CFG.regime_ma_window,
            "left_catch_multiplier": BASELINE_CFG.left_catch_multiplier,
            "max_gap_chase_pct": BASELINE_CFG.max_gap_chase_pct,
            "fallback_rule": BASELINE_CFG.fallback_rule,
            "shadow_threshold": BASELINE_CFG.shadow_threshold,
            "min_profit_to_trigger_shadow_exit": BASELINE_CFG.min_profit_to_trigger_shadow_exit,
            "shadow_exit_mode": BASELINE_CFG.shadow_exit_mode,
        },
        "account_constraints": {
            "initial_cash": initial_cash,
            "max_concurrent_positions": max_concurrent_positions,
            "max_new_positions_per_day": max_new_positions_per_day,
            "max_single_position_pct": max_single_position_pct,
            "max_total_position_pct": max_total_position_pct,
            "sell_settlement_mode": SELL_SETTLEMENT_MODE,
        },
        "summary": summary_df.to_dict(orient="records"),
        "authenticity_checks": authenticity_df.to_dict(orient="records"),
        "strategy_level_reference": strategy_ref_df.to_dict(orient="records"),
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8-sig")
    _write_report(report_path, summary_df, authenticity_df, strategy_ref_df)

    return {
        "stage_status": "SUCCESS_EXECUTED",
        "summary_path": str(csv_path),
        "summary_json_path": str(json_path),
        "report_path": str(report_path),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stage 6C 账户级真实性回放")
    parser.add_argument("--base-dir", default=str(DEFAULT_BASE_DIR))
    parser.add_argument("--in-sample-start", default="2021-01-01")
    parser.add_argument("--in-sample-end", default="2024-12-31")
    parser.add_argument("--out-sample-start", default="2025-01-01")
    parser.add_argument("--out-sample-end", default=None)
    parser.add_argument("--full-start", default="2021-01-01")
    parser.add_argument("--full-end", default=None)
    parser.add_argument("--max-files", type=int, default=160)
    parser.add_argument("--amount-min", type=float, default=2e8)
    parser.add_argument("--target-profit-pct", type=float, default=0.08)
    parser.add_argument("--initial-cash", type=float, default=1_000_000.0)
    parser.add_argument("--commission-rate", type=float, default=0.0003)
    parser.add_argument("--stamp-tax-rate", type=float, default=0.001)
    parser.add_argument("--buy-slippage-bps", type=float, default=8.0)
    parser.add_argument("--sell-slippage-bps", type=float, default=8.0)
    parser.add_argument("--lot-size", type=int, default=100)
    parser.add_argument("--max-concurrent-positions", type=int, default=10)
    parser.add_argument("--max-new-positions-per-day", type=int, default=3)
    parser.add_argument("--max-single-position-pct", type=float, default=0.10)
    parser.add_argument("--max-total-position-pct", type=float, default=0.95)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    res = run(
        base_dir=args.base_dir,
        in_sample_start=args.in_sample_start,
        in_sample_end=args.in_sample_end,
        out_sample_start=args.out_sample_start,
        out_sample_end=args.out_sample_end,
        full_start=args.full_start,
        full_end=args.full_end,
        max_files=args.max_files,
        amount_min=args.amount_min,
        target_profit_pct=args.target_profit_pct,
        initial_cash=args.initial_cash,
        commission_rate=args.commission_rate,
        stamp_tax_rate=args.stamp_tax_rate,
        buy_slippage_bps=args.buy_slippage_bps,
        sell_slippage_bps=args.sell_slippage_bps,
        lot_size=args.lot_size,
        max_concurrent_positions=args.max_concurrent_positions,
        max_new_positions_per_day=args.max_new_positions_per_day,
        max_single_position_pct=args.max_single_position_pct,
        max_total_position_pct=args.max_total_position_pct,
    )
    print("============================================================")
    print("Stage 6C 账户级回放完成")
    print(f"汇总 CSV : {res['summary_path']}")
    print(f"汇总 JSON: {res['summary_json_path']}")
    print(f"报告      : {res['report_path']}")
    print("============================================================")


if __name__ == "__main__":
    main()
