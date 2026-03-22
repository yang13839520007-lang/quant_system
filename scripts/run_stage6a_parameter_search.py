# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import math
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import pandas as pd


SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR = SCRIPT_DIR.parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import run_experiment_matrix as exp5  # noqa: E402


DEFAULT_BASE_DIR = ROOT_DIR
REGIME_MODES = ["HARD_BLOCK", "HALF_POSITION", "REDUCE_MAX_POSITIONS"]
REGIME_MA_WINDOWS = [15, 20, 25, 30]
REGIME_RISK_SCALES = [1.0, 0.7, 0.5]
LEFT_CATCH_MULTIPLIERS = [1.01, 1.015, 1.02, 1.03]
MAX_GAP_CHASE_PCTS = [0.0, 0.01, 0.02]
FALLBACK_RULES = ["NO_FILL_DROP", "NO_FILL_BUY_AT_OPEN_IF_GAP_SMALL"]
SHADOW_THRESHOLDS = [0.60, 0.65, 0.70, 0.75]
MIN_PROFIT_TO_TRIGGER = [0.0, 0.02, 0.03, 0.05]
SHADOW_EXIT_MODES = ["NEXT_OPEN_FULL_EXIT", "NEXT_OPEN_HALF_EXIT"]


@dataclass(frozen=True)
class SearchConfig:
    regime_mode: str = "HALF_POSITION"
    regime_ma_window: int = 20
    regime_risk_scale: float = 0.7
    left_catch_multiplier: float = 1.015
    max_gap_chase_pct: float = 0.01
    fallback_rule: str = "NO_FILL_BUY_AT_OPEN_IF_GAP_SMALL"
    shadow_threshold: float = 0.70
    min_profit_to_trigger_shadow_exit: float = 0.02
    shadow_exit_mode: str = "NEXT_OPEN_FULL_EXIT"
    use_volume_sanity_check: bool = False
    volume_sanity_ratio: float = 1.20


@dataclass(frozen=True)
class RunnerConfig:
    base_dir: Path
    reports_dir: Path
    data_dir: Path
    universe_file: Path
    start_date: str | None
    end_date: str | None
    max_files: int | None
    amount_min: float
    target_profit_pct: float
    initial_trade_cash: float
    commission_rate: float
    stamp_tax_rate: float
    buy_slippage_bps: float
    sell_slippage_bps: float
    lot_size: int


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
        if pd.isna(out):
            return default
        return out
    except (TypeError, ValueError):
        return default


def _calc_max_drawdown(equity_curve: list[float]) -> float:
    if not equity_curve:
        return 0.0
    running_max = equity_curve[0]
    worst = 0.0
    for equity in equity_curve:
        running_max = max(running_max, equity)
        if running_max <= 0:
            continue
        drawdown = (equity / running_max - 1.0) * 100.0
        worst = min(worst, drawdown)
    return round(worst, 4)


def _load_histories(config: RunnerConfig) -> tuple[dict[str, pd.DataFrame], list[Path]]:
    base_cfg = exp5.MatrixRunConfig(
        base_dir=config.base_dir,
        reports_dir=config.reports_dir,
        data_dir=config.data_dir,
        universe_file=config.universe_file,
        start_date=config.start_date,
        end_date=config.end_date,
        max_files=config.max_files,
        amount_min=config.amount_min,
        vol_shrink_ratio=0.8,
        target_profit_pct=config.target_profit_pct,
        shadow_threshold=0.60,
        initial_trade_cash=config.initial_trade_cash,
        commission_rate=config.commission_rate,
        stamp_tax_rate=config.stamp_tax_rate,
        buy_slippage_bps=config.buy_slippage_bps,
        sell_slippage_bps=config.sell_slippage_bps,
        lot_size=config.lot_size,
        regime_index_file=config.data_dir / "sh.000001.csv",
        regime_ma_window=20,
    )
    files = exp5._load_universe_files(base_cfg)
    histories: dict[str, pd.DataFrame] = {}
    for path in files:
        hist = exp5._prepare_history(path)
        if hist.empty:
            continue
        code = str(hist.iloc[0]["code"])
        histories[code] = hist
    return histories, files


def _build_rps_map(histories: dict[str, pd.DataFrame], start_date: str | None, end_date: str | None) -> dict[str, list[float]]:
    date_returns: dict[str, list[float]] = {}
    for hist in histories.values():
        valid = hist[hist["close_return_50"].notna()].copy()
        if start_date is not None:
            valid = valid[valid["trade_date"] >= start_date]
        if end_date is not None:
            valid = valid[valid["trade_date"] <= end_date]
        for trade_date, ret_val in zip(valid["trade_date"], valid["close_return_50"]):
            date_returns.setdefault(str(trade_date), []).append(float(ret_val))
    return {trade_date: sorted(values) for trade_date, values in date_returns.items() if values}


def _build_regime_maps(config: RunnerConfig) -> dict[int, dict[str, bool]]:
    index_path = config.data_dir / "sh.000001.csv"
    index_df = exp5._prepare_history(index_path)
    if index_df.empty:
        return {}
    out: dict[int, dict[str, bool]] = {}
    for window in REGIME_MA_WINDOWS:
        work = index_df.copy()
        work["regime_ma"] = work["close"].rolling(window, min_periods=window).mean()
        work["regime_safe"] = (work["close"] > work["regime_ma"]).fillna(False)
        out[window] = dict(zip(work["trade_date"], work["regime_safe"].astype(bool)))
    return out


def _passes_candidate_base(row: pd.Series, config: RunnerConfig, search_cfg: SearchConfig, rps50: float | None) -> bool:
    if rps50 is None:
        return False
    close_price = _safe_float(row.get("close"))
    ma5 = _safe_float(row.get("ma5"))
    ma20 = _safe_float(row.get("ma20"))
    amount = _safe_float(row.get("amount"))
    volume = _safe_float(row.get("volume"))
    vol_ma5 = _safe_float(row.get("vol_ma5"))
    if min(close_price, ma5, ma20, volume, vol_ma5) <= 0:
        return False
    if amount < config.amount_min:
        return False
    if close_price <= ma20 or close_price >= ma5:
        return False
    if search_cfg.use_volume_sanity_check and volume > vol_ma5 * search_cfg.volume_sanity_ratio:
        return False
    return True


def _build_candidate_events(
    histories: dict[str, pd.DataFrame],
    rps_map: dict[str, list[float]],
    config: RunnerConfig,
    search_cfg: SearchConfig,
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, list[dict[str, Any]]]]:
    events_by_stock: dict[str, list[dict[str, Any]]] = {}
    events_by_date: dict[str, list[dict[str, Any]]] = {}
    for code, hist in histories.items():
        stock_events: list[dict[str, Any]] = []
        for idx in range(50, len(hist) - 1):
            row = hist.iloc[idx]
            signal_date = str(row["trade_date"])
            if config.start_date is not None and signal_date < config.start_date:
                continue
            if config.end_date is not None and signal_date > config.end_date:
                break
            rps50 = exp5._calc_rps50(signal_date, _safe_float(row.get("close_return_50"), default=float("nan")), rps_map)
            if not _passes_candidate_base(row=row, config=config, search_cfg=search_cfg, rps50=rps50):
                continue
            event = {
                "code": code,
                "signal_date": signal_date,
                "signal_idx": idx,
                "rps50": float(rps50),
                "signal_low": _safe_float(row.get("low")),
            }
            stock_events.append(event)
            events_by_date.setdefault(signal_date, []).append(event)
        events_by_stock[code] = stock_events
    return events_by_stock, events_by_date


def _resolve_regime_state(signal_date: str, cfg: SearchConfig, regime_maps: dict[int, dict[str, bool]]) -> bool:
    return regime_maps.get(cfg.regime_ma_window, {}).get(signal_date, True)


def _build_daily_regime_allow_map(
    events_by_date: dict[str, list[dict[str, Any]]],
    regime_maps: dict[int, dict[str, bool]],
    cfg: SearchConfig,
) -> dict[tuple[str, str, int], float]:
    allow_map: dict[tuple[str, str, int], float] = {}
    for signal_date, events in events_by_date.items():
        is_safe = _resolve_regime_state(signal_date, cfg=cfg, regime_maps=regime_maps)
        if is_safe:
            for event in events:
                allow_map[(event["code"], signal_date, int(event["signal_idx"]))] = 1.0
            continue

        if cfg.regime_mode == "HARD_BLOCK":
            continue

        if cfg.regime_mode == "HALF_POSITION":
            scale = max(min(cfg.regime_risk_scale, 1.0), 0.0)
            for event in events:
                allow_map[(event["code"], signal_date, int(event["signal_idx"]))] = scale
            continue

        keep_fraction = max(min(cfg.regime_risk_scale, 1.0), 0.0)
        if keep_fraction <= 0:
            continue
        ranked = sorted(events, key=lambda item: (-float(item["rps50"]), item["code"]))
        keep_count = max(1, int(math.ceil(len(ranked) * keep_fraction)))
        for event in ranked[:keep_count]:
            allow_map[(event["code"], signal_date, int(event["signal_idx"]))] = 1.0
    return allow_map


def _calc_buy_shares(entry_price: float, position_scale: float, config: RunnerConfig) -> tuple[int, float]:
    budget = config.initial_trade_cash * max(min(position_scale, 1.0), 0.0)
    if entry_price <= 0 or budget <= 0:
        return 0, 0.0
    shares = int(budget // entry_price)
    shares = (shares // config.lot_size) * config.lot_size
    while shares >= config.lot_size:
        gross = shares * entry_price
        commission = max(gross * config.commission_rate, 5.0)
        total_cost = gross + commission
        if total_cost <= budget:
            return shares, total_cost
        shares -= config.lot_size
    return 0, 0.0


def _try_left_catch_entry(signal_row: pd.Series, exec_row: pd.Series, cfg: SearchConfig, config: RunnerConfig, position_scale: float) -> dict[str, Any]:
    open_price = _safe_float(exec_row.get("open"))
    low_price = _safe_float(exec_row.get("low"))
    if open_price <= 0 or low_price <= 0:
        return {"filled": False, "reason": "INVALID_EXECUTION_BAR"}
    if exp5._is_one_word_limit_down(exec_row):
        return {"filled": False, "reason": "ONE_WORD_LIMIT_DOWN"}

    target_buy_price_base = _safe_float(signal_row.get("low")) * float(cfg.left_catch_multiplier)
    if target_buy_price_base <= 0:
        return {"filled": False, "reason": "INVALID_TARGET_BUY_PRICE"}
    planned_buy_price = min(open_price, target_buy_price_base)

    if low_price <= planned_buy_price:
        raw_entry = planned_buy_price
    elif cfg.fallback_rule == "NO_FILL_BUY_AT_OPEN_IF_GAP_SMALL" and open_price <= target_buy_price_base * (1 + float(cfg.max_gap_chase_pct)):
        raw_entry = open_price
    else:
        return {"filled": False, "reason": "TARGET_NOT_TOUCHED"}

    executed_price = round(raw_entry * (1 + config.buy_slippage_bps / 10000.0), 4)
    shares, total_cost = _calc_buy_shares(entry_price=executed_price, position_scale=position_scale, config=config)
    if shares < config.lot_size:
        return {"filled": False, "reason": "LESS_THAN_ONE_LOT"}
    return {
        "filled": True,
        "entry_price": executed_price,
        "shares": shares,
        "entry_total_cost": round(total_cost, 2),
    }


def _evaluate_exit_signal(review_row: pd.Series, entry_price: float, cfg: SearchConfig, config: RunnerConfig) -> tuple[str, str]:
    close_price = _safe_float(review_row.get("close"))
    ma20 = _safe_float(review_row.get("ma20"))
    upper_shadow = _safe_float(review_row.get("upper_shadow"))
    if close_price <= 0:
        return "", ""
    pnl_ratio = close_price / entry_price - 1.0
    if pnl_ratio >= config.target_profit_pct:
        return "TARGET_PROFIT_REACHED", f"收盘浮盈 {pnl_ratio:.2%} 达到固定止盈阈值 {config.target_profit_pct:.2%}"
    if pnl_ratio >= float(cfg.min_profit_to_trigger_shadow_exit) and upper_shadow >= float(cfg.shadow_threshold):
        return "UPPER_SHADOW_WARNING", f"长上影比例 {upper_shadow:.2f} 超过阈值 {cfg.shadow_threshold:.2f} 且浮盈达到 {cfg.min_profit_to_trigger_shadow_exit:.2%}"
    if ma20 > 0 and close_price < ma20:
        return "CLOSE_BELOW_MA20", f"收盘价 {close_price:.2f} 跌破 MA20 {ma20:.2f}"
    return "", ""


def _simulate_stock(
    hist: pd.DataFrame,
    stock_events: list[dict[str, Any]],
    allow_map: dict[tuple[str, str, int], float],
    cfg: SearchConfig,
    config: RunnerConfig,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    trades: list[dict[str, Any]] = []
    counters = {"signal_count": len(stock_events), "attempted_entry_count": 0, "missed_entry_count": 0}
    next_available_idx = 0
    code = str(hist.iloc[0]["code"]) if not hist.empty else ""

    for event in stock_events:
        signal_idx = int(event["signal_idx"])
        signal_date = str(event["signal_date"])
        if signal_idx < next_available_idx:
            continue
        position_scale = allow_map.get((code, signal_date, signal_idx), 0.0)
        if position_scale <= 0:
            continue

        counters["attempted_entry_count"] += 1
        entry_idx = signal_idx + 1
        if entry_idx >= len(hist):
            break

        signal_row = hist.iloc[signal_idx]
        entry_row = hist.iloc[entry_idx]
        entry_res = _try_left_catch_entry(signal_row=signal_row, exec_row=entry_row, cfg=cfg, config=config, position_scale=position_scale)
        if not entry_res.get("filled", False):
            counters["missed_entry_count"] += 1
            continue

        entry_price = float(entry_res["entry_price"])
        remaining_shares = int(entry_res["shares"])
        remaining_cost = float(entry_res["entry_total_cost"])
        review_idx = entry_idx

        while review_idx < len(hist) - 1 and remaining_shares > 0:
            review_row = hist.iloc[review_idx]
            exit_signal, exit_reason = _evaluate_exit_signal(review_row=review_row, entry_price=entry_price, cfg=cfg, config=config)
            if not exit_signal:
                review_idx += 1
                continue

            exit_idx = review_idx + 1
            exit_row = hist.iloc[exit_idx]
            if exp5._is_one_word_limit_down(exit_row):
                review_idx += 1
                continue
            exit_open = _safe_float(exit_row.get("open"))
            if exit_open <= 0:
                review_idx += 1
                continue

            if exit_signal == "UPPER_SHADOW_WARNING" and cfg.shadow_exit_mode == "NEXT_OPEN_HALF_EXIT" and remaining_shares >= config.lot_size * 2:
                exit_shares = max((remaining_shares // 2) // config.lot_size * config.lot_size, config.lot_size)
            else:
                exit_shares = remaining_shares

            executed_sell_price = round(exit_open * (1 - config.sell_slippage_bps / 10000.0), 4)
            gross_sell_amount = exit_shares * executed_sell_price
            sell_commission = max(gross_sell_amount * config.commission_rate, 5.0)
            stamp_tax = gross_sell_amount * config.stamp_tax_rate
            net_sell_amount = gross_sell_amount - sell_commission - stamp_tax
            cost_alloc = remaining_cost * (exit_shares / remaining_shares)
            pnl_amount = net_sell_amount - cost_alloc
            return_pct = pnl_amount / cost_alloc * 100.0 if cost_alloc > 0 else 0.0

            trades.append(
                {
                    "code": code,
                    "signal_date": signal_date,
                    "entry_date": str(entry_row["trade_date"]),
                    "exit_date": str(exit_row["trade_date"]),
                    "exit_signal": exit_signal,
                    "exit_reason": exit_reason,
                    "entry_price": round(entry_price, 4),
                    "exit_price": round(executed_sell_price, 4),
                    "shares": int(exit_shares),
                    "holding_days": int(exit_idx - entry_idx + 1),
                    "pnl_amount": round(pnl_amount, 2),
                    "return_pct": round(return_pct, 4),
                    "position_scale": round(position_scale, 4),
                    "shadow_exit_mode": cfg.shadow_exit_mode,
                }
            )

            remaining_shares -= exit_shares
            remaining_cost -= cost_alloc
            next_available_idx = exit_idx if remaining_shares <= 0 else exit_idx + 1
            review_idx = exit_idx + 1

        if remaining_shares > 0:
            last_row = hist.iloc[-1]
            exit_price = _safe_float(last_row.get("close"))
            if exit_price > 0:
                gross_sell_amount = remaining_shares * exit_price
                sell_commission = max(gross_sell_amount * config.commission_rate, 5.0)
                stamp_tax = gross_sell_amount * config.stamp_tax_rate
                net_sell_amount = gross_sell_amount - sell_commission - stamp_tax
                pnl_amount = net_sell_amount - remaining_cost
                return_pct = pnl_amount / remaining_cost * 100.0 if remaining_cost > 0 else 0.0
                trades.append(
                    {
                        "code": code,
                        "signal_date": signal_date,
                        "entry_date": str(entry_row["trade_date"]),
                        "exit_date": str(last_row["trade_date"]),
                        "exit_signal": "END_OF_SAMPLE",
                        "exit_reason": "样本结束按最后收盘价结算",
                        "entry_price": round(entry_price, 4),
                        "exit_price": round(exit_price, 4),
                        "shares": int(remaining_shares),
                        "holding_days": int(len(hist) - entry_idx),
                        "pnl_amount": round(pnl_amount, 2),
                        "return_pct": round(return_pct, 4),
                        "position_scale": round(position_scale, 4),
                        "shadow_exit_mode": cfg.shadow_exit_mode,
                    }
                )
            next_available_idx = len(hist)

    return trades, counters


def _summarize_combo(search_stage: str, tag: str, cfg: SearchConfig, trades: list[dict[str, Any]], counters: dict[str, int]) -> dict[str, Any]:
    trade_df = pd.DataFrame(trades)
    if trade_df.empty:
        return {
            "search_stage": search_stage,
            "tag": tag,
            **asdict(cfg),
            "total_return_pct": 0.0,
            "win_rate": 0.0,
            "profit_factor": None,
            "expectancy": 0.0,
            "max_drawdown_pct": 0.0,
            "avg_holding_days": 0.0,
            "trade_count": 0,
            "signal_count": counters["signal_count"],
            "attempted_entry_count": counters["attempted_entry_count"],
            "missed_entry_count": counters["missed_entry_count"],
            "miss_trade_rate": round(counters["missed_entry_count"] / counters["attempted_entry_count"] * 100.0, 4) if counters["attempted_entry_count"] else None,
        }

    trade_df["return_ratio"] = pd.to_numeric(trade_df["return_pct"], errors="coerce").fillna(0.0) / 100.0
    gross_profit = float(trade_df.loc[trade_df["return_ratio"] > 0, "return_ratio"].sum())
    gross_loss = float(trade_df.loc[trade_df["return_ratio"] < 0, "return_ratio"].sum())
    equity = 1.0
    equity_curve = [equity]
    for ret in trade_df["return_ratio"]:
        equity += float(ret)
        equity_curve.append(equity)
    profit_factor = None
    if gross_loss < 0:
        profit_factor = gross_profit / abs(gross_loss)

    miss_trade_rate = None
    if counters["attempted_entry_count"]:
        miss_trade_rate = counters["missed_entry_count"] / counters["attempted_entry_count"] * 100.0

    return {
        "search_stage": search_stage,
        "tag": tag,
        **asdict(cfg),
        "total_return_pct": round((equity - 1.0) * 100.0, 4),
        "win_rate": round(float((trade_df["return_pct"] > 0).mean() * 100.0), 4),
        "profit_factor": round(profit_factor, 4) if profit_factor is not None else None,
        "expectancy": round(float(trade_df["return_pct"].mean()), 4),
        "max_drawdown_pct": _calc_max_drawdown(equity_curve),
        "avg_holding_days": round(float(trade_df["holding_days"].mean()), 4),
        "trade_count": int(len(trade_df)),
        "signal_count": counters["signal_count"],
        "attempted_entry_count": counters["attempted_entry_count"],
        "missed_entry_count": counters["missed_entry_count"],
        "miss_trade_rate": round(miss_trade_rate, 4) if miss_trade_rate is not None else None,
    }


def _rank_normalized(series: pd.Series, ascending: bool) -> pd.Series:
    ranked = series.rank(pct=True, ascending=ascending, method="average")
    return ranked.fillna(0.0)


def _attach_scores(summary_df: pd.DataFrame) -> pd.DataFrame:
    work = summary_df.copy()
    work["return_rank_score"] = _rank_normalized(pd.to_numeric(work["total_return_pct"], errors="coerce"), ascending=True)
    work["drawdown_rank_score"] = _rank_normalized(pd.to_numeric(work["max_drawdown_pct"], errors="coerce"), ascending=True)
    work["expectancy_rank_score"] = _rank_normalized(pd.to_numeric(work["expectancy"], errors="coerce"), ascending=True)
    work["pf_rank_score"] = _rank_normalized(pd.to_numeric(work["profit_factor"], errors="coerce"), ascending=True)
    miss_series = pd.to_numeric(work["miss_trade_rate"], errors="coerce").fillna(9999.0)
    work["miss_rank_score"] = _rank_normalized(-miss_series, ascending=True)
    work["balanced_score"] = (
        work["return_rank_score"] * 0.35
        + work["drawdown_rank_score"] * 0.25
        + work["expectancy_rank_score"] * 0.20
        + work["pf_rank_score"] * 0.15
        + work["miss_rank_score"] * 0.05
    ).round(6)
    return work


def _select_best(df: pd.DataFrame, stage_name: str) -> pd.Series:
    stage_df = df[df["search_stage"] == stage_name].copy()
    if stage_df.empty:
        return pd.Series(dtype=object)
    stage_df = stage_df.sort_values(["balanced_score", "total_return_pct", "max_drawdown_pct"], ascending=[False, False, False])
    return stage_df.iloc[0]


def _select_best_low_miss(df: pd.DataFrame) -> pd.Series:
    work = df[df["trade_count"] > 0].copy()
    if work.empty:
        return pd.Series(dtype=object)
    work["miss_trade_rate_num"] = pd.to_numeric(work["miss_trade_rate"], errors="coerce").fillna(9999.0)
    work = work.sort_values(["miss_trade_rate_num", "balanced_score"], ascending=[True, False])
    return work.iloc[0]


def _markdown_table(df: pd.DataFrame) -> str:
    if df.empty:
        return "_无数据_"
    safe_df = df.copy().where(pd.notna(df), "N/A")
    columns = list(safe_df.columns)
    lines = ["|" + "|".join(columns) + "|", "|" + "|".join(["---"] * len(columns)) + "|"]
    for _, row in safe_df.iterrows():
        rendered = [str(row[col]).replace("|", "\\|") for col in columns]
        lines.append("|" + "|".join(rendered) + "|")
    return "\n".join(lines)


def _evaluate_combo(
    search_stage: str,
    tag: str,
    cfg: SearchConfig,
    histories: dict[str, pd.DataFrame],
    regime_maps: dict[int, dict[str, bool]],
    events_by_stock: dict[str, list[dict[str, Any]]],
    events_by_date: dict[str, list[dict[str, Any]]],
    config: RunnerConfig,
) -> dict[str, Any]:
    allow_map = _build_daily_regime_allow_map(events_by_date=events_by_date, regime_maps=regime_maps, cfg=cfg)
    all_trades: list[dict[str, Any]] = []
    counters = {"signal_count": 0, "attempted_entry_count": 0, "missed_entry_count": 0}
    for code, hist in histories.items():
        stock_events = events_by_stock.get(code, [])
        stock_trades, stock_counters = _simulate_stock(
            hist=hist,
            stock_events=stock_events,
            allow_map=allow_map,
            cfg=cfg,
            config=config,
        )
        all_trades.extend(stock_trades)
        for key in counters:
            counters[key] += stock_counters.get(key, 0)
    return _summarize_combo(search_stage=search_stage, tag=tag, cfg=cfg, trades=all_trades, counters=counters)


def _write_recommendation_md(
    path: Path,
    summary_df: pd.DataFrame,
    best_regime: pd.Series,
    best_left: pd.Series,
    best_shadow: pd.Series,
    best_balanced: pd.Series,
    best_low_miss: pd.Series,
    final_baseline: pd.Series,
) -> None:
    lines = [
        "# Stage 6A Recommendation",
        "",
        "## 最均衡参数组",
        "",
        _markdown_table(pd.DataFrame([best_balanced])),
        "",
        "## 最低 miss_trade_rate 参数组",
        "",
        _markdown_table(pd.DataFrame([best_low_miss])),
        "",
        "## 分阶段最优",
        "",
        "### Regime_Safe",
        "",
        _markdown_table(pd.DataFrame([best_regime])),
        "",
        "### LEFT_CATCH",
        "",
        _markdown_table(pd.DataFrame([best_left])),
        "",
        "### Upper Shadow exit",
        "",
        _markdown_table(pd.DataFrame([best_shadow])),
        "",
        "## 下一轮正式 baseline 建议",
        "",
        _markdown_table(pd.DataFrame([final_baseline])),
        "",
        "## 说明",
        "",
        "- Volume Shrink 本轮默认关闭，仅保留弱化 sanity check 开关，未作为主搜索轴。",
        "- 搜索采用分阶段小范围优化：先 Regime，再 LEFT_CATCH，再 Upper Shadow。",
        "- balanced_score 综合 total_return_pct、max_drawdown_pct、expectancy、profit_factor 与 miss_trade_rate 排名得出。",
        "",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8-sig")


def run(
    base_dir: str = str(DEFAULT_BASE_DIR),
    start_date: str | None = "2024-01-01",
    end_date: str | None = None,
    max_files: int | None = 80,
    amount_min: float = 2e8,
    target_profit_pct: float = 0.08,
    initial_trade_cash: float = 100000.0,
    commission_rate: float = 0.0003,
    stamp_tax_rate: float = 0.001,
    buy_slippage_bps: float = 8.0,
    sell_slippage_bps: float = 8.0,
    lot_size: int = 100,
) -> dict[str, Any]:
    base_path = Path(base_dir)
    reports_dir = base_path / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    config = RunnerConfig(
        base_dir=base_path,
        reports_dir=reports_dir,
        data_dir=base_path / "stock_data_5years",
        universe_file=reports_dir / "batch_backtest_summary.csv",
        start_date=start_date,
        end_date=end_date,
        max_files=max_files,
        amount_min=float(amount_min),
        target_profit_pct=float(target_profit_pct),
        initial_trade_cash=float(initial_trade_cash),
        commission_rate=float(commission_rate),
        stamp_tax_rate=float(stamp_tax_rate),
        buy_slippage_bps=float(buy_slippage_bps),
        sell_slippage_bps=float(sell_slippage_bps),
        lot_size=int(lot_size),
    )

    histories, loaded_files = _load_histories(config)
    if not histories:
        raise FileNotFoundError("未加载到有效历史行情文件，无法执行 Stage 6A 搜索")
    rps_map = _build_rps_map(histories=histories, start_date=config.start_date, end_date=config.end_date)
    regime_maps = _build_regime_maps(config)

    summary_rows: list[dict[str, Any]] = []
    base_search_cfg = SearchConfig()
    events_by_stock, events_by_date = _build_candidate_events(histories=histories, rps_map=rps_map, config=config, search_cfg=base_search_cfg)

    print("============================================================")
    print("Stage 6A 参数搜索开始")
    print(f"样本文件数: {len(loaded_files)}")
    print(f"起始日期  : {config.start_date or '不限'}")
    print(f"结束日期  : {config.end_date or '不限'}")
    print("============================================================")

    reference_cfg = SearchConfig(regime_mode="HARD_BLOCK", regime_ma_window=20, regime_risk_scale=1.0, left_catch_multiplier=1.01, max_gap_chase_pct=0.0, fallback_rule="NO_FILL_DROP", shadow_threshold=0.60, min_profit_to_trigger_shadow_exit=0.0, shadow_exit_mode="NEXT_OPEN_FULL_EXIT")
    summary_rows.append(_evaluate_combo("REFERENCE", "STAGE5_UPGRADED_REFERENCE", reference_cfg, histories, regime_maps, events_by_stock, events_by_date, config))

    for mode in REGIME_MODES:
        for window in REGIME_MA_WINDOWS:
            for scale in REGIME_RISK_SCALES:
                cfg = SearchConfig(
                    regime_mode=mode,
                    regime_ma_window=window,
                    regime_risk_scale=scale,
                    left_catch_multiplier=base_search_cfg.left_catch_multiplier,
                    max_gap_chase_pct=base_search_cfg.max_gap_chase_pct,
                    fallback_rule=base_search_cfg.fallback_rule,
                    shadow_threshold=base_search_cfg.shadow_threshold,
                    min_profit_to_trigger_shadow_exit=base_search_cfg.min_profit_to_trigger_shadow_exit,
                    shadow_exit_mode=base_search_cfg.shadow_exit_mode,
                )
                tag = f"{mode}|ma{window}|scale{scale}"
                summary_rows.append(_evaluate_combo("REGIME", tag, cfg, histories, regime_maps, events_by_stock, events_by_date, config))

    regime_df = _attach_scores(pd.DataFrame(summary_rows))
    best_regime = _select_best(regime_df, "REGIME")

    for multiplier in LEFT_CATCH_MULTIPLIERS:
        for gap_pct in MAX_GAP_CHASE_PCTS:
            for fallback_rule in FALLBACK_RULES:
                cfg = SearchConfig(
                    regime_mode=str(best_regime["regime_mode"]),
                    regime_ma_window=int(best_regime["regime_ma_window"]),
                    regime_risk_scale=float(best_regime["regime_risk_scale"]),
                    left_catch_multiplier=float(multiplier),
                    max_gap_chase_pct=float(gap_pct),
                    fallback_rule=str(fallback_rule),
                    shadow_threshold=base_search_cfg.shadow_threshold,
                    min_profit_to_trigger_shadow_exit=base_search_cfg.min_profit_to_trigger_shadow_exit,
                    shadow_exit_mode=base_search_cfg.shadow_exit_mode,
                )
                tag = f"mult{multiplier}|gap{gap_pct}|{fallback_rule}"
                summary_rows.append(_evaluate_combo("LEFT", tag, cfg, histories, regime_maps, events_by_stock, events_by_date, config))

    left_df = _attach_scores(pd.DataFrame(summary_rows))
    best_left = _select_best(left_df, "LEFT")

    for threshold in SHADOW_THRESHOLDS:
        for min_profit in MIN_PROFIT_TO_TRIGGER:
            for exit_mode in SHADOW_EXIT_MODES:
                cfg = SearchConfig(
                    regime_mode=str(best_left["regime_mode"]),
                    regime_ma_window=int(best_left["regime_ma_window"]),
                    regime_risk_scale=float(best_left["regime_risk_scale"]),
                    left_catch_multiplier=float(best_left["left_catch_multiplier"]),
                    max_gap_chase_pct=float(best_left["max_gap_chase_pct"]),
                    fallback_rule=str(best_left["fallback_rule"]),
                    shadow_threshold=float(threshold),
                    min_profit_to_trigger_shadow_exit=float(min_profit),
                    shadow_exit_mode=str(exit_mode),
                )
                tag = f"thr{threshold}|minp{min_profit}|{exit_mode}"
                summary_rows.append(_evaluate_combo("SHADOW", tag, cfg, histories, regime_maps, events_by_stock, events_by_date, config))

    full_df = _attach_scores(pd.DataFrame(summary_rows))
    best_shadow = _select_best(full_df, "SHADOW")

    final_cfg = SearchConfig(
        regime_mode=str(best_shadow["regime_mode"]),
        regime_ma_window=int(best_shadow["regime_ma_window"]),
        regime_risk_scale=float(best_shadow["regime_risk_scale"]),
        left_catch_multiplier=float(best_shadow["left_catch_multiplier"]),
        max_gap_chase_pct=float(best_shadow["max_gap_chase_pct"]),
        fallback_rule=str(best_shadow["fallback_rule"]),
        shadow_threshold=float(best_shadow["shadow_threshold"]),
        min_profit_to_trigger_shadow_exit=float(best_shadow["min_profit_to_trigger_shadow_exit"]),
        shadow_exit_mode=str(best_shadow["shadow_exit_mode"]),
    )
    summary_rows.append(_evaluate_combo("FINAL", "RECOMMENDED_BASELINE", final_cfg, histories, regime_maps, events_by_stock, events_by_date, config))

    summary_df = _attach_scores(pd.DataFrame(summary_rows))
    best_balanced = summary_df.sort_values(["balanced_score", "total_return_pct", "max_drawdown_pct"], ascending=[False, False, False]).iloc[0]
    best_low_miss = _select_best_low_miss(summary_df)
    final_baseline = _select_best(summary_df, "FINAL")

    csv_path = reports_dir / "stage6a_parameter_search_summary.csv"
    json_path = reports_dir / "stage6a_parameter_search.json"
    md_path = reports_dir / "stage6a_recommendation.md"

    summary_df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    payload = {
        "config": {
            "base_dir": str(config.base_dir),
            "start_date": config.start_date,
            "end_date": config.end_date,
            "max_files": config.max_files,
            "amount_min": config.amount_min,
            "target_profit_pct": config.target_profit_pct,
            "initial_trade_cash": config.initial_trade_cash,
            "commission_rate": config.commission_rate,
            "stamp_tax_rate": config.stamp_tax_rate,
            "buy_slippage_bps": config.buy_slippage_bps,
            "sell_slippage_bps": config.sell_slippage_bps,
            "lot_size": config.lot_size,
            "volume_shrink_search_disabled": True,
        },
        "best_balanced": best_balanced.to_dict(),
        "best_low_miss_trade_rate": best_low_miss.to_dict(),
        "best_regime": best_regime.to_dict(),
        "best_left": best_left.to_dict(),
        "best_shadow": best_shadow.to_dict(),
        "recommended_baseline": final_baseline.to_dict(),
        "rows": summary_df.to_dict(orient="records"),
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8-sig")
    _write_recommendation_md(md_path, summary_df, best_regime, best_left, best_shadow, best_balanced, best_low_miss, final_baseline)

    print("============================================================")
    print("Stage 6A 参数搜索完成")
    print(f"汇总 CSV : {csv_path}")
    print(f"汇总 JSON: {json_path}")
    print(f"建议报告 : {md_path}")
    print("============================================================")

    return {
        "stage_status": "SUCCESS_EXECUTED",
        "summary_path": str(csv_path),
        "summary_json_path": str(json_path),
        "recommendation_path": str(md_path),
        "row_count": int(len(summary_df)),
        "sample_file_count": int(len(loaded_files)),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stage 6A 小范围参数搜索")
    parser.add_argument("--base-dir", default=str(DEFAULT_BASE_DIR))
    parser.add_argument("--start-date", default="2024-01-01")
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--max-files", type=int, default=80)
    parser.add_argument("--amount-min", type=float, default=2e8)
    parser.add_argument("--target-profit-pct", type=float, default=0.08)
    parser.add_argument("--initial-trade-cash", type=float, default=100000.0)
    parser.add_argument("--commission-rate", type=float, default=0.0003)
    parser.add_argument("--stamp-tax-rate", type=float, default=0.001)
    parser.add_argument("--buy-slippage-bps", type=float, default=8.0)
    parser.add_argument("--sell-slippage-bps", type=float, default=8.0)
    parser.add_argument("--lot-size", type=int, default=100)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run(
        base_dir=args.base_dir,
        start_date=args.start_date,
        end_date=args.end_date,
        max_files=args.max_files,
        amount_min=args.amount_min,
        target_profit_pct=args.target_profit_pct,
        initial_trade_cash=args.initial_trade_cash,
        commission_rate=args.commission_rate,
        stamp_tax_rate=args.stamp_tax_rate,
        buy_slippage_bps=args.buy_slippage_bps,
        sell_slippage_bps=args.sell_slippage_bps,
        lot_size=args.lot_size,
    )


if __name__ == "__main__":
    main()
