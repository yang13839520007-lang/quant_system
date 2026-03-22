# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import math
import sys
from bisect import bisect_right
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import pandas as pd


ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


DEFAULT_BASE_DIR = ROOT_DIR


@dataclass(frozen=True)
class ExperimentConfig:
    experiment_id: str
    group_name: str
    arm: str
    description: str
    use_regime_safe: bool
    use_volume_shrink: bool
    entry_mode: str
    use_upper_shadow_exit: bool


@dataclass(frozen=True)
class MatrixRunConfig:
    base_dir: Path
    reports_dir: Path
    data_dir: Path
    universe_file: Path
    start_date: str | None
    end_date: str | None
    max_files: int | None
    amount_min: float
    vol_shrink_ratio: float
    target_profit_pct: float
    shadow_threshold: float
    initial_trade_cash: float
    commission_rate: float
    stamp_tax_rate: float
    buy_slippage_bps: float
    sell_slippage_bps: float
    lot_size: int
    regime_index_file: Path
    regime_ma_window: int


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


def _read_csv_auto(path: Path) -> pd.DataFrame:
    encodings = ("utf-8-sig", "utf-8", "gbk", "gb2312")
    last_error: Exception | None = None
    for encoding in encodings:
        try:
            return pd.read_csv(path, encoding=encoding)
        except Exception as exc:
            last_error = exc
    if last_error is not None:
        raise last_error
    raise FileNotFoundError(path)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
        if math.isnan(out):
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


def _calc_upper_shadow(open_price: float, high_price: float, low_price: float, close_price: float) -> float:
    full_range = high_price - low_price
    if full_range <= 0:
        return 0.0
    upper_shadow = high_price - max(open_price, close_price)
    return round(max(upper_shadow, 0.0) / full_range, 4)


def _pct_chg(row: pd.Series) -> float:
    pct_chg = _safe_float(row.get("pctChg"), default=float("nan"))
    if not math.isnan(pct_chg):
        return pct_chg / 100.0 if abs(pct_chg) > 1.5 else pct_chg
    preclose = _safe_float(row.get("preclose"), default=0.0)
    close_price = _safe_float(row.get("close"), default=0.0)
    if preclose <= 0 or close_price <= 0:
        return 0.0
    return close_price / preclose - 1.0


def _is_one_word_limit_up(row: pd.Series) -> bool:
    open_price = _safe_float(row.get("open"), default=0.0)
    high_price = _safe_float(row.get("high"), default=0.0)
    low_price = _safe_float(row.get("low"), default=0.0)
    close_price = _safe_float(row.get("close"), default=0.0)
    if min(open_price, high_price, low_price, close_price) <= 0:
        return False
    flat_board = abs(open_price - high_price) <= 1e-6 and abs(open_price - low_price) <= 1e-6 and abs(open_price - close_price) <= 1e-6
    return flat_board and _pct_chg(row) >= 0.095


def _is_one_word_limit_down(row: pd.Series) -> bool:
    open_price = _safe_float(row.get("open"), default=0.0)
    high_price = _safe_float(row.get("high"), default=0.0)
    low_price = _safe_float(row.get("low"), default=0.0)
    close_price = _safe_float(row.get("close"), default=0.0)
    if min(open_price, high_price, low_price, close_price) <= 0:
        return False
    flat_board = abs(open_price - high_price) <= 1e-6 and abs(open_price - low_price) <= 1e-6 and abs(open_price - close_price) <= 1e-6
    return flat_board and _pct_chg(row) <= -0.095


def _load_universe_files(config: MatrixRunConfig) -> list[Path]:
    files: list[Path] = []
    if config.universe_file.exists():
        universe_df = _read_csv_auto(config.universe_file)
        if "file_name" in universe_df.columns:
            for file_name in universe_df["file_name"].dropna().astype(str):
                candidate = config.data_dir / file_name
                if candidate.exists():
                    files.append(candidate)
    if not files:
        files = sorted(config.data_dir.glob("*.csv"))
    deduped: list[Path] = []
    seen: set[str] = set()
    for path in files:
        key = str(path.resolve()).lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(path)
    if config.max_files is not None:
        deduped = deduped[: config.max_files]
    return deduped


def _prepare_history(path: Path) -> pd.DataFrame:
    raw = _read_csv_auto(path)
    required = ["date", "code", "open", "high", "low", "close", "preclose", "volume", "amount"]
    if any(column not in raw.columns for column in required):
        return pd.DataFrame()
    extra_cols = [col for col in ("pctChg", "name", "is_st", "paused") if col in raw.columns]
    work = raw[required + extra_cols].copy()
    work["date"] = pd.to_datetime(work["date"], errors="coerce")
    work = work[work["date"].notna()].sort_values("date").reset_index(drop=True)
    if work.empty:
        return pd.DataFrame()
    for column in ("open", "high", "low", "close", "preclose", "volume", "amount"):
        work[column] = pd.to_numeric(work[column], errors="coerce")
    work["code"] = work["code"].map(_normalize_code)
    work["trade_date"] = work["date"].dt.strftime("%Y-%m-%d")
    work["ma5"] = work["close"].rolling(5, min_periods=5).mean()
    work["ma20"] = work["close"].rolling(20, min_periods=20).mean()
    work["vol_ma5"] = work["volume"].rolling(5, min_periods=5).mean()
    work["close_shift_50"] = work["close"].shift(50)
    work["close_return_50"] = work["close"] / work["close_shift_50"] - 1.0
    work["upper_shadow"] = work.apply(
        lambda row: _calc_upper_shadow(
            _safe_float(row["open"]),
            _safe_float(row["high"]),
            _safe_float(row["low"]),
            _safe_float(row["close"]),
        ),
        axis=1,
    )
    return work


def _build_rps50_map(files: Iterable[Path], start_date: str | None, end_date: str | None) -> dict[str, list[float]]:
    date_returns: dict[str, list[float]] = {}
    for path in files:
        hist = _prepare_history(path)
        if hist.empty:
            continue
        valid = hist[hist["close_return_50"].notna()].copy()
        if start_date is not None:
            valid = valid[valid["trade_date"] >= start_date]
        if end_date is not None:
            valid = valid[valid["trade_date"] <= end_date]
        for trade_date, ret_val in zip(valid["trade_date"], valid["close_return_50"]):
            date_returns.setdefault(str(trade_date), []).append(float(ret_val))
    return {trade_date: sorted(values) for trade_date, values in date_returns.items() if values}


def _calc_rps50(trade_date: str, close_return_50: float, rps_map: dict[str, list[float]]) -> float | None:
    values = rps_map.get(trade_date)
    if not values:
        return None
    rank = bisect_right(values, close_return_50)
    return rank / len(values)


def _build_regime_safe_map(config: MatrixRunConfig) -> tuple[dict[str, bool], list[str]]:
    caveats: list[str] = []
    if not config.regime_index_file.exists():
        caveats.append(f"Exp1 缺少历史大盘风控信号档案，且未找到代理指数文件 {config.regime_index_file.name}。Regime_Safe 实验将退化为恒放行。")
        return {}, caveats

    index_df = _prepare_history(config.regime_index_file)
    if index_df.empty:
        caveats.append(f"Exp1 无法从 {config.regime_index_file.name} 构造 Regime_Safe 代理。Regime_Safe 实验将退化为恒放行。")
        return {}, caveats

    ma_window = max(int(config.regime_ma_window), 2)
    index_df["regime_ma"] = index_df["close"].rolling(ma_window, min_periods=ma_window).mean()
    index_df["regime_safe"] = (index_df["close"] > index_df["regime_ma"]).fillna(False)
    regime_map = dict(zip(index_df["trade_date"], index_df["regime_safe"].astype(bool)))
    caveats.append(
        f"Exp1 未找到历史 daily_market_risk_signal.json 档案，已使用 {config.regime_index_file.name} 的 close > MA{ma_window} 作为 Regime_Safe 代理。"
    )
    return regime_map, caveats


def _candidate_signal(row: pd.Series, experiment: ExperimentConfig, config: MatrixRunConfig, rps50: float | None) -> bool:
    if rps50 is None:
        return False
    close_price = _safe_float(row.get("close"), default=0.0)
    ma5 = _safe_float(row.get("ma5"), default=0.0)
    ma20 = _safe_float(row.get("ma20"), default=0.0)
    volume = _safe_float(row.get("volume"), default=0.0)
    vol_ma5 = _safe_float(row.get("vol_ma5"), default=0.0)
    amount = _safe_float(row.get("amount"), default=0.0)
    if min(close_price, ma5, ma20, volume, vol_ma5) <= 0:
        return False
    if amount < config.amount_min:
        return False
    if close_price <= ma20:
        return False
    if close_price >= ma5:
        return False
    if experiment.use_volume_shrink and volume >= vol_ma5 * config.vol_shrink_ratio:
        return False
    return True


def _calc_buy_shares(entry_price: float, config: MatrixRunConfig) -> tuple[int, float]:
    if entry_price <= 0:
        return 0, 0.0
    shares = int(config.initial_trade_cash // entry_price)
    shares = (shares // config.lot_size) * config.lot_size
    while shares >= config.lot_size:
        gross = shares * entry_price
        commission = max(gross * config.commission_rate, 5.0)
        total_cost = gross + commission
        if total_cost <= config.initial_trade_cash:
            return shares, total_cost
        shares -= config.lot_size
    return 0, 0.0


def _try_enter_trade(
    signal_row: pd.Series,
    execution_row: pd.Series,
    experiment: ExperimentConfig,
    config: MatrixRunConfig,
) -> dict[str, Any]:
    open_price = _safe_float(execution_row.get("open"), default=0.0)
    low_price = _safe_float(execution_row.get("low"), default=0.0)
    if open_price <= 0 or low_price <= 0:
        return {"filled": False, "reason": "INVALID_EXECUTION_BAR"}

    if experiment.entry_mode == "OPEN_BUY":
        if _is_one_word_limit_up(execution_row):
            return {"filled": False, "reason": "ONE_WORD_LIMIT_UP"}
        raw_price = open_price
    else:
        target_buy_price_base = _safe_float(signal_row.get("low"), default=0.0) * 1.01
        planned_buy_price = min(open_price, target_buy_price_base) if target_buy_price_base > 0 else 0.0
        if planned_buy_price <= 0:
            return {"filled": False, "reason": "INVALID_TARGET_BUY_PRICE"}
        if _is_one_word_limit_down(execution_row):
            return {"filled": False, "reason": "ONE_WORD_LIMIT_DOWN"}
        if low_price > planned_buy_price:
            return {"filled": False, "reason": "TARGET_NOT_TOUCHED"}
        raw_price = planned_buy_price

    executed_price = round(raw_price * (1 + config.buy_slippage_bps / 10000.0), 4)
    shares, entry_total_cost = _calc_buy_shares(entry_price=executed_price, config=config)
    if shares < config.lot_size:
        return {"filled": False, "reason": "LESS_THAN_ONE_LOT"}
    return {
        "filled": True,
        "entry_price": executed_price,
        "shares": shares,
        "entry_total_cost": round(entry_total_cost, 2),
    }


def _evaluate_exit_trigger(
    row: pd.Series,
    entry_price: float,
    experiment: ExperimentConfig,
    config: MatrixRunConfig,
) -> tuple[str, str]:
    close_price = _safe_float(row.get("close"), default=0.0)
    ma20 = _safe_float(row.get("ma20"), default=0.0)
    upper_shadow = _safe_float(row.get("upper_shadow"), default=0.0)
    if close_price <= 0:
        return "", ""
    pnl_ratio = close_price / entry_price - 1.0
    if pnl_ratio >= config.target_profit_pct:
        return "TARGET_PROFIT_REACHED", f"收盘浮盈 {pnl_ratio:.2%} 达到固定止盈阈值 {config.target_profit_pct:.2%}"
    if experiment.use_upper_shadow_exit and pnl_ratio > 0 and upper_shadow >= config.shadow_threshold:
        return "UPPER_SHADOW_WARNING", f"长上影比例 {upper_shadow:.2f} 超过阈值 {config.shadow_threshold:.2f} 且收盘浮盈为正"
    if ma20 > 0 and close_price < ma20:
        return "CLOSE_BELOW_MA20", f"收盘价 {close_price:.2f} 跌破 MA20 {ma20:.2f}"
    return "", ""


def _simulate_stock(
    hist: pd.DataFrame,
    experiment: ExperimentConfig,
    config: MatrixRunConfig,
    rps_map: dict[str, list[float]],
    regime_safe_map: dict[str, bool],
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    trades: list[dict[str, Any]] = []
    counters = {
        "signal_count": 0,
        "regime_blocked_count": 0,
        "attempted_entry_count": 0,
        "missed_entry_count": 0,
    }
    if hist.empty or len(hist) < 52:
        return trades, counters

    idx = 50
    last_signal_index = len(hist) - 2
    while idx <= last_signal_index:
        signal_row = hist.iloc[idx]
        signal_date = str(signal_row["trade_date"])
        if config.start_date is not None and signal_date < config.start_date:
            idx += 1
            continue
        if config.end_date is not None and signal_date > config.end_date:
            break

        rps50 = _calc_rps50(signal_date, _safe_float(signal_row.get("close_return_50"), default=float("nan")), rps_map)
        if not _candidate_signal(signal_row, experiment, config, rps50):
            idx += 1
            continue

        counters["signal_count"] += 1
        if experiment.use_regime_safe and not regime_safe_map.get(signal_date, True):
            counters["regime_blocked_count"] += 1
            idx += 1
            continue

        counters["attempted_entry_count"] += 1
        entry_idx = idx + 1
        entry_row = hist.iloc[entry_idx]
        entry_res = _try_enter_trade(signal_row=signal_row, execution_row=entry_row, experiment=experiment, config=config)
        if not entry_res.get("filled", False):
            counters["missed_entry_count"] += 1
            idx += 1
            continue

        entry_price = float(entry_res["entry_price"])
        shares = int(entry_res["shares"])
        entry_total_cost = float(entry_res["entry_total_cost"])

        exited = False
        for review_idx in range(entry_idx, len(hist) - 1):
            review_row = hist.iloc[review_idx]
            exit_signal, exit_reason = _evaluate_exit_trigger(
                row=review_row,
                entry_price=entry_price,
                experiment=experiment,
                config=config,
            )
            if not exit_signal:
                continue

            exit_idx = review_idx + 1
            exit_row = hist.iloc[exit_idx]
            if _is_one_word_limit_down(exit_row):
                continue
            exit_open = _safe_float(exit_row.get("open"), default=0.0)
            if exit_open <= 0:
                continue

            executed_sell_price = round(exit_open * (1 - config.sell_slippage_bps / 10000.0), 4)
            gross_sell_amount = shares * executed_sell_price
            sell_commission = max(gross_sell_amount * config.commission_rate, 5.0)
            stamp_tax = gross_sell_amount * config.stamp_tax_rate
            net_sell_amount = gross_sell_amount - sell_commission - stamp_tax
            pnl_amount = net_sell_amount - entry_total_cost
            return_pct = pnl_amount / entry_total_cost * 100.0 if entry_total_cost > 0 else 0.0

            trades.append(
                {
                    "code": signal_row["code"],
                    "signal_date": signal_date,
                    "entry_date": str(entry_row["trade_date"]),
                    "exit_date": str(exit_row["trade_date"]),
                    "entry_mode": experiment.entry_mode,
                    "exit_signal": exit_signal,
                    "exit_reason": exit_reason,
                    "shares": shares,
                    "entry_price": round(entry_price, 4),
                    "exit_price": round(executed_sell_price, 4),
                    "holding_days": int(exit_idx - entry_idx + 1),
                    "pnl_amount": round(pnl_amount, 2),
                    "return_pct": round(return_pct, 4),
                }
            )
            idx = exit_idx
            exited = True
            break

        if exited:
            continue

        last_row = hist.iloc[-1]
        exit_price = _safe_float(last_row.get("close"), default=0.0)
        if exit_price > 0:
            gross_sell_amount = shares * exit_price
            sell_commission = max(gross_sell_amount * config.commission_rate, 5.0)
            stamp_tax = gross_sell_amount * config.stamp_tax_rate
            net_sell_amount = gross_sell_amount - sell_commission - stamp_tax
            pnl_amount = net_sell_amount - entry_total_cost
            return_pct = pnl_amount / entry_total_cost * 100.0 if entry_total_cost > 0 else 0.0
            trades.append(
                {
                    "code": signal_row["code"],
                    "signal_date": signal_date,
                    "entry_date": str(entry_row["trade_date"]),
                    "exit_date": str(last_row["trade_date"]),
                    "entry_mode": experiment.entry_mode,
                    "exit_signal": "END_OF_SAMPLE",
                    "exit_reason": "样本结束按最后一个收盘价结算",
                    "shares": shares,
                    "entry_price": round(entry_price, 4),
                    "exit_price": round(exit_price, 4),
                    "holding_days": int(len(hist) - entry_idx),
                    "pnl_amount": round(pnl_amount, 2),
                    "return_pct": round(return_pct, 4),
                }
            )
        break

    return trades, counters


def _summarize_experiment(
    experiment: ExperimentConfig,
    trades: list[dict[str, Any]],
    counters: dict[str, int],
) -> dict[str, Any]:
    trade_df = pd.DataFrame(trades)
    if trade_df.empty:
        return {
            "experiment_id": experiment.experiment_id,
            "group_name": experiment.group_name,
            "arm": experiment.arm,
            "description": experiment.description,
            "use_regime_safe": experiment.use_regime_safe,
            "use_volume_shrink": experiment.use_volume_shrink,
            "entry_mode": experiment.entry_mode,
            "use_upper_shadow_exit": experiment.use_upper_shadow_exit,
            "total_return_pct": 0.0,
            "win_rate": 0.0,
            "profit_factor": None,
            "pnl_ratio": None,
            "max_drawdown_pct": 0.0,
            "expectancy": 0.0,
            "avg_holding_days": 0.0,
            "trade_count": 0,
            "signal_count": counters["signal_count"],
            "regime_blocked_count": counters["regime_blocked_count"],
            "attempted_entry_count": counters["attempted_entry_count"],
            "missed_entry_count": counters["missed_entry_count"],
            "miss_trade_rate": round(
                counters["missed_entry_count"] / counters["attempted_entry_count"] * 100.0,
                4,
            ) if counters["attempted_entry_count"] else None,
        }

    trade_df["return_ratio"] = pd.to_numeric(trade_df["return_pct"], errors="coerce").fillna(0.0) / 100.0
    trade_df = trade_df.sort_values(["exit_date", "entry_date", "code"]).reset_index(drop=True)
    gross_profit = float(trade_df.loc[trade_df["return_ratio"] > 0, "return_ratio"].sum())
    gross_loss = float(trade_df.loc[trade_df["return_ratio"] < 0, "return_ratio"].sum())
    equity = 1.0
    equity_curve = [equity]
    for return_ratio in trade_df["return_ratio"]:
        equity += float(return_ratio)
        equity_curve.append(equity)

    profit_factor = None
    if gross_loss < 0:
        profit_factor = gross_profit / abs(gross_loss)
    miss_trade_rate = None
    if counters["attempted_entry_count"]:
        miss_trade_rate = counters["missed_entry_count"] / counters["attempted_entry_count"] * 100.0

    return {
        "experiment_id": experiment.experiment_id,
        "group_name": experiment.group_name,
        "arm": experiment.arm,
        "description": experiment.description,
        "use_regime_safe": experiment.use_regime_safe,
        "use_volume_shrink": experiment.use_volume_shrink,
        "entry_mode": experiment.entry_mode,
        "use_upper_shadow_exit": experiment.use_upper_shadow_exit,
        "total_return_pct": round((equity - 1.0) * 100.0, 4),
        "win_rate": round(float((trade_df["return_pct"] > 0).mean() * 100.0), 4),
        "profit_factor": round(profit_factor, 4) if profit_factor is not None else None,
        "pnl_ratio": round(profit_factor, 4) if profit_factor is not None else None,
        "max_drawdown_pct": _calc_max_drawdown(equity_curve),
        "expectancy": round(float(trade_df["return_pct"].mean()), 4),
        "avg_holding_days": round(float(trade_df["holding_days"].mean()), 4),
        "trade_count": int(len(trade_df)),
        "signal_count": counters["signal_count"],
        "regime_blocked_count": counters["regime_blocked_count"],
        "attempted_entry_count": counters["attempted_entry_count"],
        "missed_entry_count": counters["missed_entry_count"],
        "miss_trade_rate": round(miss_trade_rate, 4) if miss_trade_rate is not None else None,
    }


def _default_experiments() -> list[ExperimentConfig]:
    return [
        ExperimentConfig("BASELINE_ORIGINAL", "Baseline", "anchor", "原始版基线：无 Regime_Safe、无缩量过滤、T+1 开盘买入、固定止盈", False, False, "OPEN_BUY", False),
        ExperimentConfig("ROUTE_A_FULL_UPGRADED", "Baseline", "anchor", "升级版基线：Regime_Safe + 缩量过滤 + 左侧承接买入 + 长上影预警退出", True, True, "LEFT_CATCH", True),
        ExperimentConfig("EXP1_CONTROL", "Exp1", "control", "无 Regime_Safe，其余保持升级版", False, True, "LEFT_CATCH", True),
        ExperimentConfig("EXP1_TEST", "Exp1", "test", "加入 Regime_Safe，其余保持升级版", True, True, "LEFT_CATCH", True),
        ExperimentConfig("EXP2_CONTROL", "Exp2", "control", "无缩量条件，其余保持升级版", True, False, "LEFT_CATCH", True),
        ExperimentConfig("EXP2_TEST", "Exp2", "test", "加入缩量条件，其余保持升级版", True, True, "LEFT_CATCH", True),
        ExperimentConfig("EXP3_CONTROL", "Exp3", "control", "T+1 开盘直接买入，其余保持升级版", True, True, "OPEN_BUY", True),
        ExperimentConfig("EXP3_TEST", "Exp3", "test", "min(open, pre_low*1.01) 左侧承接买入，其余保持升级版", True, True, "LEFT_CATCH", True),
        ExperimentConfig("EXP4_CONTROL", "Exp4", "control", "固定止盈，其余保持升级版", True, True, "LEFT_CATCH", False),
        ExperimentConfig("EXP4_TEST", "Exp4", "test", "固定止盈 + 长上影预警退出，其余保持升级版", True, True, "LEFT_CATCH", True),
    ]


def _build_pairwise_delta(summary_df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for group_name in ("Exp1", "Exp2", "Exp3", "Exp4"):
        control = summary_df[(summary_df["group_name"] == group_name) & (summary_df["arm"] == "control")]
        test = summary_df[(summary_df["group_name"] == group_name) & (summary_df["arm"] == "test")]
        if control.empty or test.empty:
            continue
        control_row = control.iloc[0]
        test_row = test.iloc[0]
        rows.append(
            {
                "group_name": group_name,
                "control_id": control_row["experiment_id"],
                "test_id": test_row["experiment_id"],
                "delta_total_return_pct": round(_safe_float(test_row["total_return_pct"]) - _safe_float(control_row["total_return_pct"]), 4),
                "delta_win_rate": round(_safe_float(test_row["win_rate"]) - _safe_float(control_row["win_rate"]), 4),
                "delta_profit_factor": round(_safe_float(test_row["profit_factor"]) - _safe_float(control_row["profit_factor"]), 4),
                "delta_max_drawdown_pct": round(_safe_float(test_row["max_drawdown_pct"]) - _safe_float(control_row["max_drawdown_pct"]), 4),
                "delta_expectancy": round(_safe_float(test_row["expectancy"]) - _safe_float(control_row["expectancy"]), 4),
                "delta_avg_holding_days": round(_safe_float(test_row["avg_holding_days"]) - _safe_float(control_row["avg_holding_days"]), 4),
                "delta_trade_count": int(_safe_float(test_row["trade_count"]) - _safe_float(control_row["trade_count"])),
                "delta_miss_trade_rate": round(_safe_float(test_row["miss_trade_rate"]) - _safe_float(control_row["miss_trade_rate"]), 4),
            }
        )
    return pd.DataFrame(rows)


def _markdown_table(df: pd.DataFrame) -> str:
    if df.empty:
        return "_无数据_"
    safe_df = df.copy().where(pd.notna(df), "N/A")
    columns = list(safe_df.columns)
    lines = [
        "|" + "|".join(columns) + "|",
        "|" + "|".join(["---"] * len(columns)) + "|",
    ]
    for _, row in safe_df.iterrows():
        lines.append("|" + "|".join(str(row[col]) for col in columns) + "|")
    return "\n".join(lines)


def _write_report(
    path: Path,
    config: MatrixRunConfig,
    summary_df: pd.DataFrame,
    pairwise_df: pd.DataFrame,
    caveats: list[str],
) -> None:
    anchor_df = summary_df[summary_df["group_name"] == "Baseline"][
        ["experiment_id", "description", "total_return_pct", "win_rate", "profit_factor", "max_drawdown_pct", "expectancy", "avg_holding_days", "trade_count", "miss_trade_rate"]
    ]
    lines = [
        "# Stage 5 Strategy Comparison Report",
        "",
        "## 运行说明",
        "",
        f"- 样本起始日: `{config.start_date or '不限'}`",
        f"- 样本结束日: `{config.end_date or '不限'}`",
        f"- Universe 来源: `{config.universe_file}`",
        f"- 最大文件数: `{config.max_files if config.max_files is not None else '全部'}`",
        f"- Amount_Min: `{config.amount_min}`",
        f"- Vol_Shrink_Ratio: `{config.vol_shrink_ratio}`",
        f"- Target_Profit_Pct: `{config.target_profit_pct}`",
        f"- Shadow_Threshold: `{config.shadow_threshold}`",
        "",
        "## 基线对比",
        "",
        _markdown_table(anchor_df),
        "",
        "## 四组实验增量对比",
        "",
        _markdown_table(pairwise_df),
        "",
        "## 完整实验矩阵",
        "",
        _markdown_table(summary_df),
        "",
        "## 说明与限制",
        "",
    ]
    for caveat in caveats:
        lines.append(f"- {caveat}")
    lines.extend(
        [
            "- 指标按交易级别等额单笔资金的加总权益曲线计算，`total_return_pct` 与 `max_drawdown_pct` 不使用逐笔复利串接，避免并行交易被夸大。",
            "- `miss_trade_rate` 定义为尝试入场后未能成交的比例；Exp3 是重点参考项。",
            "- 历史日线文件缺少逐日 ST 标签档案，本阶段未单独评估 ST 过滤贡献，未伪造该项结果。",
            "- 原始 CSV 未提供板块级涨跌停制度，本阶段一字板锁死使用 OHLC 全等 + 约 10% 涨跌幅启发式识别。",
            "",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8-sig")


def run(
    base_dir: str = str(DEFAULT_BASE_DIR),
    start_date: str | None = "2024-01-01",
    end_date: str | None = None,
    max_files: int | None = None,
    amount_min: float = 2e8,
    vol_shrink_ratio: float = 0.8,
    target_profit_pct: float = 0.08,
    shadow_threshold: float = 0.60,
    initial_trade_cash: float = 100000.0,
    commission_rate: float = 0.0003,
    stamp_tax_rate: float = 0.001,
    buy_slippage_bps: float = 8.0,
    sell_slippage_bps: float = 8.0,
    lot_size: int = 100,
    universe_file: str | None = None,
    regime_index_file: str | None = None,
    regime_ma_window: int = 20,
) -> dict[str, Any]:
    base_path = Path(base_dir)
    reports_dir = base_path / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    data_dir = base_path / "stock_data_5years"
    config = MatrixRunConfig(
        base_dir=base_path,
        reports_dir=reports_dir,
        data_dir=data_dir,
        universe_file=Path(universe_file) if universe_file else reports_dir / "batch_backtest_summary.csv",
        start_date=start_date,
        end_date=end_date,
        max_files=max_files,
        amount_min=float(amount_min),
        vol_shrink_ratio=float(vol_shrink_ratio),
        target_profit_pct=float(target_profit_pct),
        shadow_threshold=float(shadow_threshold),
        initial_trade_cash=float(initial_trade_cash),
        commission_rate=float(commission_rate),
        stamp_tax_rate=float(stamp_tax_rate),
        buy_slippage_bps=float(buy_slippage_bps),
        sell_slippage_bps=float(sell_slippage_bps),
        lot_size=int(lot_size),
        regime_index_file=Path(regime_index_file) if regime_index_file else data_dir / "sh.000001.csv",
        regime_ma_window=int(regime_ma_window),
    )

    universe_files = _load_universe_files(config)
    if not universe_files:
        raise FileNotFoundError(f"未找到可用于实验矩阵的历史日线文件。data_dir={data_dir}")

    print("============================================================")
    print("Stage 5 实验矩阵开始")
    print(f"样本文件数: {len(universe_files)}")
    print(f"起始日期  : {config.start_date or '不限'}")
    print(f"结束日期  : {config.end_date or '不限'}")
    print("============================================================")

    rps_map = _build_rps50_map(universe_files, start_date=config.start_date, end_date=config.end_date)
    regime_safe_map, caveats = _build_regime_safe_map(config)
    experiments = _default_experiments()

    results: list[dict[str, Any]] = []
    for experiment in experiments:
        trade_records: list[dict[str, Any]] = []
        counters = {
            "signal_count": 0,
            "regime_blocked_count": 0,
            "attempted_entry_count": 0,
            "missed_entry_count": 0,
        }
        print(f"[*] 运行 {experiment.experiment_id} | {experiment.description}")
        for path in universe_files:
            hist = _prepare_history(path)
            if hist.empty:
                continue
            stock_trades, stock_counters = _simulate_stock(
                hist=hist,
                experiment=experiment,
                config=config,
                rps_map=rps_map,
                regime_safe_map=regime_safe_map,
            )
            trade_records.extend(stock_trades)
            for key in counters:
                counters[key] += stock_counters.get(key, 0)
        results.append(_summarize_experiment(experiment=experiment, trades=trade_records, counters=counters))

    summary_df = pd.DataFrame(results)
    pairwise_df = _build_pairwise_delta(summary_df)

    csv_path = reports_dir / "experiment_matrix_summary.csv"
    json_path = reports_dir / "experiment_matrix_summary.json"
    report_path = reports_dir / "strategy_comparison_report.md"

    summary_df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    payload = {
        "config": {
            "base_dir": str(config.base_dir),
            "data_dir": str(config.data_dir),
            "universe_file": str(config.universe_file),
            "start_date": config.start_date,
            "end_date": config.end_date,
            "max_files": config.max_files,
            "amount_min": config.amount_min,
            "vol_shrink_ratio": config.vol_shrink_ratio,
            "target_profit_pct": config.target_profit_pct,
            "shadow_threshold": config.shadow_threshold,
            "initial_trade_cash": config.initial_trade_cash,
            "commission_rate": config.commission_rate,
            "stamp_tax_rate": config.stamp_tax_rate,
            "buy_slippage_bps": config.buy_slippage_bps,
            "sell_slippage_bps": config.sell_slippage_bps,
            "lot_size": config.lot_size,
            "regime_index_file": str(config.regime_index_file),
            "regime_ma_window": config.regime_ma_window,
        },
        "caveats": caveats,
        "experiments": summary_df.to_dict(orient="records"),
        "pairwise_delta": pairwise_df.to_dict(orient="records"),
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8-sig")
    _write_report(report_path, config=config, summary_df=summary_df, pairwise_df=pairwise_df, caveats=caveats)

    print("============================================================")
    print("Stage 5 实验矩阵完成")
    print(f"汇总 CSV : {csv_path}")
    print(f"汇总 JSON: {json_path}")
    print(f"对比报告 : {report_path}")
    print("============================================================")

    return {
        "stage_status": "SUCCESS_EXECUTED",
        "summary_path": str(csv_path),
        "summary_json_path": str(json_path),
        "report_path": str(report_path),
        "experiment_count": int(len(summary_df)),
        "sample_file_count": int(len(universe_files)),
        "caveats": caveats,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stage 5 实验矩阵与基线对比")
    parser.add_argument("--base-dir", default=str(DEFAULT_BASE_DIR))
    parser.add_argument("--start-date", default="2024-01-01")
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--max-files", type=int, default=None)
    parser.add_argument("--amount-min", type=float, default=2e8)
    parser.add_argument("--vol-shrink-ratio", type=float, default=0.8)
    parser.add_argument("--target-profit-pct", type=float, default=0.08)
    parser.add_argument("--shadow-threshold", type=float, default=0.60)
    parser.add_argument("--initial-trade-cash", type=float, default=100000.0)
    parser.add_argument("--commission-rate", type=float, default=0.0003)
    parser.add_argument("--stamp-tax-rate", type=float, default=0.001)
    parser.add_argument("--buy-slippage-bps", type=float, default=8.0)
    parser.add_argument("--sell-slippage-bps", type=float, default=8.0)
    parser.add_argument("--lot-size", type=int, default=100)
    parser.add_argument("--universe-file", default=None)
    parser.add_argument("--regime-index-file", default=None)
    parser.add_argument("--regime-ma-window", type=int, default=20)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run(
        base_dir=args.base_dir,
        start_date=args.start_date,
        end_date=args.end_date,
        max_files=args.max_files,
        amount_min=args.amount_min,
        vol_shrink_ratio=args.vol_shrink_ratio,
        target_profit_pct=args.target_profit_pct,
        shadow_threshold=args.shadow_threshold,
        initial_trade_cash=args.initial_trade_cash,
        commission_rate=args.commission_rate,
        stamp_tax_rate=args.stamp_tax_rate,
        buy_slippage_bps=args.buy_slippage_bps,
        sell_slippage_bps=args.sell_slippage_bps,
        lot_size=args.lot_size,
        universe_file=args.universe_file,
        regime_index_file=args.regime_index_file,
        regime_ma_window=args.regime_ma_window,
    )


if __name__ == "__main__":
    main()
