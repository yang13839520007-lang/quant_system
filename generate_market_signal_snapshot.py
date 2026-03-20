# -*- coding: utf-8 -*-
"""
Created on Thu Mar 19 14:41:12 2026

@author: DELL
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from functools import lru_cache
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


DATE_ALIASES = [
    "trade_date", "trading_date", "date", "datetime", "dt",
]

CODE_ALIASES = [
    "code", "ts_code", "stock_code", "symbol",
]

NAME_ALIASES = [
    "name", "stock_name", "security_name",
]

OPEN_ALIASES = [
    "open", "open_price", "open_px", "today_open", "今开", "开盘", "开盘价",
]

HIGH_ALIASES = [
    "high", "high_price", "最高", "最高价",
]

LOW_ALIASES = [
    "low", "low_price", "最低", "最低价",
]

CLOSE_ALIASES = [
    "close", "close_price", "latest_price", "last_price", "current_price", "最新价", "收盘", "收盘价",
]

PREV_CLOSE_ALIASES = [
    "prev_close", "pre_close", "preclose", "last_close", "yesterday_close", "昨收", "昨收价",
]

VOLUME_ALIASES = [
    "volume", "vol", "成交量",
]

AMOUNT_ALIASES = [
    "amount", "turnover", "turnover_amount", "成交额",
]

TURNOVER_RATE_ALIASES = [
    "turnover_rate", "换手率",
]

PCT_CHG_ALIASES = [
    "pct_chg", "pctchg", "change_pct", "chg_pct", "涨跌幅", "涨跌幅%",
]

CHANGE_ALIASES = [
    "change", "chg", "price_change", "涨跌额", "diff",
]

MA10_ALIASES = [
    "ma10", "MA10",
]

MA20_ALIASES = [
    "ma20", "MA20",
]

HIGH_LIMIT_ALIASES = [
    "high_limit", "up_limit", "limit_up", "涨停价",
]

LOW_LIMIT_ALIASES = [
    "low_limit", "down_limit", "limit_down", "跌停价",
]

PAUSED_ALIASES = [
    "paused", "is_paused", "suspended", "停牌",
]

SOURCE_FILE_ALIASES = [
    "source_file", "file_path", "csv_path", "path",
]


@dataclass(frozen=True)
class SnapshotConfig:
    project_root: Path
    output_dir: Path
    candidate_path: Path
    trading_date: str
    source_file_column: str | None = None
    code_column: str | None = None
    name_column: str | None = None


def normalize_code(code: Any) -> str:
    text = str(code).strip().lower()
    if not text:
        return text

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
    if digits.startswith(("000", "001", "002", "003", "300", "301")):
        return f"sz.{digits}"
    if digits.startswith(("430", "831", "832", "833", "834", "835", "836", "837", "838", "839", "870", "871", "872", "873", "874", "875", "876", "877", "878", "879", "920")):
        return f"bj.{digits}"
    return f"sz.{digits}"


def _norm_col(name: str) -> str:
    return "".join(ch for ch in str(name).strip().lower() if ch.isalnum())


def pick_first_column(df: pd.DataFrame, aliases: list[str], required: bool = False) -> str | None:
    mapping = {_norm_col(col): col for col in df.columns}
    for alias in aliases:
        hit = mapping.get(_norm_col(alias))
        if hit is not None:
            return hit
    if required:
        raise KeyError(f"缺少必要字段，候选别名: {aliases}；当前字段: {list(df.columns)}")
    return None


def resolve_project_root(project_root: str | Path | None = None) -> Path:
    if project_root:
        return Path(project_root).resolve()
    return Path(__file__).resolve().parent


def _round_price(value: float | int | None) -> float | None:
    if value is None:
        return None
    try:
        dec = Decimal(str(float(value))).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        return float(dec)
    except Exception:
        return None


def _safe_num(value: Any) -> float | None:
    series = pd.to_numeric(pd.Series([value]), errors="coerce")
    item = series.iloc[0]
    if pd.isna(item):
        return None
    return float(item)


def _series_num(series: pd.Series | None, index=None, default: float | None = None) -> pd.Series:
    if series is None:
        if index is None:
            out = pd.Series(dtype=float)
        else:
            out = pd.Series(np.nan, index=index, dtype=float)
    else:
        out = pd.to_numeric(series, errors="coerce")
    if default is not None:
        out = out.fillna(default)
    return out


def _compute_limit_pct(code: str, name: str | None) -> float:
    nm = str(name or "").upper()
    if "ST" in nm:
        return 0.05
    code = normalize_code(code)
    if code.startswith("bj."):
        return 0.30
    digits = code.split(".")[-1]
    if digits.startswith("688"):
        return 0.20
    if digits.startswith(("300", "301")):
        return 0.20
    return 0.10


def _derive_limit_prices(prev_close: float | None, limit_pct: float) -> tuple[float | None, float | None]:
    if prev_close is None or prev_close <= 0:
        return None, None
    high = _round_price(prev_close * (1 + limit_pct))
    low = _round_price(prev_close * (1 - limit_pct))
    return high, low


def _search_source_file(project_root: Path, code: str) -> Path | None:
    digits = normalize_code(code).split(".")[-1]
    candidates = [
        project_root / "data" / "daily" / f"{digits}.csv",
        project_root / "data" / "daily_bar" / f"{digits}.csv",
        project_root / "data" / "stocks" / f"{digits}.csv",
        project_root / "dataset" / "daily" / f"{digits}.csv",
        project_root / "dataset" / "stocks" / f"{digits}.csv",
    ]
    for path in candidates:
        if path.exists():
            return path
    return None


@lru_cache(maxsize=16384)
def _read_source_csv_cached(path_str: str) -> pd.DataFrame:
    path = Path(path_str)
    if not path.exists():
        raise FileNotFoundError(f"源文件不存在: {path}")
    return pd.read_csv(path)


def _prepare_source_df(raw_df: pd.DataFrame, trading_date: str) -> tuple[pd.DataFrame, str]:
    date_col = pick_first_column(raw_df, DATE_ALIASES, required=True)
    df = raw_df.copy()
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df[df[date_col].notna()].copy()
    if df.empty:
        raise ValueError("源文件日期列全部为空")
    df = df.sort_values(date_col).reset_index(drop=True)
    df["__trade_date__"] = df[date_col].dt.strftime("%Y-%m-%d")
    df = df[df["__trade_date__"] <= trading_date].copy()
    if df.empty:
        raise ValueError("源文件在目标交易日前无有效数据")
    return df.reset_index(drop=True), date_col


def _compute_ma_from_close(source_df: pd.DataFrame, close_col: str, window: int) -> float | None:
    closes = pd.to_numeric(source_df[close_col], errors="coerce")
    if closes.dropna().empty:
        return None
    ma = closes.rolling(window=window, min_periods=1).mean().iloc[-1]
    if pd.isna(ma):
        return None
    return float(ma)


def _extract_snapshot_row(
    row: pd.Series,
    trading_date: str,
    project_root: Path,
    code_col: str,
    name_col: str | None,
    source_file_col: str | None,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    raw_code = row[code_col]
    code = normalize_code(raw_code)
    name = str(row[name_col]).strip() if name_col and pd.notna(row[name_col]) else ""

    source_path: Path | None = None
    if source_file_col and pd.notna(row[source_file_col]):
        source_path = Path(str(row[source_file_col]))
        if not source_path.is_absolute():
            source_path = (project_root / source_path).resolve()
        if not source_path.exists():
            source_path = None

    if source_path is None:
        source_path = _search_source_file(project_root, code)

    if source_path is None:
        miss = {
            "trading_date": trading_date,
            "code": code,
            "name": name,
            "reason": "source_file_not_found",
        }
        return None, miss

    try:
        raw_df = _read_source_csv_cached(str(source_path))
        source_df, _ = _prepare_source_df(raw_df, trading_date)
    except Exception as exc:
        miss = {
            "trading_date": trading_date,
            "code": code,
            "name": name,
            "reason": f"source_read_error:{exc}",
            "source_file": str(source_path),
        }
        return None, miss

    code_col_src = pick_first_column(source_df, CODE_ALIASES)
    name_col_src = pick_first_column(source_df, NAME_ALIASES)
    open_col = pick_first_column(source_df, OPEN_ALIASES)
    high_col = pick_first_column(source_df, HIGH_ALIASES)
    low_col = pick_first_column(source_df, LOW_ALIASES)
    close_col = pick_first_column(source_df, CLOSE_ALIASES, required=True)
    prev_close_col = pick_first_column(source_df, PREV_CLOSE_ALIASES)
    volume_col = pick_first_column(source_df, VOLUME_ALIASES)
    amount_col = pick_first_column(source_df, AMOUNT_ALIASES)
    turnover_rate_col = pick_first_column(source_df, TURNOVER_RATE_ALIASES)
    pct_chg_col = pick_first_column(source_df, PCT_CHG_ALIASES)
    change_col = pick_first_column(source_df, CHANGE_ALIASES)
    ma10_col = pick_first_column(source_df, MA10_ALIASES)
    ma20_col = pick_first_column(source_df, MA20_ALIASES)
    high_limit_col = pick_first_column(source_df, HIGH_LIMIT_ALIASES)
    low_limit_col = pick_first_column(source_df, LOW_LIMIT_ALIASES)
    paused_col = pick_first_column(source_df, PAUSED_ALIASES)

    exact_df = source_df[source_df["__trade_date__"] == trading_date].copy()
    if not exact_df.empty:
        current_row = exact_df.iloc[-1]
        current_idx = exact_df.index[-1]
        snapshot_mode = "EXECUTION_READY" if open_col else "REPLAY_PROXY"
        paused = False
    else:
        current_row = source_df.iloc[-1]
        current_idx = source_df.index[-1]
        snapshot_mode = "PAUSED_FALLBACK"
        paused = True

    prev_row = source_df.iloc[current_idx - 1] if current_idx - 1 >= 0 else None

    latest_price = _safe_num(current_row[close_col])
    open_price = _safe_num(current_row[open_col]) if open_col else latest_price
    if open_price is None:
        open_price = latest_price

    prev_close = _safe_num(current_row[prev_close_col]) if prev_close_col else None
    prev_close_source = prev_close_col if prev_close_col else None

    if prev_close is None and prev_row is not None:
        prev_close = _safe_num(prev_row[close_col])
        prev_close_source = f"prev_row_{close_col}"

    if prev_close is None and latest_price is not None and pct_chg_col:
        pct_value = _safe_num(current_row[pct_chg_col])
        if pct_value is not None:
            pct_ratio = pct_value / 100.0 if abs(pct_value) > 2 else pct_value
            if (1 + pct_ratio) != 0:
                prev_close = latest_price / (1 + pct_ratio)
                prev_close_source = f"derived_from_{pct_chg_col}"

    if prev_close is None and latest_price is not None and change_col:
        chg_value = _safe_num(current_row[change_col])
        if chg_value is not None:
            prev_close = latest_price - chg_value
            prev_close_source = f"derived_from_{change_col}"

    if prev_close is None:
        prev_close = open_price if open_price is not None else latest_price
        prev_close_source = "fallback_equal_open"

    latest_price = _round_price(latest_price)
    open_price = _round_price(open_price)
    prev_close = _round_price(prev_close)

    high_price = _round_price(_safe_num(current_row[high_col])) if high_col else latest_price
    low_price = _round_price(_safe_num(current_row[low_col])) if low_col else latest_price

    volume = _safe_num(current_row[volume_col]) if volume_col else None
    amount = _safe_num(current_row[amount_col]) if amount_col else None
    turnover_rate = _safe_num(current_row[turnover_rate_col]) if turnover_rate_col else None

    if amount is None and volume is not None and latest_price is not None:
        amount = volume * latest_price
    if volume is None and amount is not None and latest_price not in (None, 0):
        volume = amount / latest_price

    ma10 = _safe_num(current_row[ma10_col]) if ma10_col else None
    ma20 = _safe_num(current_row[ma20_col]) if ma20_col else None
    if ma10 is None:
        ma10 = _compute_ma_from_close(source_df, close_col, 10)
    if ma20 is None:
        ma20 = _compute_ma_from_close(source_df, close_col, 20)

    high_limit = _safe_num(current_row[high_limit_col]) if high_limit_col else None
    low_limit = _safe_num(current_row[low_limit_col]) if low_limit_col else None

    if high_limit is None or low_limit is None:
        limit_pct = _compute_limit_pct(code, name)
        derived_high_limit, derived_low_limit = _derive_limit_prices(prev_close, limit_pct)
        if high_limit is None:
            high_limit = derived_high_limit
        if low_limit is None:
            low_limit = derived_low_limit
    else:
        limit_pct = _compute_limit_pct(code, name)

    if paused_col:
        raw_paused = current_row[paused_col]
        if pd.isna(raw_paused):
            pass
        elif isinstance(raw_paused, (bool, np.bool_)):
            paused = bool(raw_paused)
        else:
            paused = str(raw_paused).strip().lower() in {"1", "true", "y", "yes"}

    if code_col_src and pd.notna(current_row[code_col_src]):
        code = normalize_code(current_row[code_col_src])
    if name_col_src and pd.notna(current_row[name_col_src]) and not name:
        name = str(current_row[name_col_src]).strip()

    trade_date_value = str(current_row["__trade_date__"])
    snapshot_quality = "EXECUTION_READY" if (trade_date_value == trading_date and open_col and prev_close is not None) else "REPLAY_PROXY"

    out = {
        "trade_date": trading_date,
        "code": code,
        "name": name,
        "source_file": str(source_path),
        "source_trade_date": trade_date_value,
        "snapshot_mode": snapshot_mode,
        "snapshot_quality": snapshot_quality,
        "paused": bool(paused),
        "open_price": open_price,
        "prev_close": prev_close,
        "latest_price": latest_price,
        "close_price": latest_price,
        "high_price": high_price,
        "low_price": low_price,
        "high_limit": _round_price(high_limit),
        "low_limit": _round_price(low_limit),
        "limit_pct": limit_pct,
        "volume": None if volume is None else float(volume),
        "amount": None if amount is None else float(amount),
        "turnover_amount": None if amount is None else float(amount),
        "turnover_rate": turnover_rate,
        "ma10": None if ma10 is None else float(ma10),
        "ma20": None if ma20 is None else float(ma20),
        "price_above_ma10": bool(latest_price is not None and ma10 is not None and latest_price >= ma10),
        "price_above_ma20": bool(latest_price is not None and ma20 is not None and latest_price >= ma20),
        "ma10_above_ma20": bool(ma10 is not None and ma20 is not None and ma10 >= ma20),
        "open_price_source": open_col if open_col else close_col,
        "prev_close_source": prev_close_source,
        "latest_price_source": close_col,
    }
    return out, None


def build_market_signal_snapshot(
    trading_date: str,
    project_root: str | Path | None = None,
    candidate_path: str | Path | None = None,
    output_dir: str | Path | None = None,
    source_file_column: str | None = None,
    code_column: str | None = None,
    name_column: str | None = None,
    **_: Any,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    resolved_project_root = resolve_project_root(project_root)
    resolved_output_dir = Path(output_dir) if output_dir else resolved_project_root / "reports"
    resolved_output_dir.mkdir(parents=True, exist_ok=True)

    resolved_candidate_path = Path(candidate_path) if candidate_path else (resolved_output_dir / "daily_candidates_all.csv")
    if not resolved_candidate_path.exists():
        raise FileNotFoundError(f"候选文件不存在: {resolved_candidate_path}")

    cfg = SnapshotConfig(
        project_root=resolved_project_root,
        output_dir=resolved_output_dir,
        candidate_path=resolved_candidate_path,
        trading_date=str(trading_date),
        source_file_column=source_file_column,
        code_column=code_column,
        name_column=name_column,
    )

    candidate_df = pd.read_csv(cfg.candidate_path)
    code_col = cfg.code_column or pick_first_column(candidate_df, CODE_ALIASES, required=True)
    name_col = cfg.name_column or pick_first_column(candidate_df, NAME_ALIASES)
    source_file_col = cfg.source_file_column or pick_first_column(candidate_df, SOURCE_FILE_ALIASES)

    print("============================================================")
    print("行情快照补数开始")
    print(f"目标交易日: {cfg.trading_date}")
    print(f"候选代码数: {len(candidate_df)}")
    print(f"输出目录  : {cfg.output_dir}")
    print("入口类型  : function")
    print("调用入口  : generate_market_signal_snapshot.main")
    print("============================================================")

    records: list[dict[str, Any]] = []
    misses: list[dict[str, Any]] = []

    for _, row in candidate_df.iterrows():
        snapshot, miss = _extract_snapshot_row(
            row=row,
            trading_date=cfg.trading_date,
            project_root=cfg.project_root,
            code_col=code_col,
            name_col=name_col,
            source_file_col=source_file_col,
        )
        if snapshot is not None:
            records.append(snapshot)
        if miss is not None:
            misses.append(miss)

    snapshot_df = pd.DataFrame(records)
    miss_df = pd.DataFrame(misses)

    if not snapshot_df.empty:
        snapshot_df = snapshot_df.sort_values(["code"], kind="stable").reset_index(drop=True)

    snapshot_path = cfg.output_dir / "market_signal_snapshot.csv"
    miss_path = cfg.output_dir / "market_signal_snapshot_miss.csv"

    snapshot_df.to_csv(snapshot_path, index=False, encoding="utf-8-sig")
    miss_df.to_csv(miss_path, index=False, encoding="utf-8-sig")

    summary = {
        "trading_date": cfg.trading_date,
        "candidate_count": int(len(candidate_df)),
        "hit_count": int(len(snapshot_df)),
        "miss_count": int(len(miss_df)),
        "snapshot_path": str(snapshot_path),
        "miss_path": str(miss_path),
        "execution_ready_count": int(snapshot_df["snapshot_quality"].eq("EXECUTION_READY").sum()) if not snapshot_df.empty and "snapshot_quality" in snapshot_df.columns else 0,
        "replay_proxy_count": int(snapshot_df["snapshot_quality"].eq("REPLAY_PROXY").sum()) if not snapshot_df.empty and "snapshot_quality" in snapshot_df.columns else 0,
    }

    print("============================================================")
    print("行情快照补数完成")
    print(f"命中数: {summary['hit_count']}")
    print(f"缺失数: {summary['miss_count']}")
    print(f"快照文件: {snapshot_path}")
    print(f"缺失文件: {miss_path}")
    print("============================================================")

    return snapshot_df, miss_df, summary


def main(**kwargs: Any) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    if not kwargs:
        parser = argparse.ArgumentParser(description="执行级行情快照生成")
        parser.add_argument("--trading-date", "--trade-date", dest="trading_date", required=True)
        parser.add_argument("--project-root", "--base-dir", dest="project_root", default=None)
        parser.add_argument("--candidate-path", dest="candidate_path", default=None)
        parser.add_argument("--output-dir", dest="output_dir", default=None)
        args, _ = parser.parse_known_args()
        kwargs = vars(args)

    trading_date = kwargs.get("trading_date") or kwargs.get("trade_date") or kwargs.get("target_trading_date")
    if not trading_date:
        raise ValueError("缺少 trading_date")

    return build_market_signal_snapshot(
        trading_date=str(trading_date),
        project_root=kwargs.get("project_root") or kwargs.get("base_dir") or kwargs.get("project_dir") or kwargs.get("root_dir"),
        candidate_path=kwargs.get("candidate_path") or kwargs.get("candidates_path") or kwargs.get("candidate_file") or kwargs.get("candidates_file"),
        output_dir=kwargs.get("output_dir") or kwargs.get("reports_dir"),
        source_file_column=kwargs.get("source_file_column"),
        code_column=kwargs.get("code_column"),
        name_column=kwargs.get("name_column"),
    )


def run(**kwargs: Any) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    return main(**kwargs)


def generate_market_signal_snapshot(**kwargs: Any) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    return main(**kwargs)


if __name__ == "__main__":
    main()