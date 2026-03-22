# -*- coding: utf-8 -*-
"""
Created on Thu Mar 19 15:06:46 2026

@author: DELL
"""

# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import pandas as pd


STAGE_NO = 8
STAGE_NAME = "收盘复盘层"
TZ_SH = ZoneInfo("Asia/Shanghai")


@dataclass
class CloseReviewPaths:
    report_dir: Path
    market_snapshot: Path
    open_execution: Path
    intraday_recheck: Path
    portfolio_plan_risk_checked: Path
    portfolio_plan_top5: Path
    daily_close_review: Path
    daily_close_positions: Path
    daily_next_day_watchlist: Path
    daily_close_review_summary: Path
    daily_close_review_runtime: Path


def _now_sh_str() -> str:
    return datetime.now(TZ_SH).strftime("%Y-%m-%d %H:%M:%S")


def _safe_float(v, default=0.0) -> float:
    try:
        if v is None or (isinstance(v, float) and math.isnan(v)):
            return float(default)
        s = str(v).strip().replace(",", "")
        if s == "":
            return float(default)
        return float(s)
    except Exception:
        return float(default)


def _normalize_code(value: str) -> str:
    s = str(value).strip().lower()
    if not s:
        return ""

    s = s.replace(" ", "")
    if s.startswith(("sh.", "sz.", "bj.")):
        return s

    digits = "".join(ch for ch in s if ch.isdigit())
    if len(digits) != 6:
        return s

    if digits.startswith(("60", "68", "90")):
        return f"sh.{digits}"
    if digits.startswith(("00", "30", "20")):
        return f"sz.{digits}"
    if digits.startswith(("43", "83", "87")):
        return f"bj.{digits}"
    return digits


def _read_csv_auto(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()

    encodings = ["utf-8-sig", "utf-8", "gbk", "gb2312"]
    last_err = None
    for enc in encodings:
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception as e:
            last_err = e
    raise last_err  # noqa


def _first_existing_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols = {c.lower(): c for c in df.columns}
    for name in candidates:
        if name.lower() in cols:
            return cols[name.lower()]
    return None


def _coalesce_col(df: pd.DataFrame, candidates: List[str], default=None) -> pd.Series:
    col = _first_existing_col(df, candidates)
    if col is None:
        return pd.Series([default] * len(df), index=df.index)
    return df[col]


def _file_meta(path: Path) -> Dict:
    if not path.exists():
        return {
            "exists": False,
            "path": str(path),
            "size": 0,
            "mtime": None,
        }
    stat = path.stat()
    return {
        "exists": True,
        "path": str(path),
        "size": int(stat.st_size),
        "mtime": datetime.fromtimestamp(stat.st_mtime, TZ_SH).strftime("%Y-%m-%d %H:%M:%S"),
    }


class CloseReviewManager:
    def __init__(self, base_dir: str = r"C:\quant_system", report_dir: Optional[str] = None):
        self.base_dir = Path(base_dir)
        self.report_dir = Path(report_dir) if report_dir else self.base_dir / "reports"
        self.report_dir.mkdir(parents=True, exist_ok=True)
        self.paths = CloseReviewPaths(
            report_dir=self.report_dir,
            market_snapshot=self.report_dir / "market_signal_snapshot.csv",
            open_execution=self.report_dir / "daily_open_execution_decision.csv",
            intraday_recheck=self.report_dir / "daily_intraday_recheck_decision.csv",
            portfolio_plan_risk_checked=self.report_dir / "daily_portfolio_plan_risk_checked.csv",
            portfolio_plan_top5=self.report_dir / "daily_portfolio_plan_top5.csv",
            daily_close_review=self.report_dir / "daily_close_review.csv",
            daily_close_positions=self.report_dir / "daily_close_positions.csv",
            daily_next_day_watchlist=self.report_dir / "daily_next_day_watchlist.csv",
            daily_close_review_summary=self.report_dir / "daily_close_review_summary.txt",
            daily_close_review_runtime=self.report_dir / "daily_close_review_runtime.json",
        )

    def run(self, trading_date: str) -> Dict:
        base_df, source_name, source_meta = self._load_position_base()
        snap_df, snapshot_meta = self._load_market_snapshot()

        if base_df.empty:
            review_df = self._build_empty_review_frame()
            positions_df = review_df.copy()
            watchlist_df = review_df.copy()
        else:
            review_df = self._build_review_frame(base_df, snap_df, trading_date)
            positions_df = review_df[review_df["filled_shares"] > 0].copy()
            watchlist_df = self._build_watchlist(review_df)

        summary_text, summary_metrics = self._build_summary_text(
            review_df=review_df,
            positions_df=positions_df,
            watchlist_df=watchlist_df,
            trading_date=trading_date,
            position_source=source_name,
        )

        review_df.to_csv(self.paths.daily_close_review, index=False, encoding="utf-8-sig")
        positions_df.to_csv(self.paths.daily_close_positions, index=False, encoding="utf-8-sig")
        watchlist_df.to_csv(self.paths.daily_next_day_watchlist, index=False, encoding="utf-8-sig")
        self.paths.daily_close_review_summary.write_text(summary_text, encoding="utf-8")

        runtime_payload = {
            "stage_no": STAGE_NO,
            "stage_name": STAGE_NAME,
            "trade_date": trading_date,
            "executed_at": _now_sh_str(),
            "execution_mode": "NONCORE_FORCE_EXECUTE",
            "reuse_allowed": False,
            "reuse_hit": False,
            "position_source": source_name,
            "source_files": {
                "position_source": source_meta,
                "market_snapshot": snapshot_meta,
            },
            "output_files": {
                "daily_close_review": _file_meta(self.paths.daily_close_review),
                "daily_close_positions": _file_meta(self.paths.daily_close_positions),
                "daily_next_day_watchlist": _file_meta(self.paths.daily_next_day_watchlist),
                "daily_close_review_summary": _file_meta(self.paths.daily_close_review_summary),
            },
            "metrics": summary_metrics,
        }
        self.paths.daily_close_review_runtime.write_text(
            json.dumps(runtime_payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        return {
            "stage_no": STAGE_NO,
            "stage_name": STAGE_NAME,
            "stage_status": "SUCCESS_EXECUTED",
            "message": "收盘复盘层执行完成（非核心强制执行，不允许复用）",
            "trade_date": trading_date,
            "review_count": int(len(review_df)),
            "position_count": int(len(positions_df)),
            "watchlist_count": int(len(watchlist_df)),
            "source_name": source_name,
            "outputs": {
                "daily_close_review": str(self.paths.daily_close_review),
                "daily_close_positions": str(self.paths.daily_close_positions),
                "daily_next_day_watchlist": str(self.paths.daily_next_day_watchlist),
                "daily_close_review_summary": str(self.paths.daily_close_review_summary),
                "daily_close_review_runtime": str(self.paths.daily_close_review_runtime),
            },
            **summary_metrics,
        }

    def _load_position_base(self) -> Tuple[pd.DataFrame, str, Dict]:
        candidates = [
            ("daily_intraday_recheck_decision.csv", self.paths.intraday_recheck),
            ("daily_open_execution_decision.csv", self.paths.open_execution),
            ("daily_portfolio_plan_risk_checked.csv", self.paths.portfolio_plan_risk_checked),
            ("daily_portfolio_plan_top5.csv", self.paths.portfolio_plan_top5),
        ]

        for source_name, path in candidates:
            df = _read_csv_auto(path)
            if df.empty:
                continue

            normalized = self._normalize_position_source(df, source_name)
            normalized = normalized[normalized["code"] != ""].copy()
            normalized["filled_shares"] = normalized["filled_shares"].fillna(0).astype(int)
            normalized = normalized[normalized["filled_shares"] > 0].copy()

            if normalized.empty:
                continue

            normalized["source_file"] = source_name
            normalized = (
                normalized.sort_values(
                    by=["execution_rank", "filled_shares", "avg_fill_price"],
                    ascending=[True, False, False],
                )
                .drop_duplicates(subset=["code"], keep="first")
                .reset_index(drop=True)
            )
            return normalized, source_name, _file_meta(path)

        return pd.DataFrame(), "NO_POSITION_SOURCE", {"exists": False}

    def _normalize_position_source(self, df: pd.DataFrame, source_name: str) -> pd.DataFrame:
        out = pd.DataFrame(index=df.index)

        out["execution_rank"] = pd.to_numeric(
            _coalesce_col(df, ["execution_rank", "rank", "priority_rank"], default=9999),
            errors="coerce",
        ).fillna(9999).astype(int)

        out["code"] = _coalesce_col(df, ["code", "ts_code", "symbol"], default="").astype(str).map(_normalize_code)

        out["filled_shares"] = pd.to_numeric(
            _coalesce_col(
                df,
                [
                    "filled_shares",
                    "position_shares",
                    "actual_shares",
                    "executed_shares",
                    "net_position_shares",
                    "suggested_shares",
                    "shares",
                ],
                default=0,
            ),
            errors="coerce",
        ).fillna(0).astype(int)

        out["avg_fill_price"] = pd.to_numeric(
            _coalesce_col(
                df,
                [
                    "avg_fill_price",
                    "average_fill_price",
                    "filled_avg_price",
                    "avg_price",
                    "buy_price",
                    "entry_price",
                    "deal_price",
                ],
                default=0,
            ),
            errors="coerce",
        ).fillna(0.0)

        out["stop_loss"] = pd.to_numeric(
            _coalesce_col(df, ["stop_loss", "stop_loss_price", "risk_stop_loss"], default=0),
            errors="coerce",
        ).fillna(0.0)

        out["target_price"] = pd.to_numeric(
            _coalesce_col(df, ["target_price", "take_profit_price", "tp_price"], default=0),
            errors="coerce",
        ).fillna(0.0)

        out["action"] = _coalesce_col(df, ["action", "decision", "strategy_action"], default="正常跟踪").astype(str)
        out["score"] = pd.to_numeric(
            _coalesce_col(df, ["score", "final_score", "composite_score"], default=0),
            errors="coerce",
        ).fillna(0.0)
        out["heat_level"] = _coalesce_col(df, ["heat_level", "heat", "signal_heat"], default="正常").astype(str)
        out["signal_date"] = _coalesce_col(df, ["signal_date", "trade_date"], default="").astype(str)
        out["entry_mode"] = _coalesce_col(df, ["entry_mode"], default="").astype(str)
        out["stop_mode"] = _coalesce_col(df, ["stop_mode"], default="").astype(str)
        out["route_a_signal"] = _coalesce_col(df, ["route_a_signal"], default=False)
        out["target_profit_pct"] = pd.to_numeric(
            _coalesce_col(df, ["target_profit_pct"], default=0),
            errors="coerce",
        ).fillna(0.0)
        out["shadow_threshold"] = pd.to_numeric(
            _coalesce_col(df, ["shadow_threshold"], default=0),
            errors="coerce",
        ).fillna(0.0)

        if source_name in {"daily_portfolio_plan_risk_checked.csv", "daily_portfolio_plan_top5.csv"}:
            out["source_position_type"] = "PLAN_FALLBACK"
        else:
            out["source_position_type"] = "EXECUTED"

        return out

    def _load_market_snapshot(self) -> Tuple[pd.DataFrame, Dict]:
        df = _read_csv_auto(self.paths.market_snapshot)
        if df.empty:
            return pd.DataFrame(), _file_meta(self.paths.market_snapshot)

        snap = pd.DataFrame(index=df.index)
        snap["code"] = _coalesce_col(df, ["code", "ts_code", "symbol"], default="").astype(str).map(_normalize_code)
        snap["close_price"] = pd.to_numeric(
            _coalesce_col(
                df,
                ["close_price", "close", "last_price", "price", "snapshot_price", "latest_price"],
                default=None,
            ),
            errors="coerce",
        )
        snap["prev_close"] = pd.to_numeric(
            _coalesce_col(df, ["prev_close", "pre_close", "yclose"], default=None),
            errors="coerce",
        )
        snap["open_price"] = pd.to_numeric(_coalesce_col(df, ["open_price", "open"], default=None), errors="coerce")
        snap["high_price"] = pd.to_numeric(_coalesce_col(df, ["high_price", "high"], default=None), errors="coerce")
        snap["low_price"] = pd.to_numeric(_coalesce_col(df, ["low_price", "low"], default=None), errors="coerce")
        snap["volume"] = pd.to_numeric(_coalesce_col(df, ["volume", "vol"], default=None), errors="coerce")
        snap["amount"] = pd.to_numeric(_coalesce_col(df, ["amount", "turnover"], default=None), errors="coerce")
        snap["snapshot_mode"] = _coalesce_col(df, ["snapshot_mode"], default="UNKNOWN").astype(str)
        snap["snapshot_quality"] = _coalesce_col(df, ["snapshot_quality"], default="UNKNOWN").astype(str)
        snap["ma20"] = pd.to_numeric(_coalesce_col(df, ["ma20", "MA20"], default=None), errors="coerce")

        snap = snap[snap["code"] != ""].copy()
        snap = snap.drop_duplicates(subset=["code"], keep="first").reset_index(drop=True)
        return snap, _file_meta(self.paths.market_snapshot)

    def _build_empty_review_frame(self) -> pd.DataFrame:
        cols = [
            "execution_rank", "code", "filled_shares", "avg_fill_price", "close_price", "prev_close",
            "open_price", "high_price", "low_price", "stop_loss", "target_price", "market_change_pct",
            "unrealized_pnl_amt", "unrealized_pnl_pct", "stop_loss_gap_pct", "target_gap_pct", "ma20", "upper_shadow",
            "position_status", "next_day_action", "action", "score", "heat_level", "snapshot_mode",
            "snapshot_quality", "source_position_type", "source_file", "signal_date", "entry_mode", "stop_mode",
            "route_a_signal", "target_profit_pct", "shadow_threshold", "route_a_exit_signal", "exit_reason",
        ]
        return pd.DataFrame(columns=cols)

    def _build_review_frame(self, base_df: pd.DataFrame, snap_df: pd.DataFrame, trading_date: str) -> pd.DataFrame:
        if snap_df.empty:
            merged = base_df.copy()
            merged["close_price"] = pd.NA
            merged["prev_close"] = pd.NA
            merged["open_price"] = pd.NA
            merged["high_price"] = pd.NA
            merged["low_price"] = pd.NA
            merged["volume"] = pd.NA
            merged["amount"] = pd.NA
            merged["snapshot_mode"] = "MISSING"
            merged["snapshot_quality"] = "MISSING"
        else:
            merged = base_df.merge(snap_df, on="code", how="left")

        for col in [
            "avg_fill_price", "close_price", "prev_close", "open_price", "high_price",
            "low_price", "stop_loss", "target_price", "score",
        ]:
            merged[col] = pd.to_numeric(merged[col], errors="coerce")

        merged["market_change_pct"] = (
            (merged["close_price"] - merged["prev_close"]) / merged["prev_close"] * 100.0
        ).where(merged["prev_close"] > 0)

        merged["unrealized_pnl_amt"] = (
            (merged["close_price"] - merged["avg_fill_price"]) * merged["filled_shares"]
        ).where(merged["close_price"].notna())

        merged["unrealized_pnl_pct"] = (
            (merged["close_price"] - merged["avg_fill_price"]) / merged["avg_fill_price"] * 100.0
        ).where(merged["avg_fill_price"] > 0)

        merged["stop_loss_gap_pct"] = (
            (merged["close_price"] - merged["stop_loss"]) / merged["stop_loss"] * 100.0
        ).where((merged["stop_loss"] > 0) & merged["close_price"].notna())

        merged["target_gap_pct"] = (
            (merged["target_price"] - merged["close_price"]) / merged["target_price"] * 100.0
        ).where((merged["target_price"] > 0) & merged["close_price"].notna())
        merged["upper_shadow"] = merged.apply(self._calc_upper_shadow, axis=1)
        route_exit_df = merged.apply(self._calc_route_a_exit_fields, axis=1, result_type="expand")
        route_exit_df.columns = ["route_a_exit_signal", "exit_reason"]
        merged[["route_a_exit_signal", "exit_reason"]] = route_exit_df

        merged["position_status"] = merged.apply(self._calc_position_status, axis=1)
        merged["next_day_action"] = merged.apply(self._calc_next_day_action, axis=1)
        merged["trade_date"] = trading_date
        merged["review_time"] = _now_sh_str()

        ordered_cols = [
            "execution_rank", "code", "filled_shares", "avg_fill_price", "close_price", "prev_close",
            "open_price", "high_price", "low_price", "stop_loss", "target_price", "market_change_pct",
            "unrealized_pnl_amt", "unrealized_pnl_pct", "stop_loss_gap_pct", "target_gap_pct", "ma20", "upper_shadow",
            "position_status", "next_day_action", "action", "score", "heat_level", "snapshot_mode",
            "snapshot_quality", "source_position_type", "source_file", "signal_date", "entry_mode", "stop_mode",
            "route_a_signal", "target_profit_pct", "shadow_threshold", "route_a_exit_signal", "exit_reason",
            "trade_date", "review_time",
        ]
        for col in ordered_cols:
            if col not in merged.columns:
                merged[col] = pd.NA

        merged = merged[ordered_cols].copy()
        merged = merged.sort_values(
            by=["execution_rank", "unrealized_pnl_pct", "code"],
            ascending=[True, False, True],
        ).reset_index(drop=True)
        return merged

    def _calc_position_status(self, row: pd.Series) -> str:
        close_price = _safe_float(row.get("close_price"), default=float("nan"))
        stop_loss = _safe_float(row.get("stop_loss"), default=0)
        target_price = _safe_float(row.get("target_price"), default=0)
        pnl_pct = _safe_float(row.get("unrealized_pnl_pct"), default=0)

        if math.isnan(close_price) or close_price <= 0:
            return "缺少收盘口径"
        if stop_loss > 0 and close_price <= stop_loss:
            return "触发止损"
        if stop_loss > 0 and close_price <= stop_loss * 1.015:
            return "接近止损"
        if target_price > 0 and close_price >= target_price:
            return "触发止盈"
        if target_price > 0 and close_price >= target_price * 0.985:
            return "接近止盈"
        if pnl_pct >= 5.0:
            return "强势盈利持有"
        if pnl_pct >= 2.0:
            return "盈利持有"
        if pnl_pct <= -3.0:
            return "弱势亏损观察"
        if pnl_pct <= -1.5:
            return "亏损观察"
        return "正常持有"

    def _calc_next_day_action(self, row: pd.Series) -> str:
        route_a_exit_signal = str(row.get("route_a_exit_signal", "")).strip()
        if route_a_exit_signal:
            return "次日开盘卖出"

        status = str(row.get("position_status", "")).strip()
        snapshot_quality = str(row.get("snapshot_quality", "")).strip().upper()
        pnl_pct = _safe_float(row.get("unrealized_pnl_pct"), default=0)

        if status == "缺少收盘口径":
            return "补行情后复核"
        if snapshot_quality in {"LOW", "BAD", "MISSING", "UNKNOWN"}:
            return "先校验快照再决策"
        if status == "触发止损":
            return "次日竞价优先止损"
        if status == "接近止损":
            return "次日优先减仓观察"
        if status == "触发止盈":
            return "次日分批止盈"
        if status == "接近止盈":
            return "次日冲高分批止盈"
        if pnl_pct >= 5.0:
            return "持有并上移止盈"
        if pnl_pct <= -3.0:
            return "弱留强汰"
        return "正常跟踪"

    def _calc_upper_shadow(self, row: pd.Series) -> float:
        high_price = _safe_float(row.get("high_price"), default=float("nan"))
        low_price = _safe_float(row.get("low_price"), default=float("nan"))
        open_price = _safe_float(row.get("open_price"), default=float("nan"))
        close_price = _safe_float(row.get("close_price"), default=float("nan"))
        if any(math.isnan(value) for value in [high_price, low_price, open_price, close_price]):
            return 0.0
        full_range = high_price - low_price
        if full_range <= 0:
            return 0.0
        upper_shadow = high_price - max(open_price, close_price)
        return round(max(upper_shadow, 0.0) / full_range, 4)

    def _calc_route_a_exit_fields(self, row: pd.Series) -> Tuple[str, str]:
        entry_mode = str(row.get("entry_mode", "")).strip().upper()
        route_a_signal = str(row.get("route_a_signal", "")).strip().lower() in {"1", "true", "yes", "y"}
        if entry_mode != "ROUTE_A_LEFT_CATCH" and not route_a_signal:
            return "", ""

        pnl_ratio = _safe_float(row.get("unrealized_pnl_pct"), default=0.0) / 100.0
        target_profit_pct = _safe_float(row.get("target_profit_pct"), default=0.0)
        shadow_threshold = _safe_float(row.get("shadow_threshold"), default=0.0)
        upper_shadow = _safe_float(row.get("upper_shadow"), default=0.0)
        close_price = _safe_float(row.get("close_price"), default=float("nan"))
        ma20 = _safe_float(row.get("ma20"), default=float("nan"))

        if target_profit_pct > 0 and pnl_ratio >= target_profit_pct:
            return "TARGET_PROFIT_REACHED", f"收盘浮盈 {pnl_ratio:.2%} 达到目标收益阈值 {target_profit_pct:.2%}"
        if pnl_ratio > 0 and shadow_threshold > 0 and upper_shadow >= shadow_threshold:
            return "UPPER_SHADOW_WARNING", f"长上影比例 {upper_shadow:.2f} 超过阈值 {shadow_threshold:.2f} 且浮盈为正"
        if not math.isnan(close_price) and not math.isnan(ma20) and ma20 > 0 and close_price < ma20:
            return "CLOSE_BELOW_MA20", f"收盘价 {close_price:.2f} 跌破 MA20 {ma20:.2f}"
        return "", ""

    def _build_watchlist(self, review_df: pd.DataFrame) -> pd.DataFrame:
        if review_df.empty:
            return review_df.copy()

        focus_status = {"触发止损", "接近止损", "触发止盈", "接近止盈", "弱势亏损观察", "缺少收盘口径"}
        out = review_df[
            (review_df["position_status"].isin(focus_status))
            | (review_df["next_day_action"].isin({"先校验快照再决策", "弱留强汰"}))
            | (review_df["route_a_exit_signal"].astype(str).str.strip() != "")
        ].copy()

        out = out.sort_values(
            by=["position_status", "unrealized_pnl_pct", "execution_rank"],
            ascending=[True, True, True],
        ).reset_index(drop=True)
        return out

    def _build_summary_text(
        self,
        review_df: pd.DataFrame,
        positions_df: pd.DataFrame,
        watchlist_df: pd.DataFrame,
        trading_date: str,
        position_source: str,
    ) -> Tuple[str, Dict]:
        pnl_amt = pd.to_numeric(review_df.get("unrealized_pnl_amt", pd.Series(dtype=float)), errors="coerce").fillna(0)
        pnl_pct = pd.to_numeric(review_df.get("unrealized_pnl_pct", pd.Series(dtype=float)), errors="coerce")

        positive_count = int((pnl_amt > 0).sum()) if not pnl_amt.empty else 0
        negative_count = int((pnl_amt < 0).sum()) if not pnl_amt.empty else 0
        near_stop_count = int((review_df.get("position_status", pd.Series(dtype=str)) == "接近止损").sum())
        hit_stop_count = int((review_df.get("position_status", pd.Series(dtype=str)) == "触发止损").sum())
        near_tp_count = int((review_df.get("position_status", pd.Series(dtype=str)) == "接近止盈").sum())
        hit_tp_count = int((review_df.get("position_status", pd.Series(dtype=str)) == "触发止盈").sum())

        total_cost = float((pd.to_numeric(review_df.get("avg_fill_price"), errors="coerce").fillna(0) *
                            pd.to_numeric(review_df.get("filled_shares"), errors="coerce").fillna(0)).sum())
        total_market_value = float((pd.to_numeric(review_df.get("close_price"), errors="coerce").fillna(0) *
                                    pd.to_numeric(review_df.get("filled_shares"), errors="coerce").fillna(0)).sum())
        total_pnl_amt = float(pnl_amt.sum()) if not pnl_amt.empty else 0.0
        avg_pnl_pct = float(pnl_pct.mean()) if not pnl_pct.dropna().empty else 0.0

        metrics = {
            "review_count": int(len(review_df)),
            "position_count": int(len(positions_df)),
            "watchlist_count": int(len(watchlist_df)),
            "positive_count": positive_count,
            "negative_count": negative_count,
            "near_stop_count": near_stop_count,
            "hit_stop_count": hit_stop_count,
            "near_take_profit_count": near_tp_count,
            "hit_take_profit_count": hit_tp_count,
            "total_cost": round(total_cost, 2),
            "total_market_value": round(total_market_value, 2),
            "total_unrealized_pnl_amt": round(total_pnl_amt, 2),
            "avg_unrealized_pnl_pct": round(avg_pnl_pct, 4),
        }

        lines = [
            "============================================================",
            "收盘复盘摘要",
            f"目标交易日: {trading_date}",
            f"生成时间  : {_now_sh_str()}",
            f"阶段编号  : {STAGE_NO}",
            f"阶段名称  : {STAGE_NAME}",
            f"执行模式  : NONCORE_FORCE_EXECUTE",
            f"持仓来源  : {position_source}",
            "------------------------------------------------------------",
            f"总标的数      : {metrics['review_count']}",
            f"持仓标的数    : {metrics['position_count']}",
            f"次日观察标的数: {metrics['watchlist_count']}",
            f"收盘浮盈数    : {metrics['positive_count']}",
            f"收盘浮亏数    : {metrics['negative_count']}",
            f"接近止损数    : {metrics['near_stop_count']}",
            f"触发止损数    : {metrics['hit_stop_count']}",
            f"接近止盈数    : {metrics['near_take_profit_count']}",
            f"触发止盈数    : {metrics['hit_take_profit_count']}",
            f"总成交成本    : {metrics['total_cost']:.2f}",
            f"收盘总市值    : {metrics['total_market_value']:.2f}",
            f"总浮盈亏      : {metrics['total_unrealized_pnl_amt']:.2f}",
            f"平均浮盈亏比  : {metrics['avg_unrealized_pnl_pct']:.4f}%",
            "============================================================",
            "",
        ]
        return "\n".join(lines), metrics
