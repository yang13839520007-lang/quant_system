# -*- coding: utf-8 -*-
"""
Created on Wed Mar 18 17:57:18 2026

@author: DELL
"""

# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, Optional

import numpy as np
import pandas as pd


BASE_DIR = r"C:\quant_system"


def _load_optional_settings():
    try:
        from config import settings  # type: ignore
        return settings
    except Exception:
        return None


def _get_setting(name: str, default):
    settings = _load_optional_settings()
    if settings is not None and hasattr(settings, name):
        return getattr(settings, name)
    return default


def _pick_col(df: pd.DataFrame, candidates):
    lower_map = {str(c).strip().lower(): c for c in df.columns}
    for c in candidates:
        if c.lower() in lower_map:
            return lower_map[c.lower()]
    return None


def _safe_float_series(df: pd.DataFrame, col: Optional[str], default: float = 0.0) -> pd.Series:
    if not col or col not in df.columns:
        return pd.Series([default] * len(df), index=df.index, dtype=float)
    return pd.to_numeric(df[col], errors="coerce").fillna(default).astype(float)


def _safe_rank_series(df: pd.DataFrame, col: Optional[str], out_name: str) -> pd.Series:
    default_rank = pd.Series(range(1, len(df) + 1), index=df.index, dtype=int)
    if not col or col not in df.columns:
        return default_rank

    rank = pd.to_numeric(df[col], errors="coerce")
    rank = rank.where(rank.notna(), default_rank)
    return rank.astype(int)


class PortfolioRiskReviewGenerator:
    def __init__(self, base_dir: str = BASE_DIR) -> None:
        self.base_dir = Path(base_dir)
        self.reports_dir = self.base_dir / "reports"
        self.reports_dir.mkdir(parents=True, exist_ok=True)

        self.capital = float(_get_setting("PORTFOLIO_CAPITAL", 1_000_000.0))
        self.max_single_position_pct = float(_get_setting("MAX_SINGLE_POSITION_PCT", 0.10))
        self.max_total_position_pct = float(_get_setting("MAX_TOTAL_POSITION_PCT", 0.40))
        self.max_total_expected_loss_pct = float(_get_setting("MAX_TOTAL_EXPECTED_LOSS_PCT", 0.05))
        self.board_lot = int(_get_setting("A_SHARE_BOARD_LOT", 100))

    def run(self, trading_date: str) -> Dict:
        print("============================================================")
        print("组合风控复核开始")
        print(f"目标交易日  : {trading_date}")
        print(f"组合计划文件: {self.reports_dir / 'daily_portfolio_plan.csv'}")
        print(f"输出目录    : {self.reports_dir}")
        print("入口类型    : function")
        print("调用入口    : generate_portfolio_risk_review.main")
        print("============================================================")

        source_path = self.reports_dir / "daily_portfolio_plan.csv"
        if not source_path.exists():
            raise FileNotFoundError("缺少 reports/daily_portfolio_plan.csv，无法进行组合风控复核。")

        df = pd.read_csv(source_path)
        reviewed_df = self._review(df, trading_date=trading_date)

        out_csv = self.reports_dir / "daily_portfolio_plan_risk_checked.csv"
        out_txt = self.reports_dir / "daily_portfolio_summary_risk_checked.txt"

        reviewed_df.to_csv(out_csv, index=False, encoding="utf-8-sig")
        self._write_summary(out_txt, trading_date=trading_date, reviewed_df=reviewed_df)

        print("============================================================")
        print("组合风控复核完成")
        print(f"目标交易日: {trading_date}")
        print(f"复核后标的数: {len(reviewed_df)}")
        print(f"风控复核文件: {out_csv}")
        print(f"摘要文件    : {out_txt}")
        print("============================================================")
        print(reviewed_df.head(10).to_string(index=False))

        return {
            "stage_status": "SUCCESS_EXECUTED",
            "success": True,
            "trading_date": trading_date,
            "review_count": int(len(reviewed_df)),
            "output_csv_path": str(out_csv),
            "output_summary_path": str(out_txt),
        }

    def _review(self, df: pd.DataFrame, trading_date: str) -> pd.DataFrame:
        code_col = _pick_col(df, ["code"])
        if not code_col:
            raise ValueError("daily_portfolio_plan.csv 缺少 code 字段。")

        name_col = _pick_col(df, ["name"])
        rank_col = _pick_col(df, ["portfolio_rank", "rank"])
        score_col = _pick_col(df, ["score"])
        entry_col = _pick_col(df, ["entry_price"])
        shares_col = _pick_col(df, ["suggested_shares", "planned_shares", "shares"])
        pos_col = _pick_col(df, ["suggested_position_pct", "position_pct", "target_position_pct"])
        stop_col = _pick_col(df, ["stop_loss"])
        target_col = _pick_col(df, ["target_price"])
        exp_loss_col = _pick_col(df, ["expected_loss_amt"])
        exp_profit_col = _pick_col(df, ["expected_profit_amt"])

        work = pd.DataFrame(index=df.index)
        work["trading_date"] = trading_date
        work["portfolio_rank"] = _safe_rank_series(df, rank_col, "portfolio_rank")
        work["code"] = df[code_col].astype(str).str.strip()
        work["name"] = df[name_col].astype(str).str.strip() if name_col else ""
        work["score"] = _safe_float_series(df, score_col, default=0.0)
        work["entry_price"] = _safe_float_series(df, entry_col, default=np.nan)
        work["stop_loss"] = _safe_float_series(df, stop_col, default=np.nan)
        work["target_price"] = _safe_float_series(df, target_col, default=np.nan)
        work["suggested_shares"] = _safe_float_series(df, shares_col, default=0.0)
        work["suggested_position_pct"] = _safe_float_series(df, pos_col, default=np.nan)
        work["expected_loss_amt"] = _safe_float_series(df, exp_loss_col, default=np.nan)
        work["expected_profit_amt"] = _safe_float_series(df, exp_profit_col, default=np.nan)

        work["suggested_shares"] = work["suggested_shares"].fillna(0).astype(int)

        missing_pos_mask = work["suggested_position_pct"].isna() | (work["suggested_position_pct"] <= 0)
        work.loc[missing_pos_mask, "suggested_position_pct"] = (
            work.loc[missing_pos_mask, "suggested_shares"] * work.loc[missing_pos_mask, "entry_price"] / self.capital
        )

        work["risk_review_note"] = ""

        over_single_mask = work["suggested_position_pct"] > self.max_single_position_pct
        if bool(over_single_mask.any()):
            scale = self.max_single_position_pct / work.loc[over_single_mask, "suggested_position_pct"]
            work.loc[over_single_mask, "suggested_position_pct"] = self.max_single_position_pct
            work.loc[over_single_mask, "suggested_shares"] = (
                (work.loc[over_single_mask, "suggested_shares"] * scale).fillna(0).astype(int) // self.board_lot * self.board_lot
            )
            work.loc[over_single_mask, "risk_review_note"] += "单票仓位截断;"

        total_position_pct = float(work["suggested_position_pct"].sum())
        if total_position_pct > self.max_total_position_pct and total_position_pct > 0:
            scale = self.max_total_position_pct / total_position_pct
            work["suggested_position_pct"] = work["suggested_position_pct"] * scale
            work["suggested_shares"] = (
                (work["suggested_shares"] * scale).fillna(0).astype(int) // self.board_lot * self.board_lot
            )
            work["risk_review_note"] += "总仓位同比例收缩;"

        risk_per_share = (work["entry_price"] - work["stop_loss"]).clip(lower=0.0)
        profit_per_share = (work["target_price"] - work["entry_price"]).clip(lower=0.0)
        work["expected_loss_amt"] = (work["suggested_shares"] * risk_per_share).round(2)
        work["expected_profit_amt"] = (work["suggested_shares"] * profit_per_share).round(2)

        total_expected_loss = float(work["expected_loss_amt"].sum())
        max_total_expected_loss_amt = self.capital * self.max_total_expected_loss_pct
        if total_expected_loss > max_total_expected_loss_amt and total_expected_loss > 0:
            scale = max_total_expected_loss_amt / total_expected_loss
            work["suggested_position_pct"] = work["suggested_position_pct"] * scale
            work["suggested_shares"] = (
                (work["suggested_shares"] * scale).fillna(0).astype(int) // self.board_lot * self.board_lot
            )
            work["expected_loss_amt"] = (work["suggested_shares"] * risk_per_share).round(2)
            work["expected_profit_amt"] = (work["suggested_shares"] * profit_per_share).round(2)
            work["risk_review_note"] += "总风险同比例收缩;"

        work["risk_review_passed"] = True
        work["risk_review_time"] = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")

        ordered_cols = [
            "trading_date", "portfolio_rank", "code", "name", "score",
            "entry_price", "stop_loss", "target_price",
            "suggested_shares", "suggested_position_pct",
            "expected_loss_amt", "expected_profit_amt",
            "risk_review_passed", "risk_review_note", "risk_review_time",
        ]
        return work[[c for c in ordered_cols if c in work.columns]].sort_values(
            by=["portfolio_rank", "score"],
            ascending=[True, False],
        ).reset_index(drop=True)

    def _write_summary(self, path: Path, trading_date: str, reviewed_df: pd.DataFrame) -> None:
        total_position = float(reviewed_df["suggested_position_pct"].sum()) if not reviewed_df.empty else 0.0
        total_loss = float(reviewed_df["expected_loss_amt"].sum()) if not reviewed_df.empty else 0.0
        total_profit = float(reviewed_df["expected_profit_amt"].sum()) if not reviewed_df.empty else 0.0

        lines = [
            "============================================================",
            "组合风控复核摘要",
            f"目标交易日: {trading_date}",
            f"标的数: {len(reviewed_df)}",
            f"复核后总仓位: {total_position:.4f}",
            f"复核后总预期风险: {total_loss:.2f}",
            f"复核后总预期收益: {total_profit:.2f}",
            f"单票仓位上限: {self.max_single_position_pct:.4f}",
            f"总仓位上限: {self.max_total_position_pct:.4f}",
            f"总风险上限: {self.max_total_expected_loss_pct:.4f}",
            "============================================================",
        ]
        path.write_text("\n".join(lines), encoding="utf-8")


def run(trading_date: str, base_dir: str = BASE_DIR) -> Dict:
    generator = PortfolioRiskReviewGenerator(base_dir=base_dir)
    return generator.run(trading_date=trading_date)


def main(trading_date: Optional[str] = None, base_dir: str = BASE_DIR) -> Dict:
    if trading_date is not None:
        return run(trading_date=trading_date, base_dir=base_dir)

    parser = argparse.ArgumentParser(description="组合风控复核")
    parser.add_argument("--trading-date", dest="trading_date", default="2026-03-17")
    parser.add_argument("--base-dir", dest="base_dir", default=BASE_DIR)
    args, _ = parser.parse_known_args()
    return run(trading_date=args.trading_date, base_dir=args.base_dir)


if __name__ == "__main__":
    result = main()
    print(json.dumps(result, ensure_ascii=False, indent=2))