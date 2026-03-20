# -*- coding: utf-8 -*-
"""
Created on Tue Mar 17 18:32:11 2026

@author: DELL
"""

from pathlib import Path
import sys
import pandas as pd

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from config.settings import (
    DATA_DIR,
    REPORT_DIR,
    INITIAL_CASH,
    COMMISSION_RATE,
    STAMP_TAX_RATE,
    LOT_SIZE,
)
from core.loader import load_daily_csv
from factors.indicators import add_moving_averages
from strategies.ma_cross import generate_ma_cross_signals
from backtest.engine import run_single_stock_backtest


MA_PAIRS = [
    (5, 10),
    (10, 20),
    (10, 30),
    (20, 60),
]


def calc_max_drawdown(equity_df: pd.DataFrame) -> float:
    if equity_df.empty or "equity" not in equity_df.columns:
        return 0.0

    s = equity_df["equity"].astype(float)
    rolling_max = s.cummax()
    drawdown = (s / rolling_max - 1.0) * 100
    return round(drawdown.min(), 2)


def run_one_file_one_param(file_path: Path, short_ma: int, long_ma: int):
    df = load_daily_csv(file_path)
    df = add_moving_averages(df, short_ma, long_ma)
    df = generate_ma_cross_signals(df)

    summary, trade_df, equity_df = run_single_stock_backtest(
        df=df,
        initial_cash=INITIAL_CASH,
        commission_rate=COMMISSION_RATE,
        stamp_tax_rate=STAMP_TAX_RATE,
        lot_size=LOT_SIZE,
    )

    max_dd = calc_max_drawdown(equity_df)
    code = df["code"].iloc[0] if not df.empty else file_path.stem
    start_date = df["date"].min() if not df.empty else None
    end_date = df["date"].max() if not df.empty else None

    return {
        "file_name": file_path.name,
        "code": code,
        "start_date": start_date,
        "end_date": end_date,
        "short_ma": short_ma,
        "long_ma": long_ma,
        "initial_cash": summary["initial_cash"],
        "final_equity": summary["final_equity"],
        "total_return_pct": summary["total_return_pct"],
        "trade_count": summary["trade_count"],
        "win_rate_pct": summary["win_rate_pct"],
        "avg_pnl": summary["avg_pnl"],
        "avg_return_pct": summary["avg_return_pct"],
        "max_drawdown_pct": max_dd,
    }


def main():
    csv_files = sorted(DATA_DIR.glob("*.csv"))

    if not csv_files:
        print(f"数据目录里没有CSV文件: {DATA_DIR}")
        return

    detail_results = []
    error_list = []

    total_jobs = len(csv_files) * len(MA_PAIRS)
    finished_jobs = 0

    print(f"开始参数扫描：股票数={len(csv_files)}，参数组数={len(MA_PAIRS)}，总任务数={total_jobs}")

    for file_idx, file_path in enumerate(csv_files, start=1):
        for short_ma, long_ma in MA_PAIRS:
            try:
                row = run_one_file_one_param(file_path, short_ma, long_ma)
                detail_results.append(row)
            except Exception as e:
                error_list.append({
                    "file_name": file_path.name,
                    "short_ma": short_ma,
                    "long_ma": long_ma,
                    "error": str(e)
                })

            finished_jobs += 1
            if finished_jobs % 500 == 0 or finished_jobs == total_jobs:
                print(f"进度: {finished_jobs}/{total_jobs}")

    detail_df = pd.DataFrame(detail_results)
    error_df = pd.DataFrame(error_list)

    # ---------- 参数组汇总 ----------
    if not detail_df.empty:
        pair_summary = (
            detail_df.groupby(["short_ma", "long_ma"], as_index=False)
            .agg(
                stock_count=("code", "count"),
                avg_total_return_pct=("total_return_pct", "mean"),
                median_total_return_pct=("total_return_pct", "median"),
                positive_ratio_pct=("total_return_pct", lambda x: (x > 0).mean() * 100),
                avg_win_rate_pct=("win_rate_pct", "mean"),
                avg_trade_count=("trade_count", "mean"),
                median_trade_count=("trade_count", "median"),
                avg_max_drawdown_pct=("max_drawdown_pct", "mean"),
                median_max_drawdown_pct=("max_drawdown_pct", "median"),
            )
        )

        # 稳健筛选通过率：收益>0、回撤>-25%、交易次数>=15
        robust_df = detail_df.copy()
        robust_df["robust_pass"] = (
            (robust_df["total_return_pct"] > 0) &
            (robust_df["max_drawdown_pct"] > -25) &
            (robust_df["trade_count"] >= 15)
        )

        robust_summary = (
            robust_df.groupby(["short_ma", "long_ma"], as_index=False)
            .agg(
                robust_pass_ratio_pct=("robust_pass", lambda x: x.mean() * 100)
            )
        )

        pair_summary = pair_summary.merge(
            robust_summary,
            on=["short_ma", "long_ma"],
            how="left"
        )

        # ---------- 每只股票最佳参数 ----------
        best_by_stock = (
            detail_df.sort_values(
                by=["total_return_pct", "max_drawdown_pct", "win_rate_pct"],
                ascending=[False, False, False]
            )
            .groupby("code", as_index=False)
            .head(1)
            .reset_index(drop=True)
        )

        # 更稳健版最佳参数：优先过稳健筛选，再比收益
        detail_df["robust_pass"] = (
            (detail_df["total_return_pct"] > 0) &
            (detail_df["max_drawdown_pct"] > -25) &
            (detail_df["trade_count"] >= 15)
        )

        robust_best_by_stock = (
            detail_df.sort_values(
                by=["robust_pass", "total_return_pct", "win_rate_pct"],
                ascending=[False, False, False]
            )
            .groupby("code", as_index=False)
            .head(1)
            .reset_index(drop=True)
        )

    else:
        pair_summary = pd.DataFrame()
        best_by_stock = pd.DataFrame()
        robust_best_by_stock = pd.DataFrame()

    # ---------- 输出 ----------
    detail_out = REPORT_DIR / "param_sweep_detail.csv"
    pair_out = REPORT_DIR / "param_sweep_pair_summary.csv"
    best_out = REPORT_DIR / "param_sweep_best_by_stock.csv"
    robust_best_out = REPORT_DIR / "param_sweep_robust_best_by_stock.csv"
    error_out = REPORT_DIR / "param_sweep_errors.csv"

    detail_df.to_csv(detail_out, index=False, encoding="utf-8-sig")
    pair_summary.to_csv(pair_out, index=False, encoding="utf-8-sig")
    best_by_stock.to_csv(best_out, index=False, encoding="utf-8-sig")
    robust_best_by_stock.to_csv(robust_best_out, index=False, encoding="utf-8-sig")
    error_df.to_csv(error_out, index=False, encoding="utf-8-sig")

    print("=" * 60)
    print("参数扫描完成")
    print(f"成功任务数: {len(detail_df)}")
    print(f"失败任务数: {len(error_df)}")
    print(f"详细结果: {detail_out}")
    print(f"参数汇总: {pair_out}")
    print(f"单股最佳参数: {best_out}")
    print(f"单股稳健最佳参数: {robust_best_out}")
    print(f"错误文件: {error_out}")
    print("=" * 60)

    if not pair_summary.empty:
        print("参数组汇总：")
        print(
            pair_summary.sort_values(
                by=["robust_pass_ratio_pct", "median_total_return_pct"],
                ascending=[False, False]
            ).to_string(index=False)
        )


if __name__ == "__main__":
    main()