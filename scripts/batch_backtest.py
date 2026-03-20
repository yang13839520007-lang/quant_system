# -*- coding: utf-8 -*-
"""
Created on Tue Mar 17 16:07:35 2026

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
    SHORT_MA,
    LONG_MA,
)
from core.loader import load_daily_csv
from factors.indicators import add_moving_averages
from strategies.ma_cross import generate_ma_cross_signals
from backtest.engine import run_single_stock_backtest


def calc_max_drawdown(equity_df: pd.DataFrame) -> float:
    if equity_df.empty or "equity" not in equity_df.columns:
        return 0.0

    s = equity_df["equity"].astype(float)
    rolling_max = s.cummax()
    drawdown = (s / rolling_max - 1.0) * 100
    return round(drawdown.min(), 2)


def main():
    csv_files = sorted(DATA_DIR.glob("*.csv"))

    if not csv_files:
        print(f"数据目录里没有CSV文件: {DATA_DIR}")
        return

    results = []
    error_list = []

    total_files = len(csv_files)
    print(f"开始批量回测，共 {total_files} 个文件")

    for idx, file_path in enumerate(csv_files, start=1):
        file_name = file_path.name

        try:
            df = load_daily_csv(file_path)
            df = add_moving_averages(df, SHORT_MA, LONG_MA)
            df = generate_ma_cross_signals(df)

            summary, trade_df, equity_df = run_single_stock_backtest(
                df=df,
                initial_cash=INITIAL_CASH,
                commission_rate=COMMISSION_RATE,
                stamp_tax_rate=STAMP_TAX_RATE,
                lot_size=LOT_SIZE,
            )

            max_dd = calc_max_drawdown(equity_df)

            code = df["code"].iloc[0] if not df.empty and "code" in df.columns else file_name
            start_date = df["date"].min() if not df.empty else None
            end_date = df["date"].max() if not df.empty else None

            results.append({
                "file_name": file_name,
                "code": code,
                "start_date": start_date,
                "end_date": end_date,
                "initial_cash": summary["initial_cash"],
                "final_equity": summary["final_equity"],
                "total_return_pct": summary["total_return_pct"],
                "trade_count": summary["trade_count"],
                "win_rate_pct": summary["win_rate_pct"],
                "avg_pnl": summary["avg_pnl"],
                "avg_return_pct": summary["avg_return_pct"],
                "max_drawdown_pct": max_dd,
            })

            if idx % 50 == 0 or idx == total_files:
                print(f"进度: {idx}/{total_files}")

        except Exception as e:
            error_list.append({
                "file_name": file_name,
                "error": str(e)
            })

    result_df = pd.DataFrame(results)
    error_df = pd.DataFrame(error_list)

    if not result_df.empty:
        result_df = result_df.sort_values(
            by=["total_return_pct", "win_rate_pct"],
            ascending=[False, False]
        ).reset_index(drop=True)

    summary_out = REPORT_DIR / "batch_backtest_summary.csv"
    error_out = REPORT_DIR / "batch_backtest_errors.csv"

    result_df.to_csv(summary_out, index=False, encoding="utf-8-sig")
    error_df.to_csv(error_out, index=False, encoding="utf-8-sig")

    print("=" * 60)
    print("批量回测完成")
    print(f"成功: {len(result_df)}")
    print(f"失败: {len(error_df)}")
    print(f"结果文件: {summary_out}")
    print(f"错误文件: {error_out}")
    print("=" * 60)

    if not result_df.empty:
        print("收益率前10名：")
        print(result_df.head(10).to_string(index=False))
    else:
        print("没有成功回测的结果")


if __name__ == "__main__":
    main()