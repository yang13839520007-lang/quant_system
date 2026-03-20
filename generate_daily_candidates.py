# -*- coding: utf-8 -*-
"""
Created on Fri Mar 20 14:26:44 2026

@author: DELL
"""

# -*- coding: utf-8 -*-
from __future__ import annotations
import sys
from pathlib import Path
from typing import Dict
import pandas as pd

BASE_DIR = r"C:\quant_system"

try:
    import generate_market_signal_snapshot
except ImportError:
    generate_market_signal_snapshot = None

def run(trading_date: str, base_dir: str = BASE_DIR) -> Dict:
    print(f"    --> [DEBUG] Stage 01 进入 run() 方法，日期: {trading_date}")
    reports_dir = Path(base_dir) / "reports"
    
    if generate_market_signal_snapshot and hasattr(generate_market_signal_snapshot, "run"):
        print("    --> [DEBUG] 正在拉起快照补数...")
        generate_market_signal_snapshot.run(trading_date=trading_date, base_dir=base_dir)

    backtest_path = reports_dir / "batch_backtest_summary.csv"
    snapshot_path = reports_dir / "market_signal_snapshot.csv"

    if not backtest_path.exists() or not snapshot_path.exists():
        return {"stage_status": "FAILED", "error": f"缺少输入文件。底池:{backtest_path.exists()} 快照:{snapshot_path.exists()}"}

    df_backtest = pd.read_csv(backtest_path)
    df_snapshot = pd.read_csv(snapshot_path)
    print(f"    --> [DEBUG] 成功加载回测摘要: {len(df_backtest)} 行")
    print(f"    --> [DEBUG] 成功加载行情快照: {len(df_snapshot)} 行")

    df = pd.merge(df_backtest, df_snapshot, on="code", how="inner")
    print(f"    --> [DEBUG] Inner Merge 匹配到的股票: {len(df)} 行")
    
    if df.empty:
         return {"stage_status": "FAILED", "error": "快照与底池合并后，匹配结果为 0 行，请检查 code 字段格式是否一致(例如 sh.600000)"}

    req_cols = ["close_price", "ma10", "ma20", "turnover_amount"]
    missing = [c for c in req_cols if c not in df.columns]
    if missing:
        return {"stage_status": "FAILED", "error": f"合并后缺失行情字段: {missing}"}

    df_valid = df.dropna(subset=req_cols).copy()
    print(f"    --> [DEBUG] 去除 NaN 行情后剩余: {len(df_valid)} 行")
    if df_valid.empty:
        return {"stage_status": "FAILED", "error": "去除行情 NaN 缺失值后，有效候选为 0。"}

    trend_pass = (df_valid["ma10"] > df_valid["ma20"]) & (df_valid["close_price"] > df_valid["ma20"])
    df_trend = df_valid[trend_pass].copy()
    print(f"    --> [DEBUG] 通过 10/20 主趋势过滤剩余: {len(df_trend)} 行")

    liquidity_pass = df_trend["turnover_amount"] >= 5e7
    df_candidates = df_trend[liquidity_pass].copy()
    print(f"    --> [DEBUG] 通过 5000万 成交额过滤剩余: {len(df_candidates)} 行")

    if df_candidates.empty:
        return {"stage_status": "FAILED", "error": "所有标的均未通过 10/20 趋势或流动性过滤。"}

    if "total_return_pct" in df_candidates.columns:
        df_candidates = df_candidates.sort_values(by="total_return_pct", ascending=False)
    else:
        df_candidates = df_candidates.sort_values(by="turnover_amount", ascending=False)

    df_candidates["rank"] = range(1, len(df_candidates) + 1)
    df_candidates["score"] = 100.0 - df_candidates["rank"]
    df_candidates["heat_level"] = "正常"
    df_candidates["action"] = "正常跟踪"

    df_candidates.to_csv(reports_dir / "daily_candidates_all.csv", index=False, encoding="utf-8-sig")
    df_candidates.head(20).to_csv(reports_dir / "daily_candidates_top20.csv", index=False, encoding="utf-8-sig")

    return {"stage_status": "SUCCESS_EXECUTED", "success": True}

def main():
    res = run(trading_date="2026-03-17")
    if res.get("stage_status") == "FAILED":
        print(f"ERROR: {res.get('error')}")
        sys.exit(1)
    sys.exit(0)

if __name__ == "__main__":
    main()