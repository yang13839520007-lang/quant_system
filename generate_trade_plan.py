# -*- coding: utf-8 -*-
"""
Created on Fri Mar 20 14:27:08 2026

@author: DELL
"""

# -*- coding: utf-8 -*-
from __future__ import annotations
import sys
from pathlib import Path
from typing import Dict
import pandas as pd

BASE_DIR = r"C:\quant_system"

def run(trading_date: str, base_dir: str = BASE_DIR) -> Dict:
    print(f"    --> [DEBUG] Stage 02 进入 run() 方法，日期: {trading_date}")
    reports_dir = Path(base_dir) / "reports"
    candidates_path = reports_dir / "daily_candidates_all.csv"
    
    if not candidates_path.exists():
        return {"stage_status": "FAILED", "error": "缺少 daily_candidates_all.csv"}

    df = pd.read_csv(candidates_path)
    if df.empty or "close_price" not in df.columns:
        return {"stage_status": "FAILED", "error": "候选文件为空或缺少 close_price 字段"}

    capital = 1_000_000.0
    base_alloc = capital / max(len(df), 1)
    plans = []

    for _, row in df.iterrows():
        code = row.get("code")
        cp = float(row.get("close_price", 0))
        if pd.isna(cp) or cp <= 0:
            continue

        shares = int((base_alloc // cp) // 100 * 100)
        if shares < 100:
            continue

        stop_loss = round(cp * 0.95, 2)
        tgt_price = round(cp * 1.10, 2)
        
        plans.append({
            "trade_date": trading_date,
            "portfolio_rank": row.get("rank", 99),
            "code": code,
            "action": row.get("action", "正常跟踪"),
            "heat_level": row.get("heat_level", "正常"),
            "score": row.get("score", 85.0),
            "entry_price": cp,
            "stop_loss": stop_loss,
            "target_price": tgt_price,
            "suggested_shares": shares,
            "suggested_position_pct": round((shares * cp) / capital, 4),
            "expected_loss_amt": round(shares * (cp - stop_loss), 2),
            "expected_profit_amt": round(shares * (tgt_price - cp), 2),
            "reason": "主趋势过滤"
        })

    df_plan = pd.DataFrame(plans)
    if df_plan.empty:
        return {"stage_status": "FAILED", "error": "分配资金计算后，没有标的满足最低买入 1 手 (100股) 的约束。"}

    df_plan = df_plan.sort_values("portfolio_rank").reset_index(drop=True)
    df_plan.to_csv(reports_dir / "daily_trade_plan_all.csv", index=False, encoding="utf-8-sig")
    df_plan.head(10).to_csv(reports_dir / "daily_trade_plan_top10.csv", index=False, encoding="utf-8-sig")

    return {"stage_status": "SUCCESS_EXECUTED", "success": True}

def main():
    res = run(trading_date="2026-03-17")
    if res.get("stage_status") == "FAILED":
        print(f"ERROR: {res.get('error')}")
        sys.exit(1)
    sys.exit(0)

if __name__ == "__main__":
    main()