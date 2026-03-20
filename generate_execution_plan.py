# -*- coding: utf-8 -*-
"""
Created on Fri Mar 20 14:33:10 2026

@author: DELL
"""

# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Dict, Optional

import pandas as pd


BASE_DIR = r"C:\quant_system"


def parse_args():
    parser = argparse.ArgumentParser(description="组合执行优先级层")
    parser.add_argument("--trading-date", dest="trading_date", required=True)
    parser.add_argument("--base-dir", dest="base_dir", default=BASE_DIR)
    # 使用 parse_known_args 防止主控透传的多余参数导致崩溃
    return parser.parse_known_args()[0]


class ExecutionPlanGenerator:
    """
    Stage 05: 组合执行优先级层
    基于风控后的组合计划 (daily_portfolio_plan_risk_checked.csv)，
    分配实际执行资金，划定核心/候补梯队，并计算买入区间。
    """
    def __init__(self, base_dir: str = BASE_DIR):
        self.base_dir = Path(base_dir)
        self.reports_dir = self.base_dir / "reports"
        
        # 业务参数
        self.capital = 1_000_000.0
        self.core_tier_count = 3  # 前3只算核心执行
        
        # 价格滑点容忍度
        self.buy_zone_down_pct = -0.015  # 允许低吸 -1.5%
        self.buy_zone_up_pct = 0.015     # 允许追高 +1.5%
        self.chase_limit_pct = 0.03      # 极限追高 +3.0%
        self.skip_open_lt_pct = -0.106   # 开盘跌停(或过低)直接放弃 (-10.6% 容错)

    def run(self, trading_date: str) -> Dict:
        print("============================================================")
        print(f"[*] 执行 Stage 05: 组合执行优先级层 | 目标日期: {trading_date}")
        
        source_path = self.reports_dir / "daily_portfolio_plan_risk_checked.csv"
        if not source_path.exists():
            return {"stage_status": "FAILED", "error": "缺少 daily_portfolio_plan_risk_checked.csv，无法生成执行计划。"}

        df = pd.read_csv(source_path)
        if df.empty:
            return {"stage_status": "FAILED", "error": "风控后的组合计划为空，无票可执行。"}

        # 确保关键字段存在
        req_cols = ["code", "entry_price", "suggested_shares"]
        missing = [c for c in req_cols if c not in df.columns]
        if missing:
            return {"stage_status": "FAILED", "error": f"输入文件缺少关键字段: {missing}"}

        plans = []
        execution_rank = 1
        
        # 按原有排序遍历生成执行策略
        for _, row in df.iterrows():
            code = row["code"]
            entry_price = float(row.get("entry_price", 0))
            shares = int(row.get("suggested_shares", 0))
            
            if pd.isna(entry_price) or entry_price <= 0 or shares <= 0:
                continue

            tier = "核心执行" if execution_rank <= self.core_tier_count else "候补执行"
            planned_capital = round(shares * entry_price, 2)
            
            # 计算执行区间
            buy_zone_low = round(entry_price * (1 + self.buy_zone_down_pct), 3)
            buy_zone_high = round(entry_price * (1 + self.buy_zone_up_pct), 3)
            chase_limit = round(entry_price * (1 + self.chase_limit_pct), 3)
            skip_price = round(entry_price * (1 + self.skip_open_lt_pct), 3)
            
            # 生成执行优先级分 (核心分数高)
            base_score = float(row.get("score", 50.0)) if pd.notna(row.get("score")) else 50.0
            tier_bonus = 50.0 if tier == "核心执行" else 0.0
            priority_score = round(base_score + tier_bonus, 2)

            plans.append({
                "trading_date": trading_date,
                "execution_rank": execution_rank,
                "execution_tier": tier,
                "portfolio_rank": row.get("portfolio_rank", execution_rank),
                "code": code,
                "name": row.get("name", ""),
                "action": "正常买入",
                "heat_level": row.get("heat_level", "正常"),
                "entry_price": entry_price,
                "buy_zone_low": buy_zone_low,
                "buy_zone_high": buy_zone_high,
                "chase_limit_price": chase_limit,
                "skip_if_open_lt": skip_price,
                "review_planned_shares": shares,  # 重要: 为后续 13 段对账保留计划股数
                "planned_capital": planned_capital,
                "planned_position_pct": round(planned_capital / self.capital, 4),
                "capital_priority_score": priority_score,
                "keep_flag": 1,
                "drop_reason": ""
            })
            execution_rank += 1

        df_plan = pd.DataFrame(plans)
        if df_plan.empty:
            return {"stage_status": "FAILED", "error": "有效标的全部剔除，生成执行计划失败。"}

        df_plan = df_plan.sort_values("execution_rank").reset_index(drop=True)
        
        all_path = self.reports_dir / "daily_execution_plan.csv"
        keep_path = self.reports_dir / "daily_execution_plan_keep.csv"
        
        df_plan.to_csv(all_path, index=False, encoding="utf-8-sig")
        df_plan[df_plan["keep_flag"] == 1].to_csv(keep_path, index=False, encoding="utf-8-sig")

        print(f"[*] 执行计划生成成功: 保留执行 {len(df_plan)} 只标的。")
        print("============================================================")
        
        return {
            "stage_status": "SUCCESS_EXECUTED",
            "success": True,
            "trading_date": trading_date,
            "execution_count": len(df_plan),
        }

def run(trading_date: str, base_dir: str = BASE_DIR) -> Dict:
    gen = ExecutionPlanGenerator(base_dir=base_dir)
    return gen.run(trading_date=trading_date)

def main():
    args = parse_args()
    res = run(trading_date=args.trading_date, base_dir=args.base_dir)
    if res.get("stage_status") == "FAILED":
        print(f"ERROR: {res.get('error')}")
        sys.exit(1)
    sys.exit(0)

if __name__ == "__main__":
    main()