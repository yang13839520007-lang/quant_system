# -*- coding: utf-8 -*-
"""
Created on Fri Mar 20 14:41:27 2026

@author: DELL
"""

# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Dict

import pandas as pd

from core.position_manager import PositionManager


BASE_DIR = r"C:\quant_system"


def parse_args():
    parser = argparse.ArgumentParser(description="次日持仓续管层")
    parser.add_argument("--trading-date", dest="trading_date", required=True)
    parser.add_argument("--base-dir", dest="base_dir", default=BASE_DIR)
    # 使用 parse_known_args 防止主控透传的多余参数导致崩溃
    return parser.parse_known_args()[0]


class NextDayManagementGenerator:
    """
    Stage 09: 次日持仓续管层
    基于 Stage 08 输出的 daily_close_positions.csv，
    执行 T+1 状态流转（将今日新买入冻结转为次日可用），
    并基于持仓时间/浮盈亏预判次日操作（如 T+2 强制止损或满足目标止盈）。
    """
    def __init__(self, base_dir: str = BASE_DIR):
        self.base_dir = Path(base_dir)
        self.reports_dir = self.base_dir / "reports"
        self.reports_dir.mkdir(parents=True, exist_ok=True)
        
        # 简单持仓管理风控参数
        self.take_profit_pct = 0.08   # 默认 8% 止盈
        self.stop_loss_pct = -0.05    # 默认 -5% 止损
        self.max_hold_days = 2        # 默认持仓不超过 2 个交易日 (短线T+1/T+2)

    def run(self, trading_date: str) -> Dict:
        print("============================================================")
        print(f"[*] 执行 Stage 09: 次日持仓续管层 | 目标日期: {trading_date}")
        
        pos_path = self.reports_dir / "daily_close_positions.csv"
        if not pos_path.exists():
            return {"stage_status": "FAILED", "error": f"缺少 {pos_path.name}，无法生成次日续管计划。"}

        df_pos = pd.read_csv(pos_path)
        if df_pos.empty:
            # 如果没有持仓，直接生成空表并标记执行成功
            self._write_empty_outputs()
            print("[*] 当前无持仓，生成空次日续管计划。")
            return {"stage_status": "SUCCESS_EXECUTED", "success": True, "holdings": 0}

        plans = []
        for _, row in df_pos.iterrows():
            code = row.get("code")
            if not code or pd.isna(code):
                continue
                
            # 兼容处理字段
            qty = float(row.get("filled_shares", 0.0))
            if qty <= 0:
                continue
                
            cost = float(row.get("avg_fill_price", 0.0))
            close_price = float(row.get("close_price", cost))
            pnl_pct = float(row.get("unrealized_pnl_pct", 0.0))
            
            # T+1 流转：假设前序文件缺少这些详细字段，这里做简单适配
            # 默认将今日复盘的持仓直接视为次日可用 (实盘中需结合真实流水与清算)
            hold_days = int(row.get("hold_days", 1)) 
            
            # 判断次日操作
            management_action = "正常持有"
            action_reason = ""
            priority_score = 50.0
            
            if pnl_pct >= self.take_profit_pct:
                management_action = "止盈卖出"
                action_reason = f"浮盈 {pnl_pct*100:.2f}% 达标"
                priority_score = 90.0
            elif pnl_pct <= self.stop_loss_pct:
                management_action = "止损卖出"
                action_reason = f"浮亏 {pnl_pct*100:.2f}% 触损"
                priority_score = 80.0
            elif hold_days > self.max_hold_days:
                management_action = "时间止损卖出"
                action_reason = f"持仓已达 {hold_days} 天"
                priority_score = 75.0
            elif pnl_pct < 0:
                management_action = "弱转强观察"
                priority_score = 60.0
                
            plans.append({
                "trading_date": trading_date,
                "code": code,
                "hold_qty": qty,
                "available_qty": qty, # T+1 直接转可用
                "cost_price": cost,
                "close_price": close_price,
                "unrealized_pnl_pct": round(pnl_pct, 4),
                "hold_days": hold_days,
                "management_action": management_action,
                "management_priority_score": priority_score,
                "action_reason": action_reason
            })

        df_plan = pd.DataFrame(plans)
        
        if df_plan.empty:
            self._write_empty_outputs()
            print("[*] 有效持仓处理后为0，生成空次日续管计划。")
            return {"stage_status": "SUCCESS_EXECUTED", "success": True, "holdings": 0}

        # 按优先级排序
        df_plan = df_plan.sort_values(by="management_priority_score", ascending=False).reset_index(drop=True)
        
        out_file = self.reports_dir / "daily_next_day_management.csv"
        df_plan.to_csv(out_file, index=False, encoding="utf-8-sig")

        # 简单摘要输出
        summary_path = self.reports_dir / "daily_next_day_management_summary.txt"
        summary_text = (
            "============================================================\n"
            f"次日持仓续管生成完成 | 目标交易日: {trading_date}\n"
            f"待处理持仓数: {len(df_plan)}\n"
            f"建议卖出数  : {len(df_plan[df_plan['management_action'].str.contains('卖出')])}\n"
            "============================================================"
        )
        summary_path.write_text(summary_text, encoding="utf-8")

        print(f"[*] 次日续管计划生成成功: 涉及 {len(df_plan)} 只持仓。")
        print("============================================================")
        
        return {
            "stage_status": "SUCCESS_EXECUTED",
            "success": True,
            "trading_date": trading_date,
            "holdings": len(df_plan),
        }

    def _write_empty_outputs(self):
        empty_df = pd.DataFrame(columns=[
            "trading_date", "code", "hold_qty", "available_qty", 
            "cost_price", "close_price", "unrealized_pnl_pct", "hold_days",
            "management_action", "management_priority_score", "action_reason"
        ])
        empty_df.to_csv(self.reports_dir / "daily_next_day_management.csv", index=False, encoding="utf-8-sig")
        
        summary_text = "============================================================\n次日持仓续管生成完成\n当前无持仓记录。\n============================================================"
        (self.reports_dir / "daily_next_day_management_summary.txt").write_text(summary_text, encoding="utf-8")


def run(trading_date: str, base_dir: str = BASE_DIR) -> Dict:
    gen = NextDayManagementGenerator(base_dir=base_dir)
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