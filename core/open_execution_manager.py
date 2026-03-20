# -*- coding: utf-8 -*-
"""
Created on Fri Mar 20 14:35:10 2026

@author: DELL
"""

# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd


class OpenExecutionManager:
    """
    Stage 06: 开盘动态执行层
    接收 Stage 05 (执行优先级层) 的执行计划与当日快照，
    基于开盘价动态决策：是正常买入、放弃买入，还是触发保护。
    """

    def __init__(self, base_dir: str = r"C:\quant_system") -> None:
        self.base_dir = Path(base_dir)
        self.reports_dir = self.base_dir / "reports"
        self.reports_dir.mkdir(parents=True, exist_ok=True)
        self.usable_capital = 1_000_000.0

    def run(self, trading_date: str) -> Dict:
        print("============================================================")
        print("开盘动态执行开始")
        print(f"目标交易日  : {trading_date}")
        print(f"执行计划文件: {self.reports_dir / 'daily_execution_plan.csv'}")
        print(f"快照文件    : {self.reports_dir / 'market_signal_snapshot.csv'}")
        print(f"输出目录    : {self.reports_dir}")
        print("入口类型    : function")
        print("调用入口    : core.open_execution_manager.main")
        print("============================================================")

        plan_path = self.reports_dir / "daily_execution_plan.csv"
        snapshot_path = self.reports_dir / "market_signal_snapshot.csv"

        if not plan_path.exists():
            raise FileNotFoundError("缺少执行计划文件 daily_execution_plan.csv")
        if not snapshot_path.exists():
            raise FileNotFoundError("缺少快照文件 market_signal_snapshot.csv")

        # 使用多编码适配
        try:
            df_plan = pd.read_csv(plan_path, encoding="utf-8-sig")
        except UnicodeDecodeError:
            df_plan = pd.read_csv(plan_path, encoding="gbk")

        try:
            df_snapshot = pd.read_csv(snapshot_path, encoding="utf-8-sig")
        except UnicodeDecodeError:
            df_snapshot = pd.read_csv(snapshot_path, encoding="gbk")

        if df_plan.empty:
            raise ValueError("执行计划文件为空，无票可执行")
            
        df_merged = pd.merge(df_plan, df_snapshot, on="code", how="left")
        
        # 定义可能的计划股数字段名
        shares_col = self._pick_col(df_merged, [
            "review_planned_shares", "suggested_shares", "planned_shares", 
            "order_shares", "shares", "target_shares", "order_qty"
        ])
        
        if not shares_col:
            raise ValueError(f"缺少必要字段，候选别名: ['review_planned_shares', 'suggested_shares', 'planned_shares', 'order_shares', 'shares']；当前字段: {list(df_merged.columns)}")
            
        entry_price_col = self._pick_col(df_merged, ["entry_price"])
        open_price_col = self._pick_col(df_merged, ["close_price", "open_price", "latest_price", "open", "price"])
        skip_lt_col = self._pick_col(df_merged, ["skip_if_open_lt"])
        chase_limit_col = self._pick_col(df_merged, ["chase_limit_price"])

        decisions = []
        orders = []
        actual_used_capital = 0.0
        
        # 按照优先级排序
        if "execution_rank" in df_merged.columns:
            df_merged = df_merged.sort_values("execution_rank").reset_index(drop=True)

        for _, row in df_merged.iterrows():
            code = row["code"]
            plan_shares = int(row.get(shares_col, 0))
            entry_price = float(row.get(entry_price_col, 0.0)) if entry_price_col else 0.0
            # 模拟开盘价（如果缺失，用昨收/参考价代替）
            open_price = float(row.get(open_price_col, entry_price)) if open_price_col else entry_price
            
            skip_lt = float(row.get(skip_lt_col, 0.0)) if skip_lt_col else 0.0
            chase_limit = float(row.get(chase_limit_col, 99999.0)) if chase_limit_col else 99999.0

            final_action = "正常买入"
            final_shares = plan_shares
            drop_reason = ""
            keep_flag = 1
            
            if pd.isna(open_price) or open_price <= 0:
                final_action = "无行情放弃"
                final_shares = 0
                drop_reason = "无法获取有效开盘价"
                keep_flag = 0
            elif open_price < skip_lt:
                final_action = "低开放弃"
                final_shares = 0
                drop_reason = f"开盘价 {open_price} 低于底线 {skip_lt}"
                keep_flag = 0
            elif open_price > chase_limit:
                final_action = "高开放弃"
                final_shares = 0
                drop_reason = f"开盘价 {open_price} 高于追高上限 {chase_limit}"
                keep_flag = 0
            elif open_price < entry_price:
                final_action = "低吸买入"
            elif open_price > entry_price:
                final_action = "谨慎追价"

            # 资金校验拦截
            cost = final_shares * open_price
            if cost > 0 and (actual_used_capital + cost) > self.usable_capital:
                final_action = "资金不足放弃"
                final_shares = 0
                drop_reason = "剩余资金不足以覆盖该笔订单"
                keep_flag = 0
                cost = 0.0
                
            actual_used_capital += cost
            
            decision = {
                "execution_rank": row.get("execution_rank", 99),
                "code": code,
                "open_price": round(open_price, 2),
                "entry_price": round(entry_price, 2),
                "final_action": final_action,
                "order_ratio": 1.0 if keep_flag else 0.0,
                "final_order_shares": final_shares,
                "final_order_price": round(open_price, 2),
                "final_order_capital": round(cost, 2),
                "final_keep_flag": keep_flag,
                "final_drop_reason": drop_reason
            }
            decisions.append(decision)
            
            if keep_flag:
                orders.append({
                    "trading_date": trading_date,
                    "code": code,
                    "action": "BUY",
                    "order_shares": final_shares,
                    "order_price": round(open_price, 2),
                    "order_type": "LIMIT",
                    "status": "PENDING"
                })

        df_decision = pd.DataFrame(decisions)
        df_orders = pd.DataFrame(orders)
        
        decision_path = self.reports_dir / "daily_open_execution_decision.csv"
        order_path = self.reports_dir / "daily_open_execution_orders.csv"
        summary_path = self.reports_dir / "daily_open_execution_summary.txt"

        df_decision.to_csv(decision_path, index=False, encoding="utf-8-sig")
        df_orders.to_csv(order_path, index=False, encoding="utf-8-sig")
        
        self._write_summary(summary_path, trading_date, df_decision, actual_used_capital)

        print("============================================================")
        print("开盘动态执行完成")
        print(f"目标交易日: {trading_date}")
        print(f"决策标的数: {len(df_decision)}")
        print(f"买入委托数: {len(df_orders)}")
        print(f"决策文件  : {decision_path}")
        print(f"委托文件  : {order_path}")
        print(f"摘要文件  : {summary_path}")
        print("============================================================")

        return {
            "stage_status": "SUCCESS_EXECUTED",
            "success": True,
            "trading_date": trading_date,
            "decision_count": len(df_decision),
            "order_count": len(df_orders),
            "used_capital": actual_used_capital
        }
        
    def _pick_col(self, df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
        lower_map = {str(c).strip().lower(): c for c in df.columns}
        for c in candidates:
            if c.lower() in lower_map:
                return lower_map[c.lower()]
        return None

    def _write_summary(self, path: Path, trading_date: str, df: pd.DataFrame, used_capital: float):
        lines = [
            "============================================================",
            "开盘动态执行摘要",
            f"目标交易日: {trading_date}",
            f"决策标的数: {len(df)}",
            f"实际占用资金: {used_capital:.2f}",
            "============================================================",
        ]
        path.write_text("\n".join(lines), encoding="utf-8")


def run(trading_date: str, base_dir: str = r"C:\quant_system") -> Dict:
    manager = OpenExecutionManager(base_dir=base_dir)
    return manager.run(trading_date)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--trading-date", required=True)
    parser.add_argument("--base-dir", default=r"C:\quant_system")
    args, _ = parser.parse_known_args()
    
    res = run(args.trading_date, args.base_dir)
    if res.get("stage_status") == "FAILED":
        print(f"ERROR: {res.get('error')}")
        sys.exit(1)
    sys.exit(0)

if __name__ == "__main__":
    main()