# -*- coding: utf-8 -*-
"""
Created on Fri Mar 20 13:36:01 2026

@author: DELL
"""

# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sys
import json
import argparse
import pandas as pd
from pathlib import Path

# 确保能加载根目录的 core 模块
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.position_manager import PositionManager

# 全局非核心层执行策略注册表 (响应主线A改造要求)
try:
    from core.noncore_force_execute_registry import NONCORE_FORCE_EXECUTE_REGISTRY
except ImportError:
    NONCORE_FORCE_EXECUTE_REGISTRY = {"Stage_09": True, "Stage_10": False, "Stage_12": False}

def parse_args():
    parser = argparse.ArgumentParser(description="Stage 09: Next Day Position Management (De-reused / Realtime)")
    parser.add_argument("--trading-date", dest="trading_date", required=True, help="Target trading date")
    parser.add_argument("--execution-mode", dest="execution_mode", type=str, default="AUTO", choices=["FORCE_EXECUTE", "PARTIAL_REUSE", "AUTO"])
    parser.add_argument("--base-dir", dest="base_dir", type=str, default=str(PROJECT_ROOT))
    parser.add_argument("--bootstrap-paths", type=str, default=None)
    # 严格使用 parse_known_args 防止 sys.argv 污染导致主控崩溃
    return parser.parse_known_args()[0]

def main():
    args = parse_args()
    target_date = args.trading_date
    execution_mode = args.execution_mode
    base_dir = Path(args.base_dir)
    reports_dir = base_dir / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    
    # 依赖 Stage 08 产出的绝对真相底稿
    close_positions_file = str(reports_dir / "daily_close_positions.csv")
    
    # 当前 Stage 的输出
    out_plan_file = reports_dir / "daily_next_day_position_plan.csv"
    out_summary_file = reports_dir / "daily_next_day_management_summary.txt"

    # ---------------------------------------------------------
    # 主线A: 统一非核心执行策略 & 去复用判定
    # ---------------------------------------------------------
    is_force_execute = False
    if execution_mode == "FORCE_EXECUTE":
        is_force_execute = True
    elif execution_mode == "AUTO" and NONCORE_FORCE_EXECUTE_REGISTRY.get("Stage_09", True):
        is_force_execute = True

    if not is_force_execute and execution_mode in ["PARTIAL_REUSE", "AUTO"]:
        if out_plan_file.exists() and out_summary_file.exists():
            print(f"[*] Stage 09 Reused: Found existing outputs and execution mode is {execution_mode}")
            sys.exit(0)  # 主控将捕获为 SUCCESS_REUSED

    print(f"[*] Stage 09 Executing: Mode={execution_mode}, Force={is_force_execute}")

    # 1. 加载 Stage 08 生成的收盘持仓
    pos_manager = PositionManager(target_date=target_date, close_positions_file=close_positions_file)

    # 2. 执行 T+1 持仓状态流转 (冻结转可用，天数累加)
    df_rolled = pos_manager.rollover_t1_positions()

    # 3. 基于高胜率短线风控生成次日操作建议
    risk_rules = {
        'take_profit_ratio': 0.05,  # >=5% 止盈
        'stop_loss_ratio': -0.03,   # <=-3% 止损
        'max_hold_days': 2          # T+2 必须离场
    }
    df_plan = pos_manager.generate_next_day_plan(df_rolled, risk_rules)

    # 4. 强制落盘覆盖 (实现去复用)
    if not df_plan.empty:
        df_plan.to_csv(out_plan_file, index=False, encoding='utf-8-sig')
    else:
        empty_cols = PositionManager.REQUIRED_COLUMNS + ['next_day_action', 'action_reason']
        pd.DataFrame(columns=empty_cols).to_csv(out_plan_file, index=False, encoding='utf-8-sig')

    # 5. 生成标准 JSON 兼容的 Summary
    total_holdings = len(df_plan)
    sell_signals = len(df_plan[df_plan['next_day_action'] == 'SELL']) if not df_plan.empty else 0
    locked_signals = len(df_plan[df_plan['next_day_action'] == 'HOLD_LOCKED']) if not df_plan.empty else 0
    
    summary_data = {
        "trading_date": target_date,
        "execution_mode": execution_mode,
        "is_force_executed": is_force_execute,
        "total_holding_symbols": total_holdings,
        "next_day_sell_signals": sell_signals,
        "t1_locked_holdings": locked_signals,
        "stage_status": "SUCCESS_EXECUTED"
    }

    with open(out_summary_file, "w", encoding="utf-8") as f:
        json.dump(summary_data, f, indent=4, ensure_ascii=False)

    print("============================================================")
    print("次日持仓续管生成完成")
    print(f"持仓标的数: {total_holdings}")
    print(f"可执行卖出信号: {sell_signals}")
    print(f"T+1锁仓拦截: {locked_signals}")
    print(f"输出文件: {out_plan_file}")
    print("============================================================")
    sys.exit(0)

if __name__ == "__main__":
    main()