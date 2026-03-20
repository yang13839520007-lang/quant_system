# -*- coding: utf-8 -*-
"""
Created on Fri Mar 20 14:45:05 2026

@author: DELL
"""

# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Dict

# 智能兼容多目录下的管理器
try:
    from scripts.reconciliation_attribution_manager import ReconciliationAttributionManager
except ImportError:
    try:
        from reconciliation_attribution_manager import ReconciliationAttributionManager
    except ImportError:
        from core.reconciliation_attribution_manager import ReconciliationAttributionManager

BASE_DIR = r"C:\quant_system"

def run(trading_date: str, base_dir: str = BASE_DIR) -> Dict:
    print("============================================================")
    print(f"[*] 执行 Stage 11: 对账异常归因层 | 目标日期: {trading_date}")
    
    manager = ReconciliationAttributionManager(
        project_root=base_dir,
        trade_date=trading_date,
        slippage_threshold=0.01,
        position_deviation_threshold=0.10,
        full_fill_threshold=0.98,
    )
    res = manager.run()
    
    res["stage_status"] = "SUCCESS_EXECUTED"
    res["success"] = True
    return res

def main():
    parser = argparse.ArgumentParser(description="对账异常归因层")
    parser.add_argument("--trading-date", dest="trading_date", required=True)
    parser.add_argument("--base-dir", dest="base_dir", default=BASE_DIR)
    args, _ = parser.parse_known_args()
    
    res = run(trading_date=args.trading_date, base_dir=args.base_dir)
    if res.get("stage_status") == "FAILED":
        print(f"ERROR: {res.get('error', '未知错误')}")
        sys.exit(1)
    sys.exit(0)

if __name__ == "__main__":
    main()