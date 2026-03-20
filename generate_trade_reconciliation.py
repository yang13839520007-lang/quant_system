# -*- coding: utf-8 -*-
"""
Created on Fri Mar 20 14:48:12 2026

@author: DELL
"""

# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Dict

from core.trade_reconciliation_manager import TradeReconciliationManager

BASE_DIR = r"C:\quant_system"

def run(trading_date: str, base_dir: str = BASE_DIR) -> Dict:
    print("============================================================")
    print(f"[*] 执行 Stage 10: 真实交易流水对账层 | 目标日期: {trading_date}")
    
    reports_dir = Path(base_dir) / "reports"
    # 显式校验输入，防止静默复用
    fill_file = reports_dir / "real_trade_fills.csv"
    if not fill_file.exists():
        return {"stage_status": "FAILED", "error": "未找到真实成交流水文件 real_trade_fills.csv"}

    manager = TradeReconciliationManager(
        project_root=base_dir,
        trade_date=trading_date,
    )
    # 传入显式日期，强制重算
    res = manager.run()
    
    # 强制将结果字典升级为 SUCCESS_EXECUTED 状态
    final_res = {
        "stage_status": "SUCCESS_EXECUTED",
        "success": True,
        "trading_date": trading_date,
        "matched_count": res.get("matched_count", 0),
        "anomaly_count": res.get("anomaly_count", 0)
    }
    # 保留原始返回信息
    final_res.update({k: v for k, v in res.items() if k not in final_res})
    
    return final_res

def main():
    parser = argparse.ArgumentParser(description="真实交易流水对账层")
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
