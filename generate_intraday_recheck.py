# -*- coding: utf-8 -*-
"""
Created on Fri Mar 20 14:38:26 2026

@author: DELL
"""

# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import sys
from typing import Any, Dict

from core.intraday_recheck_manager import IntradayRecheckManager

BASE_DIR = r"C:\quant_system"


def _normalize_result(raw_result: Any) -> Dict:
    if isinstance(raw_result, dict):
        result = dict(raw_result)
    elif isinstance(raw_result, tuple) and len(raw_result) == 3 and isinstance(raw_result[2], dict):
        decision_df, orders_df, summary = raw_result
        result = dict(summary)
        result.setdefault("decision_rows", int(len(decision_df)))
        result.setdefault("order_rows", int(len(orders_df)))
    else:
        result = {"result": raw_result}

    result["stage_status"] = "SUCCESS_EXECUTED"
    result["success"] = True
    return result


def run(trading_date: str, base_dir: str = BASE_DIR) -> Dict:
    manager = IntradayRecheckManager(base_dir=base_dir)
    return _normalize_result(manager.run(trading_date=trading_date))

def main():
    parser = argparse.ArgumentParser(description="盘中二次确认层")
    parser.add_argument("--trading-date", dest="trading_date", required=True)
    parser.add_argument("--base-dir", dest="base_dir", default=BASE_DIR)
    args, _ = parser.parse_known_args()
    
    res = run(trading_date=args.trading_date, base_dir=args.base_dir)
    if res.get("stage_status") == "FAILED":
        print(f"ERROR: {res.get('error')}")
        sys.exit(1)
    sys.exit(0)

if __name__ == "__main__":
    main()
