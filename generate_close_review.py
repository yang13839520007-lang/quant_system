# -*- coding: utf-8 -*-
"""
Created on Fri Mar 20 14:38:47 2026

@author: DELL
"""

# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import sys
from typing import Dict

from core.close_review_manager import CloseReviewManager

BASE_DIR = r"C:\quant_system"

def run(trading_date: str, base_dir: str = BASE_DIR) -> Dict:
    manager = CloseReviewManager(base_dir=base_dir)
    return manager.run(trading_date=trading_date)

def main():
    parser = argparse.ArgumentParser(description="收盘复盘层")
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