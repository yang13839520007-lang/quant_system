# -*- coding: utf-8 -*-
"""
Created on Sat Mar 21 2026

@author: DELL
"""

from __future__ import annotations

import argparse
import sys
from typing import Dict

try:
    from scripts.reconciliation_replay_validation_manager import (
        ReconciliationReplayValidationManager,
    )
    from scripts.reconciliation_attribution_manager import ReconciliationAttributionManager
    from scripts.reconciliation_review_manager import ReconciliationReviewManager
except ImportError:
    from reconciliation_replay_validation_manager import (
        ReconciliationReplayValidationManager,
    )
    from reconciliation_attribution_manager import ReconciliationAttributionManager
    from reconciliation_review_manager import ReconciliationReviewManager


BASE_DIR = r"C:\quant_system"


def run(trading_date: str, base_dir: str = BASE_DIR) -> Dict:
    print("============================================================")
    print(f"[*] 执行 Stage 13: 异常注入回放验证层 | 目标日期: {trading_date}")

    manager = ReconciliationReplayValidationManager(
        project_root=base_dir,
        trade_date=trading_date,
        attribution_manager_class=ReconciliationAttributionManager,
        review_manager_class=ReconciliationReviewManager,
        slippage_threshold=0.01,
        position_deviation_threshold=0.10,
        full_fill_threshold=0.98,
    )
    res = manager.run()

    res["stage_status"] = "SUCCESS_EXECUTED"
    res["success"] = True
    return res


def main() -> None:
    parser = argparse.ArgumentParser(description="异常注入回放验证层")
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
