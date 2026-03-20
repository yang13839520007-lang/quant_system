# -*- coding: utf-8 -*-
"""
Created on Wed Mar 18 13:33:30 2026

@author: DELL
"""

import os
import argparse

from stage_entry_runner import (
    bootstrap_paths,
    invalidate_and_import,
    resolve_entry,
    call_with_supported_kwargs,
    set_common_env,
    print_stage_header,
    print_result,
)


SCRIPT_DIR, BASE_DIR = bootstrap_paths(__file__)
REPORTS_DIR = BASE_DIR / "reports"
DEFAULT_ATTR_PATH = REPORTS_DIR / "daily_trade_reconciliation_attribution_detail.csv"

MODULE_CANDIDATES = [
    "reconciliation_review_manager",
]

FUNCTION_CANDIDATES = [
    "generate_reconciliation_review",
    "run_reconciliation_review",
    "main",
]

CLASS_NAME_CANDIDATES = [
    "ReconciliationReviewManager",
]

CLASS_KEYWORDS = [
    "review",
    "reconciliation",
]

METHOD_CANDIDATES = [
    "run",
    "execute",
    "start",
    "generate",
    "review",
    "main",
]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--attribution-path", dest="attribution_path", default=str(DEFAULT_ATTR_PATH))
    parser.add_argument("--reports-dir", dest="reports_dir", default=str(REPORTS_DIR))
    parser.add_argument("--trade-date", dest="trade_date", default=os.environ.get("TARGET_TRADE_DATE", "2026-03-17"))
    args, unknown_args = parser.parse_known_args()

    trade_date = str(args.trade_date).strip()
    reports_dir = os.path.abspath(args.reports_dir)
    attribution_path = os.path.abspath(args.attribution_path)

    os.makedirs(reports_dir, exist_ok=True)
    set_common_env(BASE_DIR, trade_date)

    module, _ = invalidate_and_import(MODULE_CANDIDATES)

    shared_kwargs = {
        "trade_date": trade_date,
        "target_trade_date": trade_date,
        "attribution_path": attribution_path,
        "input_path": attribution_path,
        "reports_dir": reports_dir,
        "base_dir": str(BASE_DIR),
        "project_root": str(BASE_DIR),
    }

    entry, entry_name, entry_type = resolve_entry(
        module=module,
        init_kwargs=shared_kwargs,
        function_candidates=FUNCTION_CANDIDATES,
        class_name_candidates=CLASS_NAME_CANDIDATES,
        class_keywords=CLASS_KEYWORDS,
        method_candidates=METHOD_CANDIDATES,
    )

    print_stage_header(
        stage_title="异常闭环复盘层开始执行",
        trade_date=trade_date,
        reports_dir=reports_dir,
        entry_type=entry_type,
        entry_name=entry_name,
        unknown_args=unknown_args,
        extra_lines=[f"归因明细文件: {attribution_path}"],
    )

    result = call_with_supported_kwargs(entry, shared_kwargs)
    print_result(result)


if __name__ == "__main__":
    main()