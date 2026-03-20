# -*- coding: utf-8 -*-
"""
Created on Wed Mar 18 13:32:51 2026

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
DEFAULT_RECON_PATH = REPORTS_DIR / "daily_trade_reconciliation_detail.csv"

MODULE_CANDIDATES = [
    "reconciliation_attribution_manager",
]

FUNCTION_CANDIDATES = [
    "generate_reconciliation_attribution",
    "run_reconciliation_attribution",
    "main",
]

CLASS_NAME_CANDIDATES = [
    "ReconciliationAttributionManager",
]

CLASS_KEYWORDS = [
    "attribution",
]

METHOD_CANDIDATES = [
    "run",
    "execute",
    "start",
    "generate",
    "main",
]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--reconciliation-path", dest="reconciliation_path", default=str(DEFAULT_RECON_PATH))
    parser.add_argument("--reports-dir", dest="reports_dir", default=str(REPORTS_DIR))
    parser.add_argument("--trade-date", dest="trade_date", default=os.environ.get("TARGET_TRADE_DATE", "2026-03-17"))
    args, unknown_args = parser.parse_known_args()

    trade_date = str(args.trade_date).strip()
    reports_dir = os.path.abspath(args.reports_dir)
    reconciliation_path = os.path.abspath(args.reconciliation_path)

    os.makedirs(reports_dir, exist_ok=True)
    set_common_env(BASE_DIR, trade_date)

    module, _ = invalidate_and_import(MODULE_CANDIDATES)

    shared_kwargs = {
        "trade_date": trade_date,
        "target_trade_date": trade_date,
        "reconciliation_path": reconciliation_path,
        "detail_path": reconciliation_path,
        "input_path": reconciliation_path,
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
        stage_title="对账异常归因层开始执行",
        trade_date=trade_date,
        reports_dir=reports_dir,
        entry_type=entry_type,
        entry_name=entry_name,
        unknown_args=unknown_args,
        extra_lines=[f"对账明细文件: {reconciliation_path}"],
    )

    result = call_with_supported_kwargs(entry, shared_kwargs)
    print_result(result)


if __name__ == "__main__":
    main()