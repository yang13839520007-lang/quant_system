# -*- coding: utf-8 -*-
"""
Created on Wed Mar 18 13:30:15 2026

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
DEFAULT_RISK_CHECKED_PATH = REPORTS_DIR / "daily_portfolio_plan_risk_checked.csv"

MODULE_CANDIDATES = [
    "execution_plan_manager",
    "portfolio_execution_manager",
    "core.portfolio_executor",
    "core.execution_plan_manager",
]

FUNCTION_CANDIDATES = [
    "generate_execution_plan",
    "build_execution_plan",
    "run_execution_plan",
    "generate_daily_execution_plan",
    "main",
]

CLASS_NAME_CANDIDATES = [
    "ExecutionPlanManager",
    "PortfolioExecutionManager",
    "PortfolioExecutor",
]

CLASS_KEYWORDS = [
    "execution",
    "executor",
    "plan",
]

METHOD_CANDIDATES = [
    "run",
    "execute",
    "start",
    "generate",
    "build",
    "main",
]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--risk-checked-path", dest="risk_checked_path", default=str(DEFAULT_RISK_CHECKED_PATH))
    parser.add_argument("--reports-dir", dest="reports_dir", default=str(REPORTS_DIR))
    parser.add_argument("--capital", dest="capital", type=float, default=None)
    parser.add_argument("--trade-date", dest="trade_date", default=os.environ.get("TARGET_TRADE_DATE", "2026-03-17"))
    args, unknown_args = parser.parse_known_args()

    trade_date = str(args.trade_date).strip()
    reports_dir = os.path.abspath(args.reports_dir)
    risk_checked_path = os.path.abspath(args.risk_checked_path)

    os.makedirs(reports_dir, exist_ok=True)
    set_common_env(BASE_DIR, trade_date)

    module, _ = invalidate_and_import(MODULE_CANDIDATES)

    shared_kwargs = {
        "trade_date": trade_date,
        "target_trade_date": trade_date,
        "risk_checked_path": risk_checked_path,
        "portfolio_path": risk_checked_path,
        "input_path": risk_checked_path,
        "reports_dir": reports_dir,
        "capital": args.capital,
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
        stage_title="组合执行优先级层开始执行",
        trade_date=trade_date,
        reports_dir=reports_dir,
        entry_type=entry_type,
        entry_name=entry_name,
        unknown_args=unknown_args,
        extra_lines=[
            f"风控结果文件: {risk_checked_path}",
            f"总资金      : {args.capital}" if args.capital is not None else "",
        ],
    )

    result = call_with_supported_kwargs(entry, shared_kwargs)
    print_result(result)


if __name__ == "__main__":
    main()