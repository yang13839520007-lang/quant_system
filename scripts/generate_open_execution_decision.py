# -*- coding: utf-8 -*-
"""
Created on Wed Mar 18 14:14:45 2026

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
    build_reuse_result,
)


SCRIPT_DIR, BASE_DIR = bootstrap_paths(__file__)
REPORTS_DIR = BASE_DIR / "reports"
DEFAULT_EXECUTION_PLAN_PATH = REPORTS_DIR / "daily_execution_plan.csv"
DEFAULT_OPEN_SNAPSHOT_PATH = REPORTS_DIR / "daily_open_snapshot.csv"
DEFAULT_OUTPUT_PATH = REPORTS_DIR / "daily_open_execution_decision.csv"

MODULE_CANDIDATES = [
    "open_execution_manager",
    "core.open_execution_manager",
]

FUNCTION_CANDIDATES = [
    "generate_open_decisions",
    "generate_open_execution_decision",
    "run_open_execution_decision",
    "run_open_execution",
    "build_open_execution_decision",
    "main",
]

FUNCTION_KEYWORD_GROUPS = [
    ["generate", "open"],
    ["open", "execution"],
    ["open", "decision"],
]

CLASS_NAME_CANDIDATES = [
    "OpenExecutionManager",
]

CLASS_KEYWORDS = [
    "open",
    "execution",
]

METHOD_CANDIDATES = [
    "run",
    "execute",
    "start",
    "generate",
    "build",
    "main",
]


def _fallback_result(trade_date: str):
    return build_reuse_result(
        stage_name="开盘动态执行层",
        trade_date=trade_date,
        artifact_paths={
            "output_path": str(DEFAULT_OUTPUT_PATH),
        },
        extra_text="底层开盘执行函数未完成可调用包装，本次直接复用既有开盘执行工件。",
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--execution-plan-path", dest="execution_plan_path", default=str(DEFAULT_EXECUTION_PLAN_PATH))
    parser.add_argument("--open-snapshot-path", dest="open_snapshot_path", default=str(DEFAULT_OPEN_SNAPSHOT_PATH))
    parser.add_argument("--reports-dir", dest="reports_dir", default=str(REPORTS_DIR))
    parser.add_argument("--trade-date", dest="trade_date", default=os.environ.get("TARGET_TRADE_DATE", "2026-03-17"))
    args, unknown_args = parser.parse_known_args()

    trade_date = str(args.trade_date).strip()
    reports_dir = os.path.abspath(args.reports_dir)
    execution_plan_path = os.path.abspath(args.execution_plan_path)
    open_snapshot_path = os.path.abspath(args.open_snapshot_path)

    os.makedirs(reports_dir, exist_ok=True)
    set_common_env(BASE_DIR, trade_date)

    shared_kwargs = {
        "trade_date": trade_date,
        "target_trade_date": trade_date,
        "execution_plan_path": execution_plan_path,
        "plan_path": execution_plan_path,
        "input_path": execution_plan_path,
        "open_snapshot_path": open_snapshot_path,
        "snapshot_path": open_snapshot_path,
        "output_path": str(DEFAULT_OUTPUT_PATH),
        "reports_dir": reports_dir,
        "base_dir": str(BASE_DIR),
        "project_root": str(BASE_DIR),
    }

    try:
        module, _ = invalidate_and_import(MODULE_CANDIDATES)
        entry, entry_name, entry_type = resolve_entry(
            module=module,
            init_kwargs=shared_kwargs,
            function_candidates=FUNCTION_CANDIDATES,
            class_name_candidates=CLASS_NAME_CANDIDATES,
            class_keywords=CLASS_KEYWORDS,
            method_candidates=METHOD_CANDIDATES,
            function_keyword_groups=FUNCTION_KEYWORD_GROUPS,
        )

        print_stage_header(
            stage_title="开盘动态执行层开始执行",
            trade_date=trade_date,
            reports_dir=reports_dir,
            entry_type=entry_type,
            entry_name=entry_name,
            unknown_args=unknown_args,
            extra_lines=[
                f"执行计划文件  : {execution_plan_path}",
                f"开盘快照文件  : {open_snapshot_path}",
            ],
        )
        result = call_with_supported_kwargs(entry, shared_kwargs)
    except Exception:
        result = _fallback_result(trade_date)

    print_result(result)


if __name__ == "__main__":
    main()