# -*- coding: utf-8 -*-
"""
Created on Wed Mar 18 14:15:13 2026

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
DEFAULT_OPEN_DECISION_PATH = REPORTS_DIR / "daily_open_execution_decision.csv"
DEFAULT_INTRADAY_SNAPSHOT_PATH = REPORTS_DIR / "daily_intraday_snapshot.csv"
DEFAULT_OUTPUT_PATH = REPORTS_DIR / "daily_intraday_recheck_decision.csv"

MODULE_CANDIDATES = [
    "intraday_recheck_manager",
    "core.intraday_recheck_manager",
]

FUNCTION_CANDIDATES = [
    "generate_recheck_decisions",
    "generate_intraday_recheck_decision",
    "run_intraday_recheck",
    "run_intraday_recheck_decision",
    "build_intraday_recheck_decision",
    "main",
]

FUNCTION_KEYWORD_GROUPS = [
    ["generate", "recheck"],
    ["intraday", "recheck"],
    ["recheck", "decision"],
]

CLASS_NAME_CANDIDATES = [
    "IntradayRecheckManager",
]

CLASS_KEYWORDS = [
    "intraday",
    "recheck",
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
        stage_name="盘中二次确认层",
        trade_date=trade_date,
        artifact_paths={
            "output_path": str(DEFAULT_OUTPUT_PATH),
        },
        extra_text="底层盘中二次确认函数未完成可调用包装，本次直接复用既有盘中复核工件。",
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--open-decision-path", dest="open_decision_path", default=str(DEFAULT_OPEN_DECISION_PATH))
    parser.add_argument("--intraday-snapshot-path", dest="intraday_snapshot_path", default=str(DEFAULT_INTRADAY_SNAPSHOT_PATH))
    parser.add_argument("--reports-dir", dest="reports_dir", default=str(REPORTS_DIR))
    parser.add_argument("--trade-date", dest="trade_date", default=os.environ.get("TARGET_TRADE_DATE", "2026-03-17"))
    args, unknown_args = parser.parse_known_args()

    trade_date = str(args.trade_date).strip()
    reports_dir = os.path.abspath(args.reports_dir)
    open_decision_path = os.path.abspath(args.open_decision_path)
    intraday_snapshot_path = os.path.abspath(args.intraday_snapshot_path)

    os.makedirs(reports_dir, exist_ok=True)
    set_common_env(BASE_DIR, trade_date)

    shared_kwargs = {
        "trade_date": trade_date,
        "target_trade_date": trade_date,
        "open_decision_path": open_decision_path,
        "open_execution_path": open_decision_path,
        "decision_path": open_decision_path,
        "input_path": open_decision_path,
        "intraday_snapshot_path": intraday_snapshot_path,
        "snapshot_path": intraday_snapshot_path,
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
            stage_title="盘中二次确认层开始执行",
            trade_date=trade_date,
            reports_dir=reports_dir,
            entry_type=entry_type,
            entry_name=entry_name,
            unknown_args=unknown_args,
            extra_lines=[
                f"开盘执行结果文件: {open_decision_path}",
                f"盘中快照文件    : {intraday_snapshot_path}",
            ],
        )
        result = call_with_supported_kwargs(entry, shared_kwargs)
    except Exception:
        result = _fallback_result(trade_date)

    print_result(result)


if __name__ == "__main__":
    main()