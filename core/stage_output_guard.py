# -*- coding: utf-8 -*-
"""
Created on Thu Mar 19 08:58:50 2026

@author: DELL
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List


STAGE_OUTPUT_PATTERNS: Dict[int, List[str]] = {
    1: [
        "reports/market_signal_snapshot*.csv",
        "reports/market_signal_snapshot*.txt",
        "reports/daily_candidates_*.csv",
    ],
    2: [
        "reports/daily_trade_plan_*.csv",
    ],
    3: [
        "reports/daily_portfolio_plan.csv",
        "reports/daily_portfolio_plan_top5.csv",
        "reports/daily_portfolio_summary.txt",
    ],
    4: [
        "reports/daily_portfolio_plan_risk_checked.csv",
        "reports/daily_portfolio_summary_risk_checked.txt",
    ],
    5: [
        "reports/daily_execution_plan.csv",
        "reports/daily_execution_plan_keep.csv",
        "reports/daily_execution_plan_summary.txt",
    ],
    6: [
        "reports/daily_open_execution_decision.csv",
        "reports/daily_open_execution_orders.csv",
        "reports/daily_open_execution_summary.txt",
    ],
    7: [
        "reports/daily_intraday_recheck_decision.csv",
        "reports/daily_intraday_recheck_orders.csv",
        "reports/daily_intraday_recheck_summary.txt",
    ],
    8: [
        "reports/daily_close_review.csv",
        "reports/daily_close_positions.csv",
        "reports/daily_close_review_summary.txt",
        "reports/daily_next_day_watchlist.csv",
    ],
    9: [
        "reports/daily_next_day_management.csv",
        "reports/daily_next_day_management_summary.txt",
        "reports/daily_next_day_hold_pool.csv",
        "reports/daily_next_day_sell_priority.csv",
        "reports/daily_next_day_take_profit_priority.csv",
        "reports/daily_next_day_watch_pool.csv",
    ],
    10: [
        "reports/daily_trade_reconciliation*.csv",
        "reports/daily_trade_reconciliation*.txt",
    ],
    11: [
        "reports/daily_trade_reconciliation_attribution*.csv",
        "reports/daily_trade_reconciliation_attribution*.txt",
        "reports/daily_reconciliation_attribution*.csv",
        "reports/daily_reconciliation_attribution*.txt",
    ],
    12: [
        "reports/daily_reconciliation_review*.csv",
        "reports/daily_reconciliation_review*.txt",
    ],
    13: [
        "reports/daily_reconciliation_replay_validation*.csv",
        "reports/daily_reconciliation_replay_validation*.txt",
    ],
}


def _resolve_root(project_root: str | Path) -> Path:
    root = Path(project_root).resolve()
    if root.name.lower() == "scripts":
        root = root.parent
    return root


def get_stage_output_patterns(stage_no: int) -> List[str]:
    return list(STAGE_OUTPUT_PATTERNS.get(int(stage_no), []))


def list_stage_output_files(stage_no: int, project_root: str | Path) -> List[str]:
    root = _resolve_root(project_root)
    paths: List[str] = []

    for pattern in get_stage_output_patterns(stage_no):
        for path in root.glob(pattern):
            if path.is_file():
                paths.append(str(path.resolve()))

    return sorted(set(paths))


def purge_stage_output_files(stage_no: int, project_root: str | Path) -> List[str]:
    root = _resolve_root(project_root)
    removed: List[str] = []

    for pattern in get_stage_output_patterns(stage_no):
        for path in root.glob(pattern):
            if not path.is_file():
                continue
            try:
                path.unlink()
                removed.append(str(path.resolve()))
            except FileNotFoundError:
                continue

    return sorted(set(removed))


def stage_has_existing_outputs(stage_no: int, project_root: str | Path) -> bool:
    return len(list_stage_output_files(stage_no=stage_no, project_root=project_root)) > 0