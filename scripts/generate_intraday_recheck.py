# -*- coding: utf-8 -*-
"""
Created on Thu Mar 19 12:37:33 2026

@author: DELL
"""

from __future__ import annotations

import argparse
import traceback
from pathlib import Path
from typing import Any

from core.intraday_recheck_manager import main as intraday_recheck_main


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="盘中二次确认层脚本入口")
    parser.add_argument("--trading-date", "--trade-date", dest="trading_date", default=None)
    parser.add_argument("--open-execution-decision-path", dest="open_execution_decision_path", default=None)
    parser.add_argument("--market-snapshot-path", dest="market_snapshot_path", default=None)
    parser.add_argument("--output-dir", dest="output_dir", default=None)
    parser.add_argument("--base-dir", "--project-root", dest="base_dir", default=None)
    parser.add_argument("--recheck-chase-limit-pct", dest="recheck_chase_limit_pct", type=float, default=0.045)
    parser.add_argument("--recheck-reduce-chase-pct", dest="recheck_reduce_chase_pct", type=float, default=0.06)
    parser.add_argument("--recheck-break-stop-buffer-pct", dest="recheck_break_stop_buffer_pct", type=float, default=0.0)
    parser.add_argument("--reduced-execute-ratio", dest="reduced_execute_ratio", type=float, default=0.5)
    return parser


def _merge_positional_args(args: tuple[Any, ...], kwargs: dict[str, Any]) -> dict[str, Any]:
    merged = dict(kwargs)
    positional_names = [
        "trading_date",
        "open_execution_decision_path",
        "market_snapshot_path",
        "output_dir",
        "base_dir",
    ]
    for idx, value in enumerate(args):
        if idx < len(positional_names) and positional_names[idx] not in merged:
            merged[positional_names[idx]] = value
    return merged


def _resolve_runtime_kwargs(**kwargs: Any) -> dict[str, Any]:
    base_dir = Path(
        kwargs.get("base_dir")
        or kwargs.get("project_dir")
        or kwargs.get("root_dir")
        or Path(__file__).resolve().parents[1]
    )
    output_dir = Path(kwargs.get("output_dir") or kwargs.get("reports_dir") or (base_dir / "reports"))

    trading_date = kwargs.get("trading_date") or kwargs.get("trade_date") or kwargs.get("target_trading_date")
    open_execution_decision_path = (
        kwargs.get("open_execution_decision_path")
        or kwargs.get("open_execution_path")
        or kwargs.get("decision_path")
        or str(output_dir / "daily_open_execution_decision.csv")
    )
    market_snapshot_path = (
        kwargs.get("market_snapshot_path")
        or kwargs.get("market_snapshot_file")
        or str(output_dir / "market_signal_snapshot.csv")
    )

    return {
        "trading_date": trading_date,
        "trade_date": trading_date,
        "target_trading_date": trading_date,
        "open_execution_decision_path": str(open_execution_decision_path),
        "open_execution_path": str(open_execution_decision_path),
        "decision_path": str(open_execution_decision_path),
        "market_snapshot_path": str(market_snapshot_path),
        "market_snapshot_file": str(market_snapshot_path),
        "output_dir": str(output_dir),
        "reports_dir": str(output_dir),
        "base_dir": str(base_dir),
        "project_dir": str(base_dir),
        "root_dir": str(base_dir),
        "recheck_chase_limit_pct": kwargs.get("recheck_chase_limit_pct", 0.045),
        "recheck_reduce_chase_pct": kwargs.get("recheck_reduce_chase_pct", 0.06),
        "recheck_break_stop_buffer_pct": kwargs.get("recheck_break_stop_buffer_pct", 0.0),
        "reduced_execute_ratio": kwargs.get("reduced_execute_ratio", 0.5),
    }


def _execute(runtime_kwargs: dict[str, Any]):
    output_dir = Path(runtime_kwargs["output_dir"])

    print("=" * 60)
    print("盘中二次确认开始")
    print(f"目标交易日  : {runtime_kwargs['trading_date']}")
    print(f"开盘决策文件: {runtime_kwargs['open_execution_decision_path'] or output_dir / 'daily_open_execution_decision.csv'}")
    print(f"快照文件    : {runtime_kwargs['market_snapshot_path'] or output_dir / 'market_signal_snapshot.csv'}")
    print(f"输出目录    : {output_dir}")
    print("入口类型    : function")
    print("调用入口    : core.intraday_recheck_manager.main")
    print("=" * 60)

    decision_df, orders_df, summary = intraday_recheck_main(**runtime_kwargs)

    print("=" * 60)
    print("盘中二次确认完成")
    print(f"目标交易日: {summary.get('trading_date')}")
    print(f"决策标的数: {len(decision_df)}")
    print(f"买入委托数: {len(orders_df)}")
    print(f"决策文件  : {summary['decision_path']}")
    print(f"委托文件  : {summary['orders_path']}")
    print(f"摘要文件  : {summary['summary_path']}")
    print("=" * 60)
    return decision_df, orders_df, summary


def main(*args: Any, **kwargs: Any):
    if not args and not kwargs:
        parsed_args, _ = _build_parser().parse_known_args()
        kwargs = vars(parsed_args)
    else:
        kwargs = _merge_positional_args(args, kwargs)

    runtime_kwargs = _resolve_runtime_kwargs(**kwargs)
    return _execute(runtime_kwargs)


def run(*args: Any, **kwargs: Any):
    return main(*args, **kwargs)


def generate_intraday_recheck(*args: Any, **kwargs: Any):
    return main(*args, **kwargs)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc()
        raise