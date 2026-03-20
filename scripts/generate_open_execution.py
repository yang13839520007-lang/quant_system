# -*- coding: utf-8 -*-
"""
Created on Thu Mar 19 11:54:00 2026

@author: DELL
"""

from __future__ import annotations

import argparse
import traceback
from pathlib import Path
from typing import Any

import pandas as pd

from core.open_execution_manager import main as open_execution_main


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="开盘动态执行层脚本入口")
    parser.add_argument("--trading-date", "--trade-date", dest="trading_date", default=None)
    parser.add_argument("--execution-plan-path", dest="execution_plan_path", default=None)
    parser.add_argument("--market-snapshot-path", dest="market_snapshot_path", default=None)
    parser.add_argument("--output-dir", dest="output_dir", default=None)
    parser.add_argument("--base-dir", "--project-root", dest="base_dir", default=None)
    parser.add_argument("--entry-chase-limit-pct", dest="entry_chase_limit_pct", type=float, default=0.035)
    parser.add_argument("--high-gap-defer-pct", dest="high_gap_defer_pct", type=float, default=0.055)
    parser.add_argument("--weak-open-reduce-pct", dest="weak_open_reduce_pct", type=float, default=-0.03)
    parser.add_argument("--broken-open-watch-pct", dest="broken_open_watch_pct", type=float, default=-0.05)
    parser.add_argument("--reduced-execute-ratio", dest="reduced_execute_ratio", type=float, default=0.5)
    return parser


def _merge_positional_args(args: tuple[Any, ...], kwargs: dict[str, Any]) -> dict[str, Any]:
    merged = dict(kwargs)
    if not args:
        return merged

    positional_names = [
        "trading_date",
        "execution_plan_path",
        "market_snapshot_path",
        "output_dir",
        "base_dir",
    ]
    for idx, value in enumerate(args):
        if idx < len(positional_names) and positional_names[idx] not in merged:
            merged[positional_names[idx]] = value
    return merged


def _infer_trading_date_from_execution_plan(execution_plan_path: str | Path | None) -> str | None:
    if not execution_plan_path:
        return None
    path = Path(execution_plan_path)
    if not path.exists():
        return None

    try:
        df = pd.read_csv(path, nrows=5)
    except Exception:
        return None

    for col in ("trading_date", "trade_date"):
        if col in df.columns:
            series = df[col].dropna()
            if not series.empty:
                return str(series.iloc[0])
    return None


def _resolve_runtime_kwargs(**kwargs: Any) -> dict[str, Any]:
    base_dir = Path(
        kwargs.get("base_dir")
        or kwargs.get("project_dir")
        or kwargs.get("root_dir")
        or Path(__file__).resolve().parents[1]
    )
    output_dir = Path(kwargs.get("output_dir") or kwargs.get("reports_dir") or (base_dir / "reports"))
    execution_plan_path = kwargs.get("execution_plan_path") or kwargs.get("execution_plan_file") or str(output_dir / "daily_execution_plan.csv")
    market_snapshot_path = kwargs.get("market_snapshot_path") or kwargs.get("market_snapshot_file") or str(output_dir / "market_signal_snapshot.csv")

    trading_date = kwargs.get("trading_date") or kwargs.get("trade_date") or kwargs.get("target_trading_date")
    if not trading_date:
        trading_date = _infer_trading_date_from_execution_plan(execution_plan_path)

    return {
        "trading_date": trading_date,
        "trade_date": trading_date,
        "target_trading_date": trading_date,
        "execution_plan_path": str(execution_plan_path),
        "execution_plan_file": str(execution_plan_path),
        "market_snapshot_path": str(market_snapshot_path),
        "market_snapshot_file": str(market_snapshot_path),
        "output_dir": str(output_dir),
        "reports_dir": str(output_dir),
        "base_dir": str(base_dir),
        "project_dir": str(base_dir),
        "root_dir": str(base_dir),
        "entry_chase_limit_pct": kwargs.get("entry_chase_limit_pct", 0.035),
        "high_gap_defer_pct": kwargs.get("high_gap_defer_pct", 0.055),
        "weak_open_reduce_pct": kwargs.get("weak_open_reduce_pct", -0.03),
        "broken_open_watch_pct": kwargs.get("broken_open_watch_pct", -0.05),
        "reduced_execute_ratio": kwargs.get("reduced_execute_ratio", 0.5),
    }


def _execute(runtime_kwargs: dict[str, Any]):
    output_dir = Path(runtime_kwargs["output_dir"])

    print("=" * 60)
    print("开盘动态执行开始")
    print(f"目标交易日  : {runtime_kwargs['trading_date']}")
    print(f"执行计划文件: {runtime_kwargs['execution_plan_path'] or output_dir / 'daily_execution_plan.csv'}")
    print(f"快照文件    : {runtime_kwargs['market_snapshot_path'] or output_dir / 'market_signal_snapshot.csv'}")
    print(f"输出目录    : {output_dir}")
    print("入口类型    : function")
    print("调用入口    : core.open_execution_manager.main")
    print("=" * 60)

    decision_df, orders_df, summary = open_execution_main(**runtime_kwargs)

    print("=" * 60)
    print("开盘动态执行完成")
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


def generate_open_execution(*args: Any, **kwargs: Any):
    return main(*args, **kwargs)


def execute_open_plan(*args: Any, **kwargs: Any):
    return main(*args, **kwargs)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc()
        raise