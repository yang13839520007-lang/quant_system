# -*- coding: utf-8 -*-
"""
Created on Fri Mar 20 13:46:53 2026

@author: DELL
"""

# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import inspect
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.stage_status import normalize_stage_status


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="交易日主控编排脚本")
    parser.add_argument(
        "--trading-date",
        dest="trading_date",
        required=True,
        help="目标交易日，格式 YYYY-MM-DD",
    )
    parser.add_argument(
        "--execution-mode",
        dest="execution_mode",
        default="AUTO",
        choices=["FORCE_EXECUTE", "PARTIAL_REUSE", "AUTO"],
        help="执行模式",
    )
    parser.add_argument(
        "--base-dir",
        dest="base_dir",
        default=str(PROJECT_ROOT),
        help="项目基准目录",
    )
    parser.add_argument(
        "--bootstrap-paths",
        dest="bootstrap_paths",
        default=None,
    )
    parser.add_argument(
        "--enable-replay-validation",
        dest="enable_replay_validation",
        action="store_true",
        help="启用第13段：异常注入回放验证层",
    )
    parser.add_argument(
        "--fail-fast",
        dest="fail_fast",
        action="store_true",
        help="出现失败阶段后立即终止后续阶段",
    )
    parser.add_argument(
        "--strict-realtime-core",
        dest="strict_realtime_core",
        action="store_true",
        help="启用核心阶段强制实时执行，不允许第01/02/04/05/06/07段复用工件",
    )
    parser.add_argument(
        "--reuse-violation-action",
        dest="reuse_violation_action",
        default="warn",
        choices=["warn", "reject"],
        help="核心实时阶段命中复用时的处理动作：warn / reject",
    )
    args, _ = parser.parse_known_args()
    return args


def _resolve_reports_dir(args: argparse.Namespace, manager: Any = None, result: Dict = None) -> Path:
    reports_dir = Path(args.base_dir) / "reports"
    if manager is not None and hasattr(manager, "reports_dir"):
        reports_dir = Path(manager.reports_dir)
    return reports_dir


def _patch_reuse_audit_csv(reports_dir: Path, stage01_ok: bool, stage08_ok: bool) -> bool:
    # Historical post-run mutation hook intentionally disabled.
    # Reuse-audit artifacts are emitted directly by the orchestrator managers.
    return False


def _patch_stage_status_csv(reports_dir: Path, stage01_ok: bool, stage08_ok: bool) -> bool:
    # Historical post-run mutation hook intentionally disabled.
    # Stage status artifacts are emitted by the post-run acceptance manager.
    return False


def _patch_summary_json(reports_dir: Path, stage01_ok: bool, stage08_ok: bool, summary: Dict) -> bool:
    # Historical post-run mutation hook intentionally disabled.
    # The wrapper no longer rewrites acceptance JSON after the orchestrator finishes.
    return False


def _repair_orchestrator_outputs(reports_dir: Path, summary: Dict) -> Dict:
    stage01_files = [
        reports_dir / "daily_candidates_all.csv",
        reports_dir / "daily_candidates_top20.csv",
    ]
    stage01_ok = all(p.exists() and p.stat().st_size > 50 for p in stage01_files)

    stage08_files = [
        reports_dir / "daily_close_review.csv",
        reports_dir / "daily_close_positions.csv",
    ]
    stage08_ok = all(p.exists() and p.stat().st_size > 50 for p in stage08_files)

    return {
        "changed": False,
        "stage01_artifacts_present": stage01_ok,
        "stage08_artifacts_present": stage08_ok,
        "csv_changed": False,
        "status_changed": False,
        "json_changed": False,
    }


def normalize_orchestrator_result(result: Dict) -> Dict:
    if "stage_results" in result:
        for stage in result["stage_results"]:
            stage["stage_status"] = normalize_stage_status(stage, stage.get("stage_status"))
    return result


def _instantiate_manager(cls, args: argparse.Namespace) -> Any:
    sig = inspect.signature(cls.__init__)
    kwargs = {}

    if "base_dir" in sig.parameters:
        kwargs["base_dir"] = args.base_dir
    if "enable_replay_validation" in sig.parameters:
        kwargs["enable_replay_validation"] = args.enable_replay_validation
    if "fail_fast" in sig.parameters:
        kwargs["fail_fast"] = args.fail_fast
    if "strict_realtime_core" in sig.parameters:
        kwargs["strict_realtime_core"] = args.strict_realtime_core
    if "reuse_violation_action" in sig.parameters:
        kwargs["reuse_violation_action"] = args.reuse_violation_action

    manager = cls(**kwargs)
    return manager


def _invoke_run(manager: Any, args: argparse.Namespace) -> Dict:
    inject_pairs = {
        "trading_date": args.trading_date,
        "target_trading_date": args.trading_date,
        "date": args.trading_date,
        "as_of_date": args.trading_date,
    }

    sig = inspect.signature(manager.run)
    run_kwargs = {}

    for param_name in sig.parameters:
        if param_name in inject_pairs:
            run_kwargs[param_name] = inject_pairs[param_name]

    return manager.run(**run_kwargs)


def main() -> None:
    args = parse_args()

    import trading_day_orchestrator_manager  # noqa: E402

    manager_cls = getattr(trading_day_orchestrator_manager, "TradingDayOrchestratorManager")
    manager = _instantiate_manager(manager_cls, args)
    result = _invoke_run(manager, args)

    result = normalize_orchestrator_result(result)

    reports_dir = _resolve_reports_dir(args, manager=manager, result=result)
    post_fix = _repair_orchestrator_outputs(reports_dir, summary=result)

    if post_fix.get("changed"):
        print("[POST-FIX] 已基于产物校验修正主控收尾摘要与复用审计工件")

    print("============================================================")
    print("主控脚本最终结论")
    print(f"目标交易日: {args.trading_date}")
    print(f"总体状态: {result.get('overall_status')}")
    print(f"验收状态: {result.get('acceptance_status')}")
    print(f"运行模式: {result.get('run_mode_label')}")
    print(f"复用审计状态: {result.get('reuse_audit_status')}")
    print(f"生产模式标识: {result.get('production_mode_label')}")
    print(
        f"核心阶段实时覆盖率: "
        f"{result.get('reuse_metrics', {}).get('core_realtime_coverage_ratio', 0.0)}"
    )
    print("============================================================")


if __name__ == "__main__":
    main()
