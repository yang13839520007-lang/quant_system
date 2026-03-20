# -*- coding: utf-8 -*-
"""
Created on Fri Mar 20 13:46:53 2026

@author: DELL
"""

# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import inspect
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

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
    return False
    audit_csv = reports_dir / "daily_orchestrator_reuse_audit.csv"
    if not audit_csv.exists():
        return False

    try:
        df = pd.read_csv(audit_csv, encoding="utf-8-sig")
        changed = False

        status_col = None
        for col in ["stage_status", "status"]:
            if col in df.columns:
                status_col = col
                break

        policy_level_col = None
        for col in ["policy_level", "policy_expected"]:
            if col in df.columns:
                policy_level_col = col
                break

        policy_message_col = None
        for col in ["policy_message", "message"]:
            if col in df.columns:
                policy_message_col = col
                df[policy_message_col] = df[policy_message_col].astype(object)
                break

        if not status_col:
            return False

        if stage01_ok:
            mask = (df["stage_no"] == 1) & (df[status_col] == "FAILED")
            if mask.any():
                df.loc[mask, status_col] = "SUCCESS_EXECUTED"
                if policy_level_col:
                    df.loc[mask, policy_level_col] = "POST_RUN_ARTIFACT_VERIFIED"
                if policy_message_col:
                    df.loc[mask, policy_message_col] = "Stage 01 产物校验通过，按执行成功修正"
                changed = True

        if stage08_ok:
            mask = (df["stage_no"] == 8) & (df[status_col] == "FAILED")
            if mask.any():
                df.loc[mask, status_col] = "SUCCESS_EXECUTED"
                if policy_level_col:
                    df.loc[mask, policy_level_col] = "POST_RUN_ARTIFACT_VERIFIED"
                if policy_message_col:
                    df.loc[mask, policy_message_col] = "Stage 08 产物校验通过，按执行成功修正"
                changed = True

        if changed:
            df.to_csv(audit_csv, index=False, encoding="utf-8-sig")
            return True
            
    except Exception as e:
        print(f"[POST-FIX ERROR] 修正 audit csv 失败: {e}")
        
    return False


def _patch_stage_status_csv(reports_dir: Path, stage01_ok: bool, stage08_ok: bool) -> bool:
    return False
    status_csv = reports_dir / "daily_orchestrator_stage_status.csv"
    if not status_csv.exists():
        return False

    try:
        df = pd.read_csv(status_csv, encoding="utf-8-sig")
        changed = False

        status_col = None
        for col in ["stage_status", "status"]:
            if col in df.columns:
                status_col = col
                break

        if not status_col:
            return False

        if stage01_ok:
            mask = (df["stage_no"] == 1) & (df[status_col] == "FAILED")
            if mask.any():
                df.loc[mask, status_col] = "SUCCESS_EXECUTED"
                changed = True

        if stage08_ok:
            mask = (df["stage_no"] == 8) & (df[status_col] == "FAILED")
            if mask.any():
                df.loc[mask, status_col] = "SUCCESS_EXECUTED"
                changed = True

        if changed:
            df.to_csv(status_csv, index=False, encoding="utf-8-sig")
            return True
            
    except Exception as e:
        print(f"[POST-FIX ERROR] 修正 stage status csv 失败: {e}")

    return False


def _patch_summary_json(reports_dir: Path, stage01_ok: bool, stage08_ok: bool, summary: Dict) -> bool:
    return False
    json_path = reports_dir / "daily_orchestrator_acceptance.json"
    if not json_path.exists():
        return False

    changed = False
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        counts = data.get("stage_status_counts", {})
        
        if stage01_ok:
            for stage in summary.get("stage_results", []):
                if stage.get("stage_no") == 1 and stage.get("stage_status") == "FAILED":
                    stage["stage_status"] = "SUCCESS_EXECUTED"
                    counts["FAILED"] = max(0, counts.get("FAILED", 0) - 1)
                    counts["SUCCESS_EXECUTED"] = counts.get("SUCCESS_EXECUTED", 0) + 1
                    changed = True
                    
        if stage08_ok:
            for stage in summary.get("stage_results", []):
                if stage.get("stage_no") == 8 and stage.get("stage_status") == "FAILED":
                    stage["stage_status"] = "SUCCESS_EXECUTED"
                    counts["FAILED"] = max(0, counts.get("FAILED", 0) - 1)
                    counts["SUCCESS_EXECUTED"] = counts.get("SUCCESS_EXECUTED", 0) + 1
                    changed = True

        if changed:
            data["stage_status_counts"] = counts
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            return True

    except Exception as e:
        print(f"[POST-FIX ERROR] 修正 summary json 失败: {e}")
        
    return False


def _repair_orchestrator_outputs(reports_dir: Path, summary: Dict) -> Dict:
    stage01_files = [
        reports_dir / "daily_candidates_all.csv",
        reports_dir / "daily_candidates_top20.csv"
    ]
    stage01_ok = all(p.exists() and p.stat().st_size > 50 for p in stage01_files)

    stage08_files = [
        reports_dir / "daily_close_review.csv",
        reports_dir / "daily_close_positions.csv"
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

    changed = False

    if summary and "stage_results" in summary:
        for stage in summary["stage_results"]:
            if stage.get("stage_no") == 1 and stage.get("stage_status") == "FAILED" and stage01_ok:
                stage["stage_status"] = "SUCCESS_EXECUTED"
                changed = True
            elif stage.get("stage_no") == 8 and stage.get("stage_status") == "FAILED" and stage08_ok:
                stage["stage_status"] = "SUCCESS_EXECUTED"
                changed = True

        if changed:
            counts = summary.get("stage_status_counts", {})
            failed_count = sum(1 for s in summary["stage_results"] if s.get("stage_status") == "FAILED")
            success_count = sum(1 for s in summary["stage_results"] if str(s.get("stage_status", "")).startswith("SUCCESS"))
            
            counts["FAILED"] = failed_count
            counts["SUCCESS_EXECUTED"] = success_count
            
            summary["overall_status"] = "SUCCESS" if failed_count == 0 else "FAILED"
            
            if failed_count == 0 and summary.get("acceptance_status") == "REJECTED_FAILED_STAGE":
                target_status = "PASS_REUSE_MODE" if counts.get("SUCCESS_REUSED", 0) > 0 else "PASS_REALTIME_MODE"
                summary["acceptance_status"] = target_status
                
            if failed_count == 0 and summary.get("run_mode_label") == "FAILED_ORCHESTRATION":
                target_mode_label = "STABLE_DISPATCH_REUSE" if counts.get("SUCCESS_REUSED", 0) > 0 else "FULL_REALTIME_RECOMPUTE"
                summary["run_mode_label"] = target_mode_label
                summary["production_mode_label"] = target_mode_label

    csv_changed = _patch_reuse_audit_csv(reports_dir, stage01_ok=stage01_ok, stage08_ok=stage08_ok)
    status_changed = _patch_stage_status_csv(reports_dir, stage01_ok=stage01_ok, stage08_ok=stage08_ok)
    json_changed = _patch_summary_json(reports_dir, stage01_ok=stage01_ok, stage08_ok=stage08_ok, summary=summary)

    changed = changed or csv_changed or status_changed or json_changed

    return {
        "changed": changed,
        "stage01_fixed": stage01_ok,
        "stage08_fixed": stage08_ok
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
