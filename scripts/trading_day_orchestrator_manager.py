# -*- coding: utf-8 -*-
"""
Created on Fri Mar 20 14:21:36 2026

@author: DELL
"""

# -*- coding: utf-8 -*-
from __future__ import annotations

import glob
import importlib
import inspect
import json
import os
import sys
import traceback
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import pandas as pd

from core.post_run_acceptance_manager import PostRunAcceptanceManager
from core.reuse_control_manager import ReuseControlManager
from core.stage_status import build_stage_status_counts, normalize_stage_status
from stage_entry_runner import run_stage


class TradingDayOrchestratorManager:
    """
    交易日主控编排层 (v14.1 终极透视版)
    """

    def __init__(
        self,
        base_dir: str = r"C:\quant_system",
        enable_replay_validation: Optional[bool] = None,
        fail_fast: bool = False,
        strict_realtime_core: bool = False,
        reuse_violation_action: str = "warn",
    ) -> None:
        self.base_dir = str(base_dir)
        self.base_path = Path(base_dir)
        self.reports_dir = self.base_path / "reports"
        self.reports_dir.mkdir(parents=True, exist_ok=True)

        self.fail_fast = bool(fail_fast)
        self.strict_realtime_core = bool(strict_realtime_core)
        self.reuse_violation_action = str(reuse_violation_action).strip().lower()
        if self.reuse_violation_action not in {"warn", "reject"}:
            self.reuse_violation_action = "warn"

        if enable_replay_validation is None:
            enable_replay_validation = self._detect_replay_validation_enabled()
        self.enable_replay_validation = bool(enable_replay_validation)

        self.reuse_control_manager = ReuseControlManager(
            base_dir=self.base_dir,
            strict_realtime_core=self.strict_realtime_core,
            reuse_violation_action=self.reuse_violation_action,
        )

    def run(self, trading_date: str) -> Dict[str, Any]:
        started_at = datetime.now()

        print("============================================================")
        print("交易日主控开始执行")
        print(f"目标交易日: {trading_date}")
        print(f"基准目录  : {self.base_dir}")
        print("入口类型  : method")
        print("调用入口  : trading_day_orchestrator_manager.TradingDayOrchestratorManager.run")
        print(f"严格核心实时: {self.strict_realtime_core}")
        print(f"复用违规动作: {self.reuse_violation_action}")
        print("============================================================")

        stage_results: List[Dict[str, Any]] = []
        stage_definitions = self._build_stage_definitions(trading_date=trading_date)

        for stage in stage_definitions:
            result = run_stage(
                stage_no=stage["stage_no"],
                stage_name=stage["stage_name"],
                entry_type=stage["entry_type"],
                entry_target=stage["entry_target"],
                entry_kwargs=stage.get("entry_kwargs", {}),
                enabled=stage.get("enabled", True),
                base_dir=self.base_dir,
            )
            result["stage_status"] = normalize_stage_status(result, result.get("stage_status"))
            stage_results.append(result)

            print(
                f"[Stage {result.get('stage_no'):02d}] "
                f"{result.get('stage_name')} -> {result.get('stage_status')}"
            )

            if self.fail_fast and result.get("stage_status") == "FAILED":
                print("检测到失败阶段，fail_fast=True，提前终止后续阶段执行。")
                break

        acceptance_manager = PostRunAcceptanceManager(base_dir=self.base_dir)
        acceptance_payload = acceptance_manager.run(
            trading_date=trading_date,
            stage_results=stage_results,
        )
        stage15_result = acceptance_payload["stage15_result"]
        stage_results.append(stage15_result)

        reuse_audit_payload = self.reuse_control_manager.audit(
            trading_date=trading_date,
            stage_results=stage_results,
        )
        stage16_result = reuse_audit_payload["stage16_result"]
        stage_results.append(stage16_result)

        for item in stage_results:
            item["stage_status"] = normalize_stage_status(item, item.get("stage_status"))

        stage_status_counts = build_stage_status_counts(stage_results)

        summary: Dict[str, Any] = {
            "trading_date": trading_date,
            "started_at": started_at.strftime("%Y-%m-%d %H:%M:%S"),
            "ended_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "stage_results": stage_results,
            "stage_status_counts": stage_status_counts,
            "acceptance_status": acceptance_payload["acceptance_status"],
            "run_mode_label": acceptance_payload["run_mode_label"],
            "acceptance_analysis": acceptance_payload["acceptance_analysis"],
            "acceptance_output_json_path": acceptance_payload["stage15_result"]["output_json_path"],
            "acceptance_output_summary_path": acceptance_payload["stage15_result"]["output_summary_path"],
            "stage_status_csv_path": acceptance_payload["stage15_result"]["output_stage_status_csv_path"],
            "reuse_audit_status": reuse_audit_payload["reuse_audit_status"],
            "production_mode_label": reuse_audit_payload["production_mode_label"],
            "reuse_metrics": reuse_audit_payload["reuse_metrics"],
            "reuse_audit_output_csv_path": reuse_audit_payload["output_csv_path"],
            "reuse_audit_output_json_path": reuse_audit_payload["stage16_result"]["output_json_path"],
            "reuse_audit_output_summary_path": reuse_audit_payload["stage16_result"]["output_summary_path"],
            "strict_realtime_core": self.strict_realtime_core,
            "reuse_violation_action": self.reuse_violation_action,
        }

        summary["success"] = (
            stage_status_counts.get("SUCCESS_EXECUTED", 0)
            + stage_status_counts.get("SUCCESS_REUSED", 0)
            + stage_status_counts.get("SUCCESS_REPAIRED", 0)
        )
        summary["failed"] = stage_status_counts.get("FAILED", 0)
        summary["skipped"] = stage_status_counts.get("SKIPPED", 0)
        summary["overall_status"] = "SUCCESS" if summary["failed"] == 0 else "FAILED"

        self._write_orchestrator_summary(summary)

        print("============================================================")
        print("交易日主控执行完成")
        print(f"目标交易日: {trading_date}")
        print(f"运行阶段数: {len(stage_results)}")
        print(f"SUCCESS_EXECUTED : {stage_status_counts.get('SUCCESS_EXECUTED', 0)}")
        print(f"SUCCESS_REUSED   : {stage_status_counts.get('SUCCESS_REUSED', 0)}")
        print(f"SUCCESS_REPAIRED : {stage_status_counts.get('SUCCESS_REPAIRED', 0)}")
        print(f"FAILED           : {stage_status_counts.get('FAILED', 0)}")
        print(f"SKIPPED          : {stage_status_counts.get('SKIPPED', 0)}")
        print(f"总体状态         : {summary.get('overall_status')}")
        print(f"验收状态         : {summary.get('acceptance_status')}")
        print(f"运行模式         : {summary.get('run_mode_label')}")
        print(f"复用审计状态     : {summary.get('reuse_audit_status')}")
        print(f"生产模式标识     : {summary.get('production_mode_label')}")
        print("------------------------------------------------------------")
        print(f"是否存在失败阶段       : {summary['acceptance_analysis']['has_failed_stage']}")
        print(f"是否存在复用阶段       : {summary['acceptance_analysis']['has_reused_stage']}")
        print(f"是否存在伪委托未成交   : {summary['acceptance_analysis']['has_pseudo_unfilled_order']}")
        print(f"闭环是否异常放大       : {summary['acceptance_analysis']['close_loop_abnormal_amplified']}")
        print(f"当前是否仅剩执行滑点类 : {summary['acceptance_analysis']['only_execution_slippage_left']}")
        print("------------------------------------------------------------")
        print(f"核心阶段被复用数       : {summary['reuse_metrics']['core_reused_count']}")
        print(f"核心阶段拒绝数         : {summary['reuse_metrics'].get('core_policy_rejected_count', 0)}")
        print(f"核心阶段实时覆盖率     : {summary['reuse_metrics']['core_realtime_coverage_ratio']}")
        print(f"验收摘要文件           : {summary.get('acceptance_output_summary_path')}")
        print(f"复用审计摘要文件       : {summary.get('reuse_audit_output_summary_path')}")
        print(f"复用审计CSV            : {summary.get('reuse_audit_output_csv_path')}")
        print("============================================================")

        return summary

    def _build_stage_definitions(self, trading_date: str) -> List[Dict[str, Any]]:
        init_kwargs = {
            "base_dir": self.base_dir,
            "enable_replay_validation": self.enable_replay_validation,
            "fail_fast": self.fail_fast,
            "strict_realtime_core": self.strict_realtime_core,
            "reuse_violation_action": self.reuse_violation_action,
        }
        return [
            {
                "stage_no": 0,
                "stage_name": "行情快照补数层",
                "entry_type": "method",
                "entry_target": "trading_day_orchestrator_manager.TradingDayOrchestratorManager._stage_market_snapshot",
                "entry_kwargs": {"__init_kwargs__": init_kwargs, "trading_date": trading_date},
                "enabled": True,
            },
            {
                "stage_no": 1,
                "stage_name": "每日候选股生成层",
                "entry_type": "method",
                "entry_target": "trading_day_orchestrator_manager.TradingDayOrchestratorManager._stage_daily_candidates",
                "entry_kwargs": {"__init_kwargs__": init_kwargs, "trading_date": trading_date},
                "enabled": True,
            },
            {
                "stage_no": 2,
                "stage_name": "每日交易计划生成层",
                "entry_type": "method",
                "entry_target": "trading_day_orchestrator_manager.TradingDayOrchestratorManager._stage_trade_plan",
                "entry_kwargs": {"__init_kwargs__": init_kwargs, "trading_date": trading_date},
                "enabled": True,
            },
            {
                "stage_no": 3,
                "stage_name": "组合层",
                "entry_type": "method",
                "entry_target": "trading_day_orchestrator_manager.TradingDayOrchestratorManager._stage_portfolio_plan",
                "entry_kwargs": {"__init_kwargs__": init_kwargs, "trading_date": trading_date},
                "enabled": True,
            },
            {
                "stage_no": 4,
                "stage_name": "组合风控复核层",
                "entry_type": "method",
                "entry_target": "trading_day_orchestrator_manager.TradingDayOrchestratorManager._stage_portfolio_risk_review",
                "entry_kwargs": {"__init_kwargs__": init_kwargs, "trading_date": trading_date},
                "enabled": True,
            },
            {
                "stage_no": 5,
                "stage_name": "组合执行优先级层",
                "entry_type": "method",
                "entry_target": "trading_day_orchestrator_manager.TradingDayOrchestratorManager._stage_execution_priority",
                "entry_kwargs": {"__init_kwargs__": init_kwargs, "trading_date": trading_date},
                "enabled": True,
            },
            {
                "stage_no": 6,
                "stage_name": "开盘动态执行层",
                "entry_type": "method",
                "entry_target": "trading_day_orchestrator_manager.TradingDayOrchestratorManager._stage_open_execution",
                "entry_kwargs": {"__init_kwargs__": init_kwargs, "trading_date": trading_date},
                "enabled": True,
            },
            {
                "stage_no": 7,
                "stage_name": "盘中二次确认层",
                "entry_type": "method",
                "entry_target": "trading_day_orchestrator_manager.TradingDayOrchestratorManager._stage_intraday_recheck",
                "entry_kwargs": {"__init_kwargs__": init_kwargs, "trading_date": trading_date},
                "enabled": True,
            },
            {
                "stage_no": 8,
                "stage_name": "收盘复盘层",
                "entry_type": "method",
                "entry_target": "trading_day_orchestrator_manager.TradingDayOrchestratorManager._stage_close_review",
                "entry_kwargs": {"__init_kwargs__": init_kwargs, "trading_date": trading_date},
                "enabled": True,
            },
            {
                "stage_no": 9,
                "stage_name": "次日持仓续管层",
                "entry_type": "method",
                "entry_target": "trading_day_orchestrator_manager.TradingDayOrchestratorManager._stage_next_day_management",
                "entry_kwargs": {"__init_kwargs__": init_kwargs, "trading_date": trading_date},
                "enabled": True,
            },
            {
                "stage_no": 10,
                "stage_name": "真实交易流水对账层",
                "entry_type": "method",
                "entry_target": "trading_day_orchestrator_manager.TradingDayOrchestratorManager._stage_trade_reconciliation",
                "entry_kwargs": {"__init_kwargs__": init_kwargs, "trading_date": trading_date},
                "enabled": True,
            },
            {
                "stage_no": 11,
                "stage_name": "对账异常归因层",
                "entry_type": "method",
                "entry_target": "trading_day_orchestrator_manager.TradingDayOrchestratorManager._stage_reconciliation_attribution",
                "entry_kwargs": {"__init_kwargs__": init_kwargs, "trading_date": trading_date},
                "enabled": True,
            },
            {
                "stage_no": 12,
                "stage_name": "异常闭环复盘层",
                "entry_type": "method",
                "entry_target": "trading_day_orchestrator_manager.TradingDayOrchestratorManager._stage_reconciliation_review",
                "entry_kwargs": {"__init_kwargs__": init_kwargs, "trading_date": trading_date},
                "enabled": True,
            },
            {
                "stage_no": 13,
                "stage_name": "异常注入回放验证层",
                "entry_type": "method",
                "entry_target": "trading_day_orchestrator_manager.TradingDayOrchestratorManager._stage_replay_validation",
                "entry_kwargs": {"__init_kwargs__": init_kwargs, "trading_date": trading_date},
                "enabled": self.enable_replay_validation,
            },
        ]

    def _stage_market_snapshot(self, trading_date: str) -> Dict[str, Any]:
        return self._execute_stage_with_fallback(
            stage_no=0,
            stage_name="行情快照补数层",
            trading_date=trading_date,
            candidate_entries=[("function", "generate_market_signal_snapshot.run")],
            required_patterns=["market_signal_snapshot.csv"],
        )

    def _stage_daily_candidates(self, trading_date: str) -> Dict[str, Any]:
        return self._execute_stage_with_fallback(
            stage_no=1,
            stage_name="每日候选股生成层",
            trading_date=trading_date,
            candidate_entries=[("function", "generate_daily_candidates.run")],
            required_patterns=["daily_candidates_top20.csv", "daily_candidates_all.csv"],
        )

    def _stage_trade_plan(self, trading_date: str) -> Dict[str, Any]:
        return self._execute_stage_with_fallback(
            stage_no=2,
            stage_name="每日交易计划生成层",
            trading_date=trading_date,
            candidate_entries=[("function", "generate_trade_plan.run")],
            required_patterns=["daily_trade_plan_top10.csv", "daily_trade_plan_all.csv"],
        )

    def _stage_portfolio_plan(self, trading_date: str) -> Dict[str, Any]:
        return self._execute_stage_with_fallback(
            stage_no=3,
            stage_name="组合层",
            trading_date=trading_date,
            candidate_entries=[
                ("method", "core.portfolio_builder.PortfolioBuilder.run"),
                ("function", "generate_portfolio_plan.run"),
            ],
            required_patterns=["daily_portfolio_plan.csv", "daily_portfolio_summary.txt"],
        )

    def _stage_portfolio_risk_review(self, trading_date: str) -> Dict[str, Any]:
        return self._execute_stage_with_fallback(
            stage_no=4,
            stage_name="组合风控复核层",
            trading_date=trading_date,
            candidate_entries=[
                ("method", "core.portfolio_builder.PortfolioBuilder.run_risk_review"),
                ("function", "generate_portfolio_risk_review.run"),
            ],
            required_patterns=["daily_portfolio_plan_risk_checked.csv"],
        )

    def _stage_execution_priority(self, trading_date: str) -> Dict[str, Any]:
        return self._execute_stage_with_fallback(
            stage_no=5,
            stage_name="组合执行优先级层",
            trading_date=trading_date,
            candidate_entries=[
                ("method", "core.portfolio_executor.PortfolioExecutor.run"),
                ("function", "generate_execution_plan.run"),
            ],
            required_patterns=["daily_execution_plan.csv"],
        )

    def _stage_open_execution(self, trading_date: str) -> Dict[str, Any]:
        return self._execute_stage_with_fallback(
            stage_no=6,
            stage_name="开盘动态执行层",
            trading_date=trading_date,
            candidate_entries=[
                ("method", "core.open_execution_manager.OpenExecutionManager.run"),
                ("function", "generate_open_execution.run"),
            ],
            required_patterns=["daily_open_execution_decision.csv"],
        )

    def _stage_intraday_recheck(self, trading_date: str) -> Dict[str, Any]:
        return self._execute_stage_with_fallback(
            stage_no=7,
            stage_name="盘中二次确认层",
            trading_date=trading_date,
            candidate_entries=[
                ("method", "core.intraday_recheck_manager.IntradayRecheckManager.run"),
                ("function", "generate_intraday_recheck.run"),
            ],
            required_patterns=["daily_intraday_recheck_decision.csv"],
        )

    def _stage_close_review(self, trading_date: str) -> Dict[str, Any]:
        return self._execute_stage_with_fallback(
            stage_no=8,
            stage_name="收盘复盘层",
            trading_date=trading_date,
            candidate_entries=[
                ("method", "core.close_review_manager.CloseReviewManager.run"),
                ("function", "generate_close_review.run"),
            ],
            required_patterns=["daily_close_review.csv"],
        )

    def _stage_next_day_management(self, trading_date: str) -> Dict[str, Any]:
        return self._execute_stage_with_fallback(
            stage_no=9,
            stage_name="次日持仓续管层",
            trading_date=trading_date,
            candidate_entries=[
                ("method", "core.position_manager.PositionManager.run"),
                ("function", "generate_next_day_management.run"),
            ],
            required_patterns=["daily_next_day_management.csv"],
        )

    def _stage_trade_reconciliation(self, trading_date: str) -> Dict[str, Any]:
        return self._execute_stage_with_fallback(
            stage_no=10,
            stage_name="真实交易流水对账层",
            trading_date=trading_date,
            candidate_entries=[
                ("method", "core.trade_reconciliation_manager.TradeReconciliationManager.run"),
                ("function", "generate_trade_reconciliation.run"),
            ],
            required_patterns=["daily_trade_reconciliation_detail.csv"],
            repair_hook=self._repair_trade_reconciliation_outputs,
        )

    def _stage_reconciliation_attribution(self, trading_date: str) -> Dict[str, Any]:
        return self._execute_stage_with_fallback(
            stage_no=11,
            stage_name="对账异常归因层",
            trading_date=trading_date,
            candidate_entries=[
                ("method", "reconciliation_attribution_manager.ReconciliationAttributionManager.run"),
                ("function", "generate_reconciliation_attribution.run"),
            ],
            required_patterns=["*reconciliation*attribution*.csv"],
        )

    def _stage_reconciliation_review(self, trading_date: str) -> Dict[str, Any]:
        return self._execute_stage_with_fallback(
            stage_no=12,
            stage_name="异常闭环复盘层",
            trading_date=trading_date,
            candidate_entries=[
                ("method", "reconciliation_review_manager.ReconciliationReviewManager.run"),
                ("function", "generate_reconciliation_review.run"),
            ],
            required_patterns=["*reconciliation*review*.csv"],
        )

    def _stage_replay_validation(self, trading_date: str) -> Dict[str, Any]:
        return self._execute_stage_with_fallback(
            stage_no=13,
            stage_name="异常注入回放验证层",
            trading_date=trading_date,
            candidate_entries=[
                ("method", "reconciliation_replay_validation_manager.ReconciliationReplayValidationManager.run"),
                ("function", "generate_reconciliation_replay_validation.run"),
            ],
            required_patterns=["*replay*validation*.csv"],
            require_all_patterns=False,
        )

    # ============================================================
    # 阶段统一执行框架
    # ============================================================
    def _execute_stage_with_fallback(
        self,
        stage_no: int,
        stage_name: str,
        trading_date: str,
        candidate_entries: Sequence[Tuple[str, str]],
        required_patterns: Sequence[str],
        require_all_patterns: bool = True,
        repair_hook: Optional[Callable[[], List[str]]] = None,
    ) -> Dict[str, Any]:
        errors: List[str] = []
        executed_result: Optional[Dict[str, Any]] = None

        for entry_type, entry_target in candidate_entries:
            try:
                raw = self._invoke_entry_candidate(
                    entry_type=entry_type,
                    entry_target=entry_target,
                    trading_date=trading_date,
                )
                executed_result = self._coerce_result_dict(raw)
                break
            except Exception as exc:
                err_msg = f"{entry_target}: {exc}"
                errors.append(err_msg)
                if not isinstance(exc, ModuleNotFoundError):
                    errors.append(traceback.format_exc())

        repair_actions: List[str] = []
        if repair_hook is not None:
            try:
                repair_actions = repair_hook()
            except Exception as exc:
                errors.append(f"repair_hook: {exc}")

        if executed_result is not None:
            result = {
                "stage_status": "SUCCESS_EXECUTED",
                "success": True,
                "executed": True,
                "reused": False,
                "repaired": False,
                "repair_actions": repair_actions,
                "message": f"{stage_name}执行成功",
            }
            result.update(executed_result)
            if repair_actions:
                result["stage_status"] = "SUCCESS_REPAIRED"
                result["repaired"] = True
                result["message"] = f"{stage_name}执行成功，并完成自动修正"
            return result

        matched_files = self._collect_artifacts(required_patterns, require_all_patterns=require_all_patterns)
        if matched_files:
            reuse_decision = self.reuse_control_manager.evaluate_reuse(
                stage_no=stage_no,
                stage_name=stage_name,
                artifact_files=matched_files,
            )

            if not reuse_decision["reuse_allowed"] and reuse_decision["policy_action"] == "reject":
                # 强透视：命中严格复用拦截，且底层报错，强制打印！
                print(f"\n    [!] {stage_name} 实时执行失败，且命中严格模式拒绝复用！底层报错详情：")
                for err in errors:
                    print(f"        {err}")
                print("\n")
                return {
                    "stage_status": "FAILED",
                    "success": False,
                    "policy_rejected": True,
                    "policy_level": reuse_decision["policy_level"],
                    "policy_message": reuse_decision["policy_message"],
                    "artifact_files": matched_files,
                    "message": f"{stage_name}命中核心阶段禁止复用规则，已拒绝",
                    "attempt_errors": errors,
                }

            result = {
                "stage_status": "SUCCESS_REUSED",
                "success": True,
                "reused": True,
                "artifact_files": matched_files,
                "message": f"{stage_name}复用既有工件成功",
                "attempt_errors": errors,
            }
            if repair_actions:
                result["stage_status"] = "SUCCESS_REPAIRED"
                result["repaired"] = True
                result["repair_actions"] = repair_actions
            return result

        print(f"\n    [!] {stage_name} 实时执行崩溃，且无有效工件可复用！底层报错详情：")
        for err in errors:
            print(f"        {err}")
        print("\n")

        return {
            "stage_status": "FAILED",
            "success": False,
            "message": f"{stage_name}执行失败，且无可复用工件",
            "attempt_errors": errors,
        }

    # ============================================================
    # 动态调用与沙箱注入
    # ============================================================
    def _invoke_entry_candidate(
        self,
        entry_type: str,
        entry_target: str,
        trading_date: str,
    ) -> Any:
        entry_type = str(entry_type).strip().lower()
        base_call_kwargs = self._build_candidate_call_kwargs(trading_date=trading_date)

        if entry_type == "function":
            module_name, func_name = entry_target.rsplit(".", 1)
            module = importlib.import_module(module_name)
            func = getattr(module, func_name)
            with self._isolated_sys_argv(trading_date):
                return self._call_with_flexible_kwargs(func, base_call_kwargs)

        if entry_type == "method":
            parts = entry_target.split(".")
            module_name = ".".join(parts[:-2])
            class_name = parts[-2]
            method_name = parts[-1]

            module = importlib.import_module(module_name)
            cls = getattr(module, class_name)
            init_kwargs = self._filter_kwargs_for_callable(cls, {"base_dir": self.base_dir})
            instance = cls(**init_kwargs) if init_kwargs is not None else cls()
            method = getattr(instance, method_name)
            with self._isolated_sys_argv(trading_date):
                return self._call_with_flexible_kwargs(method, base_call_kwargs)

        raise ValueError(f"不支持的 entry_type: {entry_type}")

    @contextmanager
    def _isolated_sys_argv(self, trading_date: str):
        original_argv = list(sys.argv)
        try:
            sys.argv = [
                original_argv[0] if original_argv else "orchestrator_stage_call",
                "--trading-date", trading_date,
            ]
            yield
        finally:
            sys.argv = original_argv

    def _build_candidate_call_kwargs(self, trading_date: str) -> List[Dict[str, Any]]:
        return [
            {"trading_date": trading_date, "base_dir": self.base_dir},
            {"trading_date": trading_date},
            {"base_dir": self.base_dir},
            {},
        ]

    def _call_with_flexible_kwargs(
        self,
        func: Callable[..., Any],
        candidate_kwargs_list: List[Dict[str, Any]],
    ) -> Any:
        last_exc: Optional[Exception] = None
        for raw_kwargs in candidate_kwargs_list:
            try:
                filtered = self._filter_kwargs_for_callable(func, raw_kwargs)
                if filtered is None:
                    filtered = {}
                return func(**filtered)
            except TypeError as exc:
                last_exc = exc
                continue
            except Exception:
                raise
        if last_exc is not None:
            raise last_exc
        return func()

    def _filter_kwargs_for_callable(
        self,
        func: Callable[..., Any],
        kwargs: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        try:
            sig = inspect.signature(func)
        except Exception:
            return kwargs

        params = sig.parameters
        if any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values()):
            return kwargs

        accepted = {}
        for name in params.keys():
            if name in ("self", "cls"):
                continue
            if name in kwargs:
                accepted[name] = kwargs[name]
        return accepted

    def _coerce_result_dict(self, raw_result: Any) -> Dict[str, Any]:
        if raw_result is None:
            return {}
        if isinstance(raw_result, dict):
            return raw_result
        if isinstance(raw_result, bool):
            return {"success": raw_result}
        return {"result": raw_result}

    # ============================================================
    # 其他辅助功能
    # ============================================================
    def _collect_artifacts(self, patterns: Sequence[str], require_all_patterns: bool = True) -> List[str]:
        matched_files: List[str] = []
        for pattern in patterns:
            files = sorted(glob.glob(str(self.reports_dir / pattern)))
            if require_all_patterns and not files:
                return []
            matched_files.extend(files)
        return sorted(set(matched_files))

    def _repair_trade_reconciliation_outputs(self) -> List[str]:
        return []

    def _detect_replay_validation_enabled(self) -> bool:
        env_value = os.getenv("ORCHESTRATOR_ENABLE_REPLAY_VALIDATION", "").strip().lower()
        if env_value in {"1", "true", "yes", "y", "on"}:
            return True
        return False

    def _write_orchestrator_summary(self, summary: Dict[str, Any]) -> None:
        json_path = self.reports_dir / "daily_orchestrator_summary.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)