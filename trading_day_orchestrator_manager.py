# -*- coding: utf-8 -*-
from __future__ import annotations

import glob
import importlib
import json
import sys
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from core.post_run_acceptance_manager import PostRunAcceptanceManager
from core.reuse_control_manager import ReuseControlManager
from core.stage_status import build_stage_status_counts, is_data_pending_stage, normalize_stage_status
from stage_entry_runner import run_stage


class TradingDayOrchestratorManager:
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
        print(f"严格核心实时: {self.strict_realtime_core}")
        print("============================================================")

        stage_results: List[Dict[str, Any]] = []
        stage_definitions = self._build_stage_definitions(trading_date=trading_date)
        blocking_stage: Optional[Dict[str, Any]] = None

        for stage in stage_definitions:
            if blocking_stage is not None and int(stage["stage_no"]) > int(blocking_stage["stage_no"]):
                result = self._build_blocked_stage_result(stage, blocking_stage)
            else:
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

            print(f"[Stage {int(result.get('stage_no', -1)):02d}] {result.get('stage_name')} -> {result.get('stage_status')}")
            if result.get("error"):
                print(f"    [!] 内部错误原因: {result['error']}")
            if result.get("error_message"):
                print(f"    [!] 系统报错拦截: {result['error_message']}")

            if blocking_stage is None and is_data_pending_stage(str(result.get("stage_status", ""))):
                blocking_stage = {
                    "stage_no": result.get("stage_no"),
                    "stage_name": result.get("stage_name"),
                    "stage_status": result.get("stage_status"),
                    "error": result.get("error") or result.get("error_message") or "",
                }

            if self.fail_fast and result.get("stage_status") == "FAILED":
                print("检测到失败阶段，fail_fast=True，提前终止后续阶段执行。")
                break

        acceptance_manager = PostRunAcceptanceManager(base_dir=self.base_dir)
        acceptance_payload = acceptance_manager.run(trading_date=trading_date, stage_results=stage_results)
        stage_results.append(acceptance_payload["stage15_result"])

        reuse_audit_payload = self.reuse_control_manager.audit(trading_date=trading_date, stage_results=stage_results)
        stage_results.append(reuse_audit_payload["stage16_result"])

        for item in stage_results:
            item["stage_status"] = normalize_stage_status(item, item.get("stage_status"))

        business_stage_results = [
            item for item in stage_results if isinstance(item.get("stage_no"), int) and 0 <= item.get("stage_no") <= 13
        ]
        post_run_stage_results = [
            item for item in stage_results if not (isinstance(item.get("stage_no"), int) and 0 <= item.get("stage_no") <= 13)
        ]

        stage_status_counts = build_stage_status_counts(business_stage_results)
        summary: Dict[str, Any] = {
            "trading_date": trading_date,
            "started_at": started_at.strftime("%Y-%m-%d %H:%M:%S"),
            "ended_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "stage_results": business_stage_results,
            "post_run_stage_results": post_run_stage_results,
            "stage_status_counts": stage_status_counts,
            "overall_status": "SUCCESS" if stage_status_counts.get("FAILED", 0) == 0 else "FAILED",
            "acceptance_status": acceptance_payload["acceptance_status"],
            "run_mode_label": acceptance_payload["run_mode_label"],
            "acceptance_analysis": acceptance_payload["acceptance_analysis"],
            "reuse_audit_status": reuse_audit_payload["reuse_audit_status"],
            "production_mode_label": reuse_audit_payload["production_mode_label"],
            "reuse_metrics": reuse_audit_payload["reuse_metrics"],
        }
        self._write_orchestrator_summary(summary)

        print("============================================================")
        print("交易日主控执行完成")
        print(f"总体状态: {summary['overall_status']}")
        print(f"验收状态: {summary['acceptance_status']}")
        print(f"生产模式: {summary['production_mode_label']}")
        print(f"核心阶段拒绝数: {summary['reuse_metrics'].get('core_policy_rejected_count', 0)}")
        print(f"核心阶段实时覆盖率: {summary['reuse_metrics'].get('core_realtime_coverage_ratio', 0.0)}")
        print("============================================================")

        return summary

    def _write_orchestrator_summary(self, summary: Dict[str, Any]) -> None:
        json_path = self.reports_dir / "daily_orchestrator_summary.json"
        txt_path = self.reports_dir / "daily_orchestrator_summary.txt"

        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2, default=str)

        counts = summary.get("stage_status_counts", {})
        analysis = summary.get("acceptance_analysis", {})
        reuse_metrics = summary.get("reuse_metrics", {})
        count_keys = [
            "SUCCESS_EXECUTED",
            "SUCCESS_REUSED",
            "SUCCESS_REPAIRED",
            "FAILED",
            "SKIPPED",
            "NON_TRADING_DAY",
            "WAITING_MARKET_DATA",
            "DATA_STALE",
        ]

        lines = [
            "============================================================",
            "交易日主控摘要",
            f"目标交易日: {summary.get('trading_date')}",
            f"开始时间: {summary.get('started_at', '')}",
            f"结束时间: {summary.get('ended_at', '')}",
            f"总体状态: {summary.get('overall_status')}",
            f"验收状态: {summary.get('acceptance_status')}",
            f"运行模式: {summary.get('run_mode_label')}",
            f"复用审计状态: {summary.get('reuse_audit_status')}",
            f"生产模式标识: {summary.get('production_mode_label')}",
            "------------------------------------------------------------",
            "阶段状态统计：",
        ]
        lines.extend([f"  {key:<20}: {counts.get(key, 0)}" for key in count_keys if counts.get(key, 0) or key in {"FAILED", "SUCCESS_EXECUTED"}])
        lines.extend(
            [
                "------------------------------------------------------------",
                "自动验收结论：",
                f"  是否存在失败阶段       : {analysis.get('has_failed_stage')}",
                f"  是否存在复用阶段       : {analysis.get('has_reused_stage')}",
                f"  是否存在伪委托未成交   : {analysis.get('has_pseudo_unfilled_order')}",
                f"  闭环是否异常放大       : {analysis.get('close_loop_abnormal_amplified')}",
                f"  当前是否仅剩执行滑点类 : {analysis.get('only_execution_slippage_left')}",
                f"  对账异常条数           : {analysis.get('anomaly_count')}",
                f"  闭环复盘记录数         : {analysis.get('review_count')}",
                "------------------------------------------------------------",
                "复用审计结论：",
                f"  核心阶段复用数         : {reuse_metrics.get('core_reused_count')}",
                f"  核心阶段拒绝数         : {reuse_metrics.get('core_policy_rejected_count')}",
                f"  核心阶段失败数         : {reuse_metrics.get('core_failed_count')}",
                f"  核心阶段跳过数         : {reuse_metrics.get('core_skipped_count')}",
                f"  核心阶段待数数         : {reuse_metrics.get('core_pending_count')}",
                f"  核心阶段实时覆盖率     : {reuse_metrics.get('core_realtime_coverage_ratio')}",
                "------------------------------------------------------------",
                "阶段明细：",
            ]
        )

        for item in summary.get("stage_results", []):
            stage_no = item.get("stage_no")
            stage_no_text = f"{stage_no:02d}" if isinstance(stage_no, int) else str(stage_no)
            lines.append(f"  [{stage_no_text}] {item.get('stage_name')} -> {item.get('stage_status')}")

        if summary.get("post_run_stage_results"):
            lines.extend(["------------------------------------------------------------", "后处理合成阶段："])
            for item in summary.get("post_run_stage_results", []):
                stage_no = item.get("stage_no")
                stage_no_text = f"{stage_no:02d}" if isinstance(stage_no, int) else str(stage_no)
                lines.append(f"  [{stage_no_text}] {item.get('stage_name')} -> {item.get('stage_status')}")

        lines.append("============================================================")
        with open(txt_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")

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
            },
            {
                "stage_no": 1,
                "stage_name": "每日候选股生成层",
                "entry_type": "method",
                "entry_target": "trading_day_orchestrator_manager.TradingDayOrchestratorManager._stage_daily_candidates",
                "entry_kwargs": {"__init_kwargs__": init_kwargs, "trading_date": trading_date},
            },
            {
                "stage_no": 2,
                "stage_name": "每日交易计划生成层",
                "entry_type": "method",
                "entry_target": "trading_day_orchestrator_manager.TradingDayOrchestratorManager._stage_trade_plan",
                "entry_kwargs": {"__init_kwargs__": init_kwargs, "trading_date": trading_date},
            },
            {
                "stage_no": 3,
                "stage_name": "组合层",
                "entry_type": "method",
                "entry_target": "trading_day_orchestrator_manager.TradingDayOrchestratorManager._stage_portfolio_plan",
                "entry_kwargs": {"__init_kwargs__": init_kwargs, "trading_date": trading_date},
            },
            {
                "stage_no": 4,
                "stage_name": "组合风控复核层",
                "entry_type": "method",
                "entry_target": "trading_day_orchestrator_manager.TradingDayOrchestratorManager._stage_portfolio_risk_review",
                "entry_kwargs": {"__init_kwargs__": init_kwargs, "trading_date": trading_date},
            },
            {
                "stage_no": 5,
                "stage_name": "组合执行优先级层",
                "entry_type": "method",
                "entry_target": "trading_day_orchestrator_manager.TradingDayOrchestratorManager._stage_execution_priority",
                "entry_kwargs": {"__init_kwargs__": init_kwargs, "trading_date": trading_date},
            },
            {
                "stage_no": 6,
                "stage_name": "开盘动态执行层",
                "entry_type": "method",
                "entry_target": "trading_day_orchestrator_manager.TradingDayOrchestratorManager._stage_open_execution",
                "entry_kwargs": {"__init_kwargs__": init_kwargs, "trading_date": trading_date},
            },
            {
                "stage_no": 7,
                "stage_name": "盘中二次确认层",
                "entry_type": "method",
                "entry_target": "trading_day_orchestrator_manager.TradingDayOrchestratorManager._stage_intraday_recheck",
                "entry_kwargs": {"__init_kwargs__": init_kwargs, "trading_date": trading_date},
            },
            {
                "stage_no": 8,
                "stage_name": "收盘复盘层",
                "entry_type": "method",
                "entry_target": "trading_day_orchestrator_manager.TradingDayOrchestratorManager._stage_close_review",
                "entry_kwargs": {"__init_kwargs__": init_kwargs, "trading_date": trading_date},
            },
            {
                "stage_no": 9,
                "stage_name": "次日持仓续管层",
                "entry_type": "method",
                "entry_target": "trading_day_orchestrator_manager.TradingDayOrchestratorManager._stage_next_day_management",
                "entry_kwargs": {"__init_kwargs__": init_kwargs, "trading_date": trading_date},
            },
            {
                "stage_no": 10,
                "stage_name": "真实交易流水对账层",
                "entry_type": "method",
                "entry_target": "trading_day_orchestrator_manager.TradingDayOrchestratorManager._stage_trade_reconciliation",
                "entry_kwargs": {"__init_kwargs__": init_kwargs, "trading_date": trading_date},
            },
            {
                "stage_no": 11,
                "stage_name": "对账异常归因层",
                "entry_type": "method",
                "entry_target": "trading_day_orchestrator_manager.TradingDayOrchestratorManager._stage_reconciliation_attribution",
                "entry_kwargs": {"__init_kwargs__": init_kwargs, "trading_date": trading_date},
            },
            {
                "stage_no": 12,
                "stage_name": "异常闭环复盘层",
                "entry_type": "method",
                "entry_target": "trading_day_orchestrator_manager.TradingDayOrchestratorManager._stage_reconciliation_review",
                "entry_kwargs": {"__init_kwargs__": init_kwargs, "trading_date": trading_date},
            },
            {
                "stage_no": 13,
                "stage_name": "异常注入回放验证层",
                "entry_type": "method",
                "entry_target": "trading_day_orchestrator_manager.TradingDayOrchestratorManager._stage_replay_validation",
                "entry_kwargs": {"__init_kwargs__": init_kwargs, "trading_date": trading_date},
                "enabled": self.enable_replay_validation or self.strict_realtime_core,
            },
        ]

    def _stage_market_snapshot(self, trading_date: str) -> Dict[str, Any]:
        return self._execute_stage_with_fallback(0, "行情快照补数层", trading_date, [("function", "generate_market_signal_snapshot.run")], ["market_signal_snapshot.csv"])

    def _stage_daily_candidates(self, trading_date: str) -> Dict[str, Any]:
        return self._execute_stage_with_fallback(1, "每日候选股生成层", trading_date, [("function", "generate_daily_candidates.run")], ["daily_candidates_top20.csv", "daily_candidates_all.csv"])

    def _stage_trade_plan(self, trading_date: str) -> Dict[str, Any]:
        return self._execute_stage_with_fallback(2, "每日交易计划生成层", trading_date, [("function", "generate_trade_plan.run")], ["daily_trade_plan_top10.csv", "daily_trade_plan_all.csv"])

    def _stage_portfolio_plan(self, trading_date: str) -> Dict[str, Any]:
        return self._execute_stage_with_fallback(3, "组合层", trading_date, [("function", "core.portfolio_builder.build_portfolio_plan")], ["daily_portfolio_plan.csv"])

    def _stage_portfolio_risk_review(self, trading_date: str) -> Dict[str, Any]:
        return self._execute_stage_with_fallback(4, "组合风控复核层", trading_date, [("function", "generate_portfolio_risk_review.run")], ["daily_portfolio_plan_risk_checked.csv"])

    def _stage_execution_priority(self, trading_date: str) -> Dict[str, Any]:
        return self._execute_stage_with_fallback(5, "组合执行优先级层", trading_date, [("function", "generate_execution_plan.run")], ["daily_execution_plan.csv"])

    def _stage_open_execution(self, trading_date: str) -> Dict[str, Any]:
        return self._execute_stage_with_fallback(6, "开盘动态执行层", trading_date, [("function", "generate_open_execution.run")], ["daily_open_execution_decision.csv"])

    def _stage_intraday_recheck(self, trading_date: str) -> Dict[str, Any]:
        return self._execute_stage_with_fallback(7, "盘中二次确认层", trading_date, [("function", "generate_intraday_recheck.run")], ["daily_intraday_recheck_decision.csv"])

    def _stage_close_review(self, trading_date: str) -> Dict[str, Any]:
        return self._execute_stage_with_fallback(8, "收盘复盘层", trading_date, [("function", "generate_close_review.run")], ["daily_close_review.csv"])

    def _stage_next_day_management(self, trading_date: str) -> Dict[str, Any]:
        return self._execute_stage_with_fallback(9, "次日持仓续管层", trading_date, [("function", "generate_next_day_management.run")], ["daily_next_day_management.csv"])

    def _stage_trade_reconciliation(self, trading_date: str) -> Dict[str, Any]:
        return self._execute_stage_with_fallback(10, "真实交易流水对账层", trading_date, [("function", "generate_trade_reconciliation.run")], ["daily_trade_reconciliation_detail.csv"])

    def _stage_reconciliation_attribution(self, trading_date: str) -> Dict[str, Any]:
        return self._execute_stage_with_fallback(11, "对账异常归因层", trading_date, [("function", "generate_reconciliation_attribution.run")], ["*reconciliation*attribution*.csv"])

    def _stage_reconciliation_review(self, trading_date: str) -> Dict[str, Any]:
        return self._execute_stage_with_fallback(12, "异常闭环复盘层", trading_date, [("function", "generate_reconciliation_review.run")], ["*reconciliation*review*.csv"])

    def _stage_replay_validation(self, trading_date: str) -> Dict[str, Any]:
        return self._execute_stage_with_fallback(13, "异常注入回放验证层", trading_date, [("function", "generate_reconciliation_replay_validation.run")], ["*replay*validation*.csv"], require_all_patterns=False)

    def _build_blocked_stage_result(self, stage: Dict[str, Any], blocker: Dict[str, Any]) -> Dict[str, Any]:
        blocker_stage_no = int(blocker.get("stage_no", -1))
        blocker_stage_name = str(blocker.get("stage_name", ""))
        blocker_status = str(blocker.get("stage_status", ""))
        blocker_reason = str(blocker.get("error", "")).strip()
        reason = f"Stage {blocker_stage_no:02d} {blocker_stage_name} 返回 {blocker_status}"
        if blocker_reason:
            reason = f"{reason}: {blocker_reason}"
        return {
            "stage_no": stage["stage_no"],
            "stage_name": stage["stage_name"],
            "entry_type": stage["entry_type"],
            "entry_target": stage["entry_target"],
            "stage_status": blocker_status,
            "success": False,
            "blocked": True,
            "blocked_by_stage_no": blocker_stage_no,
            "blocked_by_stage_name": blocker_stage_name,
            "error": reason,
        }

    def _execute_stage_with_fallback(
        self,
        stage_no: int,
        stage_name: str,
        trading_date: str,
        candidate_entries: Sequence[Tuple[str, str]],
        required_patterns: Sequence[str],
        require_all_patterns: bool = True,
        repair_hook=None,
    ) -> Dict[str, Any]:
        errors: List[str] = []
        executed_result: Optional[Dict[str, Any]] = None

        for entry_type, entry_target in candidate_entries:
            try:
                raw = self._invoke_entry_candidate(entry_type, entry_target, trading_date)
                executed_result = raw if isinstance(raw, dict) else {"result": raw}
                break
            except Exception as exc:
                errors.append(f"{entry_target} 失败: {exc}")

        if executed_result is not None:
            normalized_status = normalize_stage_status(executed_result, executed_result.get("stage_status"))
            raw_stage_status = str(executed_result.get("stage_status", "")).strip()
            if raw_stage_status and normalized_status != "SUCCESS_EXECUTED":
                executed_result["stage_status"] = normalized_status
                return executed_result

            result = {
                "stage_status": "SUCCESS_EXECUTED",
                "success": True,
                "executed": True,
                "reused": False,
                "message": f"{stage_name}执行成功",
            }
            result.update(executed_result)
            return result

        matched_files = self._collect_artifacts(required_patterns, require_all_patterns)
        if matched_files:
            reuse_decision = self.reuse_control_manager.evaluate_reuse(stage_no, stage_name, matched_files)
            if not reuse_decision["reuse_allowed"] and reuse_decision["policy_action"] == "reject":
                return {
                    "stage_status": "FAILED",
                    "policy_rejected": True,
                    "error": f"{stage_name} 命中核心禁止复用规则，已拒绝。尝试过的入口错误: {errors}",
                }
            return {"stage_status": "SUCCESS_REUSED", "success": True, "reused": True}

        return {
            "stage_status": "FAILED",
            "success": False,
            "error": f"{stage_name} 执行崩溃且无工件。底层报错: {errors}",
        }

    def _invoke_entry_candidate(self, entry_type: str, entry_target: str, trading_date: str) -> Any:
        module_name, func_name = entry_target.rsplit(".", 1)
        module = importlib.import_module(module_name)
        func = getattr(module, func_name)
        with self._isolated_sys_argv(trading_date):
            return func(trading_date=trading_date, base_dir=self.base_dir)

    @contextmanager
    def _isolated_sys_argv(self, trading_date: str):
        original_argv = list(sys.argv)
        try:
            sys.argv = ["orchestrator", "--trading-date", trading_date]
            yield
        finally:
            sys.argv = original_argv

    def _collect_artifacts(self, patterns: Sequence[str], require_all_patterns: bool = True) -> List[str]:
        matched: List[str] = []
        for pattern in patterns:
            files = glob.glob(str(self.reports_dir / pattern))
            if require_all_patterns and not files:
                return []
            matched.extend(files)
        return matched

    def _detect_replay_validation_enabled(self) -> bool:
        return False
