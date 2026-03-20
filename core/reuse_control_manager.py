# -*- coding: utf-8 -*-
"""
Created on Wed Mar 18 15:28:47 2026

@author: DELL
"""

# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set

import pandas as pd

from core.stage_status import normalize_stage_status


DEFAULT_CORE_REALTIME_REQUIRED_STAGE_NOS: Set[int] = {1, 2, 4, 5, 6, 7}


class ReuseControlManager:
    """
    第16段：
    复用阶段审计清单层 + 强制实时执行白名单 + 非白名单复用告警/拒绝机制
    """

    def __init__(
        self,
        base_dir: str = r"C:\quant_system",
        strict_realtime_core: bool = False,
        reuse_violation_action: str = "warn",
        realtime_required_stage_nos: Optional[Iterable[int]] = None,
    ) -> None:
        self.base_dir = str(base_dir)
        self.base_path = Path(base_dir)
        self.reports_dir = self.base_path / "reports"
        self.reports_dir.mkdir(parents=True, exist_ok=True)

        self.strict_realtime_core = bool(strict_realtime_core)
        self.reuse_violation_action = str(reuse_violation_action).strip().lower()
        if self.reuse_violation_action not in {"warn", "reject"}:
            self.reuse_violation_action = "warn"

        if realtime_required_stage_nos is None:
            realtime_required_stage_nos = DEFAULT_CORE_REALTIME_REQUIRED_STAGE_NOS
        self.realtime_required_stage_nos = {
            int(x) for x in realtime_required_stage_nos if str(x).strip()
        }

    # ============================================================
    # 运行时复用判定
    # ============================================================
    def evaluate_reuse(
        self,
        stage_no: int,
        stage_name: str,
        artifact_files: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        artifact_files = artifact_files or []
        is_core_realtime_required = int(stage_no) in self.realtime_required_stage_nos

        if not is_core_realtime_required:
            return {
                "reuse_allowed": True,
                "policy_action": "allow",
                "policy_level": "NON_CORE_REUSE_ALLOWED",
                "policy_message": f"阶段{stage_no:02d}非核心实时白名单约束阶段，允许复用",
                "artifact_files": artifact_files,
            }

        action = "reject" if self.strict_realtime_core or self.reuse_violation_action == "reject" else "warn"
        return {
            "reuse_allowed": False,
            "policy_action": action,
            "policy_level": "CORE_REALTIME_REQUIRED",
            "policy_message": (
                f"阶段{stage_no:02d} {stage_name} 属于核心实时执行白名单阶段，"
                f"禁止复用既有工件，当前触发 {action.upper()} 机制"
            ),
            "artifact_files": artifact_files,
        }

    # ============================================================
    # 主控结束后审计
    # ============================================================
    def audit(
        self,
        trading_date: str,
        stage_results: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        rows: List[Dict[str, Any]] = []

        for row in stage_results:
            stage_no = int(row.get("stage_no", 0))
            stage_status = normalize_stage_status(row, row.get("stage_status"))
            is_reused = stage_status == "SUCCESS_REUSED"
            is_repaired = stage_status == "SUCCESS_REPAIRED"
            is_failed = stage_status == "FAILED"
            is_skipped = stage_status == "SKIPPED"
            is_core_required = stage_no in self.realtime_required_stage_nos
            policy_rejected = bool(row.get("policy_rejected"))

            violation_type = ""
            policy_expected = "REALTIME_REQUIRED" if is_core_required else "REUSE_ALLOWED"

            if is_core_required and is_reused:
                violation_type = "CORE_STAGE_REUSED"
            elif is_core_required and is_failed and policy_rejected:
                violation_type = "CORE_STAGE_REUSE_REJECTED"
            elif is_core_required and is_failed:
                violation_type = "CORE_STAGE_FAILED"
            elif is_core_required and is_skipped:
                violation_type = "CORE_STAGE_SKIPPED"

            rows.append(
                {
                    "trading_date": trading_date,
                    "stage_no": stage_no,
                    "stage_name": row.get("stage_name"),
                    "stage_status": stage_status,
                    "is_success": str(stage_status).startswith("SUCCESS"),
                    "is_reused": is_reused,
                    "is_repaired": is_repaired,
                    "is_failed": is_failed,
                    "is_skipped": is_skipped,
                    "is_core_realtime_required": is_core_required,
                    "policy_rejected": policy_rejected,
                    "policy_expected": policy_expected,
                    "violation_type": violation_type,
                    "policy_message": row.get("policy_message", ""),
                    "entry_type": row.get("entry_type"),
                    "entry_target": row.get("entry_target"),
                    "artifact_files": " | ".join(row.get("artifact_files", []) or []),
                }
            )

        df = pd.DataFrame(rows)
        csv_path = self.reports_dir / "daily_orchestrator_reuse_audit.csv"
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")

        core_df = df[df["is_core_realtime_required"] == True].copy()
        core_stage_count = int(len(core_df))
        core_reused_count = int((core_df["is_reused"] == True).sum()) if not core_df.empty else 0
        core_failed_count = int((core_df["is_failed"] == True).sum()) if not core_df.empty else 0
        core_skipped_count = int((core_df["is_skipped"] == True).sum()) if not core_df.empty else 0
        core_policy_rejected_count = int((core_df["policy_rejected"] == True).sum()) if not core_df.empty else 0
        core_success_executed_like_count = int(
            core_df["stage_status"].isin(["SUCCESS_EXECUTED", "SUCCESS_REPAIRED"]).sum()
        ) if not core_df.empty else 0

        reused_stage_count = int((df["is_reused"] == True).sum()) if not df.empty else 0
        repaired_stage_count = int((df["is_repaired"] == True).sum()) if not df.empty else 0
        failed_stage_count = int((df["is_failed"] == True).sum()) if not df.empty else 0
        skipped_stage_count = int((df["is_skipped"] == True).sum()) if not df.empty else 0

        core_realtime_coverage_ratio = (
            round(core_success_executed_like_count / core_stage_count, 4)
            if core_stage_count > 0 else 1.0
        )

        if core_policy_rejected_count > 0:
            production_mode_label = "CORE_CHAIN_BLOCKED_BY_REJECT"
        elif core_failed_count > 0 or core_skipped_count > 0:
            production_mode_label = "CORE_CHAIN_NOT_REALTIME"
        elif core_reused_count > 0:
            production_mode_label = "CORE_CHAIN_NOT_REALTIME"
        elif reused_stage_count > 0:
            production_mode_label = "CORE_REALTIME_NONCORE_REUSE"
        else:
            production_mode_label = "FULL_REALTIME_RECOMPUTE"

        if core_policy_rejected_count > 0 or core_reused_count > 0 or core_failed_count > 0 or core_skipped_count > 0:
            reuse_audit_status = "NOT_READY_FOR_FULL_REALTIME"
        elif reused_stage_count > 0:
            reuse_audit_status = "CORE_READY_NONCORE_REUSE"
        else:
            reuse_audit_status = "FULL_REALTIME_READY"

        payload: Dict[str, Any] = {
            "trading_date": trading_date,
            "strict_realtime_core": self.strict_realtime_core,
            "reuse_violation_action": self.reuse_violation_action,
            "realtime_required_stage_nos": sorted(self.realtime_required_stage_nos),
            "reuse_audit_status": reuse_audit_status,
            "production_mode_label": production_mode_label,
            "reuse_metrics": {
                "stage_count": int(len(df)),
                "reused_stage_count": reused_stage_count,
                "repaired_stage_count": repaired_stage_count,
                "failed_stage_count": failed_stage_count,
                "skipped_stage_count": skipped_stage_count,
                "core_stage_count": core_stage_count,
                "core_reused_count": core_reused_count,
                "core_failed_count": core_failed_count,
                "core_skipped_count": core_skipped_count,
                "core_policy_rejected_count": core_policy_rejected_count,
                "core_success_executed_like_count": core_success_executed_like_count,
                "core_realtime_coverage_ratio": core_realtime_coverage_ratio,
            },
            "output_csv_path": str(csv_path),
        }

        json_path = self.reports_dir / "daily_orchestrator_reuse_audit.json"
        txt_path = self.reports_dir / "daily_orchestrator_reuse_audit_summary.txt"

        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        self._write_txt_summary(
            txt_path=txt_path,
            payload=payload,
            audit_df=df,
        )

        stage16_result = {
            "stage_no": 16,
            "stage_name": "复用阶段审计清单层 + 实时重算切换层",
            "stage_status": "SUCCESS_EXECUTED",
            "success": True,
            "reuse_audit_status": reuse_audit_status,
            "production_mode_label": production_mode_label,
            "strict_realtime_core": self.strict_realtime_core,
            "reuse_violation_action": self.reuse_violation_action,
            "core_reused_count": core_reused_count,
            "core_policy_rejected_count": core_policy_rejected_count,
            "core_realtime_coverage_ratio": core_realtime_coverage_ratio,
            "output_csv_path": str(csv_path),
            "output_json_path": str(json_path),
            "output_summary_path": str(txt_path),
        }
        payload["stage16_result"] = stage16_result
        return payload

    def _write_txt_summary(
        self,
        txt_path: Path,
        payload: Dict[str, Any],
        audit_df: pd.DataFrame,
    ) -> None:
        metrics = payload["reuse_metrics"]

        lines = [
            "============================================================",
            "复用阶段审计清单层 + 实时重算切换层",
            f"目标交易日: {payload['trading_date']}",
            f"审计状态: {payload['reuse_audit_status']}",
            f"生产模式: {payload['production_mode_label']}",
            f"严格核心实时: {payload['strict_realtime_core']}",
            f"违规处理动作: {payload['reuse_violation_action']}",
            f"核心实时白名单阶段: {payload['realtime_required_stage_nos']}",
            "------------------------------------------------------------",
            "审计统计：",
            f"  总阶段数                 : {metrics['stage_count']}",
            f"  复用阶段数               : {metrics['reused_stage_count']}",
            f"  自动修正阶段数           : {metrics['repaired_stage_count']}",
            f"  失败阶段数               : {metrics['failed_stage_count']}",
            f"  跳过阶段数               : {metrics['skipped_stage_count']}",
            f"  核心白名单阶段数         : {metrics['core_stage_count']}",
            f"  核心阶段被复用数         : {metrics['core_reused_count']}",
            f"  核心阶段拒绝数           : {metrics['core_policy_rejected_count']}",
            f"  核心阶段失败数           : {metrics['core_failed_count']}",
            f"  核心阶段跳过数           : {metrics['core_skipped_count']}",
            f"  核心阶段实时覆盖率       : {metrics['core_realtime_coverage_ratio']}",
            "------------------------------------------------------------",
            "违规阶段：",
        ]

        violation_df = audit_df[
            audit_df["violation_type"].astype(str).str.strip() != ""
        ].copy()

        if violation_df.empty:
            lines.append("  无")
        else:
            for _, row in violation_df.iterrows():
                lines.append(
                    f"  [{int(row['stage_no']):02d}] {row['stage_name']} -> "
                    f"{row['stage_status']} | {row['violation_type']}"
                )

        lines.append("============================================================")

        with open(txt_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))