# -*- coding: utf-8 -*-
"""
Created on Wed Mar 18 14:45:06 2026

@author: DELL
"""

# -*- coding: utf-8 -*-
from __future__ import annotations

import glob
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from core.stage_status import (
    FAILED,
    SKIPPED,
    SUCCESS_EXECUTED,
    SUCCESS_REPAIRED,
    SUCCESS_REUSED,
    build_stage_status_counts,
    derive_run_mode_label,
    normalize_stage_status,
)


class PostRunAcceptanceManager:
    """
    第15段：主控后自动验收层 + 运行模式标识层

    目标：
    1. 汇总所有 stage_status，输出升级后状态统计
    2. 自动判断：
       - 是否存在失败阶段
       - 是否存在复用阶段
       - 是否存在伪“委托未成交”
       - 闭环是否异常放大
       - 当前是否仅剩执行滑点类问题
    3. 输出：
       - reports/daily_orchestrator_stage_status.csv
       - reports/daily_orchestrator_acceptance.json
       - reports/daily_orchestrator_acceptance_summary.txt
    """

    def __init__(self, base_dir: str = r"C:\quant_system") -> None:
        self.base_dir = Path(base_dir)
        self.reports_dir = self.base_dir / "reports"
        self.reports_dir.mkdir(parents=True, exist_ok=True)

    def run(
        self,
        trading_date: str,
        stage_results: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        normalized_stage_results = self._normalize_stage_results(stage_results)
        stage_status_counts = build_stage_status_counts(normalized_stage_results)
        run_mode_label = derive_run_mode_label(normalized_stage_results)

        stage_status_csv_path = self._write_stage_status_csv(
            trading_date=trading_date,
            stage_results=normalized_stage_results,
        )

        anomalies_df, anomalies_path = self._load_first_csv(
            patterns=[
                "*trade*reconciliation*anomal*.csv",
                "*reconciliation*anomal*.csv",
                "*anomal*.csv",
            ]
        )
        attribution_df, attribution_path = self._load_first_csv(
            patterns=[
                "*trade*reconciliation*attribution*.csv",
                "*reconciliation*attribution*.csv",
                "*attribution*.csv",
            ]
        )
        review_df, review_path = self._load_first_csv(
            patterns=[
                "*trade*reconciliation*review*.csv",
                "*reconciliation*review*.csv",
                "*review*.csv",
            ]
        )

        analysis = self._build_acceptance_analysis(
            stage_results=normalized_stage_results,
            anomalies_df=anomalies_df,
            attribution_df=attribution_df,
            review_df=review_df,
        )

        acceptance_status = self._derive_acceptance_status(
            has_failed_stage=analysis["has_failed_stage"],
            has_pseudo_unfilled_order=analysis["has_pseudo_unfilled_order"],
            close_loop_abnormal_amplified=analysis["close_loop_abnormal_amplified"],
            has_reused_stage=analysis["has_reused_stage"],
        )

        payload: Dict[str, Any] = {
            "trading_date": trading_date,
            "acceptance_status": acceptance_status,
            "run_mode_label": run_mode_label,
            "stage_status_counts": stage_status_counts,
            "acceptance_analysis": analysis,
            "source_reports": {
                "stage_status_csv": str(stage_status_csv_path),
                "reconciliation_anomalies_csv": anomalies_path,
                "reconciliation_attribution_csv": attribution_path,
                "reconciliation_review_csv": review_path,
            },
        }

        json_path = self.reports_dir / "daily_orchestrator_acceptance.json"
        txt_path = self.reports_dir / "daily_orchestrator_acceptance_summary.txt"

        self._write_json(json_path, payload)
        self._write_txt_summary(txt_path, payload)

        stage15_result = {
            "stage_no": 15,
            "stage_name": "主控后自动验收层 + 运行模式标识层",
            "stage_status": SUCCESS_EXECUTED,
            "success": True,
            "trading_date": trading_date,
            "acceptance_status": acceptance_status,
            "run_mode_label": run_mode_label,
            "has_failed_stage": analysis["has_failed_stage"],
            "has_reused_stage": analysis["has_reused_stage"],
            "has_pseudo_unfilled_order": analysis["has_pseudo_unfilled_order"],
            "close_loop_abnormal_amplified": analysis["close_loop_abnormal_amplified"],
            "only_execution_slippage_left": analysis["only_execution_slippage_left"],
            "output_json_path": str(json_path),
            "output_summary_path": str(txt_path),
            "output_stage_status_csv_path": str(stage_status_csv_path),
        }

        payload["stage15_result"] = stage15_result
        return payload

    def _normalize_stage_results(
        self,
        stage_results: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        normalized_rows: List[Dict[str, Any]] = []

        for row in stage_results:
            item = dict(row)
            item["stage_status"] = normalize_stage_status(item, item.get("stage_status"))
            item["success"] = item["stage_status"] in {
                SUCCESS_EXECUTED,
                SUCCESS_REUSED,
                SUCCESS_REPAIRED,
            }
            normalized_rows.append(item)

        return normalized_rows

    def _write_stage_status_csv(
        self,
        trading_date: str,
        stage_results: List[Dict[str, Any]],
    ) -> Path:
        rows: List[Dict[str, Any]] = []
        for item in stage_results:
            rows.append(
                {
                    "trading_date": trading_date,
                    "stage_no": item.get("stage_no"),
                    "stage_name": item.get("stage_name"),
                    "stage_status": item.get("stage_status"),
                    "duration_sec": item.get("duration_sec"),
                    "entry_type": item.get("entry_type"),
                    "entry_target": item.get("entry_target"),
                    "reused": item.get("stage_status") == SUCCESS_REUSED,
                    "repaired": item.get("stage_status") == SUCCESS_REPAIRED,
                    "error_message": item.get("error_message") or item.get("error"),
                }
            )

        output_path = self.reports_dir / "daily_orchestrator_stage_status.csv"
        pd.DataFrame(rows).to_csv(output_path, index=False, encoding="utf-8-sig")
        return output_path

    def _load_first_csv(self, patterns: List[str]) -> Tuple[pd.DataFrame, Optional[str]]:
        matched_files: List[str] = []
        for pattern in patterns:
            matched_files.extend(glob.glob(str(self.reports_dir / pattern)))

        matched_files = sorted(set(matched_files), key=lambda x: os.path.getmtime(x), reverse=True)
        for file_path in matched_files:
            try:
                df = pd.read_csv(file_path)
                return df, file_path
            except Exception:
                continue

        return pd.DataFrame(), None

    def _build_acceptance_analysis(
        self,
        stage_results: List[Dict[str, Any]],
        anomalies_df: pd.DataFrame,
        attribution_df: pd.DataFrame,
        review_df: pd.DataFrame,
    ) -> Dict[str, Any]:
        has_failed_stage = any(
            normalize_stage_status(row, row.get("stage_status")) == FAILED
            for row in stage_results
        )
        has_reused_stage = any(
            normalize_stage_status(row, row.get("stage_status")) == SUCCESS_REUSED
            for row in stage_results
        )

        anomaly_base_df = attribution_df if not attribution_df.empty else anomalies_df
        anomaly_count = int(len(anomaly_base_df)) if not anomaly_base_df.empty else 0
        review_count = int(len(review_df)) if not review_df.empty else 0

        has_pseudo_unfilled_order = self._detect_pseudo_unfilled_order(
            anomalies_df=anomalies_df,
            attribution_df=attribution_df,
            review_df=review_df,
        )

        close_loop_abnormal_amplified, amplified_reason = self._detect_close_loop_amplified(
            anomaly_count=anomaly_count,
            review_df=review_df,
        )

        only_execution_slippage_left = self._detect_only_execution_slippage_left(
            anomalies_df=anomalies_df,
            attribution_df=attribution_df,
            review_df=review_df,
        )

        return {
            "has_failed_stage": has_failed_stage,
            "has_reused_stage": has_reused_stage,
            "has_pseudo_unfilled_order": has_pseudo_unfilled_order,
            "close_loop_abnormal_amplified": close_loop_abnormal_amplified,
            "close_loop_abnormal_amplified_reason": amplified_reason,
            "only_execution_slippage_left": only_execution_slippage_left,
            "anomaly_count": anomaly_count,
            "review_count": review_count,
        }

    def _detect_pseudo_unfilled_order(
        self,
        anomalies_df: pd.DataFrame,
        attribution_df: pd.DataFrame,
        review_df: pd.DataFrame,
    ) -> bool:
        for df in [anomalies_df, attribution_df, review_df]:
            if df.empty:
                continue

            text_df = df.fillna("").astype(str)
            all_text = text_df.apply(lambda row: " | ".join(row.values.tolist()), axis=1).str.upper()

            unfilled_mask = all_text.str.contains("委托未成交|未成交|UNFILLED|NOT FILLED", regex=True)
            pseudo_mask = all_text.str.contains(
                "伪|FALSE_POSITIVE|PSEUDO|非异常|无需处理|计划股数缺失|PLANNED_SHARES",
                regex=True,
            )
            if bool((unfilled_mask & pseudo_mask).any()):
                return True

        return False

    def _detect_close_loop_amplified(
        self,
        anomaly_count: int,
        review_df: pd.DataFrame,
    ) -> Tuple[bool, str]:
        if review_df.empty:
            return False, ""

        review_count = int(len(review_df))
        if anomaly_count > 0 and review_count > anomaly_count:
            return True, f"闭环复盘记录数({review_count}) > 原始异常数({anomaly_count})"

        priority_col = self._pick_existing_col(
            review_df,
            [
                "priority",
                "priority_level",
                "severity",
                "严重级别",
                "优先级",
                "level",
            ],
        )
        if priority_col:
            priority_values = review_df[priority_col].fillna("").astype(str).str.upper().tolist()
            if any(p in {"P1", "P2", "P3", "1", "2", "3"} for p in priority_values):
                return True, "闭环复盘出现 P1/P2/P3，异常等级放大"

        return False, ""

    def _detect_only_execution_slippage_left(
        self,
        anomalies_df: pd.DataFrame,
        attribution_df: pd.DataFrame,
        review_df: pd.DataFrame,
    ) -> bool:
        base_df = attribution_df if not attribution_df.empty else anomalies_df
        if base_df.empty:
            return False

        if not self._all_rows_are_slippage(base_df):
            return False

        if not self._all_numeric_zero(
            base_df,
            candidates=[
                "qty_diff",
                "shares_diff",
                "share_diff",
                "quantity_diff",
                "数量偏差",
                "偏差股数",
                "planned_shares_diff",
            ],
        ):
            return False

        if not self._all_numeric_zero(
            base_df,
            candidates=[
                "position_diff_pct",
                "position_deviation_pct",
                "weight_diff_pct",
                "仓位偏差",
                "仓位偏差_pct",
                "position_diff",
            ],
        ):
            return False

        if not review_df.empty:
            if not self._all_execution_side(review_df):
                return False
            if not self._all_priority_p4(review_df):
                return False

        if self._detect_pseudo_unfilled_order(anomalies_df, attribution_df, review_df):
            return False

        return True

    def _all_rows_are_slippage(self, df: pd.DataFrame) -> bool:
        text_series = df.fillna("").astype(str).apply(lambda row: " | ".join(row.values.tolist()), axis=1).str.upper()
        if text_series.empty:
            return False

        slippage_mask = text_series.str.contains("滑点|SLIPPAGE|PRICE_SLIPPAGE", regex=True)
        negative_mask = text_series.str.contains(
            "委托未成交|未成交|UNFILLED|数量偏差|仓位偏差|QTY_DIFF|POSITION_DIFF",
            regex=True,
        )
        return bool(slippage_mask.all() and (~negative_mask).all())

    def _all_execution_side(self, df: pd.DataFrame) -> bool:
        col = self._pick_existing_col(
            df,
            ["responsibility", "owner", "责任归口", "责任侧", "归口", "责任人"],
        )
        if not col:
            text_series = df.fillna("").astype(str).apply(lambda row: " | ".join(row.values.tolist()), axis=1).str.upper()
            return bool(text_series.str.contains("执行侧|EXECUTION", regex=True).all())

        values = df[col].fillna("").astype(str).str.upper()
        return bool(values.str.contains("执行侧|EXECUTION", regex=True).all())

    def _all_priority_p4(self, df: pd.DataFrame) -> bool:
        col = self._pick_existing_col(
            df,
            ["priority", "priority_level", "severity", "严重级别", "优先级", "level"],
        )
        if not col:
            text_series = df.fillna("").astype(str).apply(lambda row: " | ".join(row.values.tolist()), axis=1).str.upper()
            return bool(text_series.str.contains("P4|优先级.?4|LEVEL.?4", regex=True).all())

        values = df[col].fillna("").astype(str).str.upper()
        return bool(values.isin(["P4", "4", "LEVEL4", "LEVEL_4"]).all() or values.str.contains("P4", regex=True).all())

    def _all_numeric_zero(self, df: pd.DataFrame, candidates: List[str]) -> bool:
        col = self._pick_existing_col(df, candidates)
        if not col:
            return True

        series = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
        return bool((series.abs() <= 1e-12).all())

    def _pick_existing_col(self, df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
        lower_map = {str(col).strip().lower(): col for col in df.columns}
        for candidate in candidates:
            if candidate.lower() in lower_map:
                return lower_map[candidate.lower()]
        return None

    def _derive_acceptance_status(
        self,
        has_failed_stage: bool,
        has_pseudo_unfilled_order: bool,
        close_loop_abnormal_amplified: bool,
        has_reused_stage: bool,
    ) -> str:
        if has_failed_stage:
            return "REJECTED_FAILED_STAGE"
        if has_pseudo_unfilled_order:
            return "REJECTED_PSEUDO_UNFILLED_ORDER"
        if close_loop_abnormal_amplified:
            return "REJECTED_CLOSE_LOOP_AMPLIFIED"
        if has_reused_stage:
            return "PASS_REUSE_MODE"
        return "PASS_REALTIME_MODE"

    def _write_json(self, path: Path, payload: Dict[str, Any]) -> None:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

    def _write_txt_summary(self, path: Path, payload: Dict[str, Any]) -> None:
        analysis = payload["acceptance_analysis"]
        counts = payload["stage_status_counts"]

        lines = [
            "============================================================",
            "主控后自动验收层 + 运行模式标识层",
            f"目标交易日: {payload['trading_date']}",
            f"验收状态: {payload['acceptance_status']}",
            f"运行模式: {payload['run_mode_label']}",
            "------------------------------------------------------------",
            "阶段状态统计：",
            f"  SUCCESS_EXECUTED : {counts.get(SUCCESS_EXECUTED, 0)}",
            f"  SUCCESS_REUSED   : {counts.get(SUCCESS_REUSED, 0)}",
            f"  SUCCESS_REPAIRED : {counts.get(SUCCESS_REPAIRED, 0)}",
            f"  FAILED           : {counts.get(FAILED, 0)}",
            f"  SKIPPED          : {counts.get(SKIPPED, 0)}",
            "------------------------------------------------------------",
            "自动验收结论：",
            f"  是否存在失败阶段       : {analysis['has_failed_stage']}",
            f"  是否存在复用阶段       : {analysis['has_reused_stage']}",
            f"  是否存在伪委托未成交   : {analysis['has_pseudo_unfilled_order']}",
            f"  闭环是否异常放大       : {analysis['close_loop_abnormal_amplified']}",
            f"  当前是否仅剩执行滑点类 : {analysis['only_execution_slippage_left']}",
            f"  对账异常条数           : {analysis['anomaly_count']}",
            f"  闭环复盘记录数         : {analysis['review_count']}",
        ]

        amplified_reason = analysis.get("close_loop_abnormal_amplified_reason")
        if amplified_reason:
            lines.append(f"  放大原因               : {amplified_reason}")

        lines.append("============================================================")

        with open(path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))