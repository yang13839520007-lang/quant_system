# -*- coding: utf-8 -*-
"""
Created on Wed Mar 18 02:04:41 2026

@author: DELL
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd


@dataclass
class ReviewConfig:
    watchlist_priorities: tuple = ("P1", "P2")
    severe_priority_score_threshold: int = 88


class ReconciliationReviewManager:
    """
    第13.3段：异常闭环复盘层

    输入（默认）:
    - reports/daily_trade_reconciliation_attribution_detail.csv    必需

    输出:
    - reports/daily_trade_reconciliation_review_detail.csv
    - reports/daily_trade_reconciliation_review_summary.csv
    - reports/daily_trade_reconciliation_review_watchlist.csv
    - reports/daily_trade_reconciliation_review_summary.txt
    """

    PRIORITY_ORDER = {"P1": 1, "P2": 2, "P3": 3, "P4": 4}

    def __init__(
        self,
        project_root: str | Path,
        trade_date: str,
    ) -> None:
        self.project_root = Path(project_root)
        self.reports_dir = self.project_root / "reports"
        self.trade_date = trade_date
        self.config = ReviewConfig()

        self.input_detail_path = self.reports_dir / "daily_trade_reconciliation_attribution_detail.csv"

        self.output_detail_path = self.reports_dir / "daily_trade_reconciliation_review_detail.csv"
        self.output_summary_path = self.reports_dir / "daily_trade_reconciliation_review_summary.csv"
        self.output_watchlist_path = self.reports_dir / "daily_trade_reconciliation_review_watchlist.csv"
        self.output_text_path = self.reports_dir / "daily_trade_reconciliation_review_summary.txt"

    # =========================
    # public
    # =========================
    def run(self) -> Dict[str, Any]:
        attribution_df = self._read_csv_required(self.input_detail_path)
        attribution_df = self._prepare_input_df(attribution_df)

        review_detail_df = self._build_review_detail_df(attribution_df)
        review_summary_df = self._build_review_summary_df(review_detail_df)
        watchlist_df = self._build_watchlist_df(review_detail_df)
        summary_text = self._build_summary_text(
            input_rows=len(attribution_df),
            detail_df=review_detail_df,
            summary_df=review_summary_df,
            watchlist_df=watchlist_df,
        )

        self._write_csv(review_detail_df, self.output_detail_path)
        self._write_csv(review_summary_df, self.output_summary_path)
        self._write_csv(watchlist_df, self.output_watchlist_path)
        self.output_text_path.write_text(summary_text, encoding="utf-8")

        return {
            "input_rows": int(len(attribution_df)),
            "review_rows": int(len(review_detail_df)),
            "watchlist_rows": int(len(watchlist_df)),
            "detail_path": str(self.output_detail_path),
            "summary_path": str(self.output_summary_path),
            "watchlist_path": str(self.output_watchlist_path),
            "text_path": str(self.output_text_path),
            "summary_text": summary_text,
        }

    # =========================
    # prepare
    # =========================
    def _prepare_input_df(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        if out.empty:
            return self._empty_input_df()

        required_cols = [
            "trade_date",
            "code",
            "anomaly_type",
            "priority",
            "priority_score",
            "plan_action",
            "actual_action",
            "planned_shares",
            "actual_shares",
            "planned_price",
            "actual_price",
            "planned_amount",
            "actual_amount",
            "qty_deviation",
            "amt_deviation",
            "fill_ratio",
            "slippage_pct",
            "position_deviation_pct",
            "match_status",
            "source_file",
            "source_stage",
            "attribution_reason",
            "suggested_action",
        ]
        for col in required_cols:
            if col not in out.columns:
                out[col] = np.nan

        out["trade_date"] = out["trade_date"].fillna(self.trade_date).astype(str)
        out["code"] = out["code"].astype(str).str.strip().replace({"": np.nan, "nan": np.nan})
        out["anomaly_type"] = out["anomaly_type"].fillna("").astype(str).str.strip()
        out["priority"] = out["priority"].fillna("P4").astype(str).str.strip().str.upper()
        out["priority_score"] = pd.to_numeric(out["priority_score"], errors="coerce").fillna(0).astype(int)

        numeric_cols = [
            "planned_shares",
            "actual_shares",
            "planned_price",
            "actual_price",
            "planned_amount",
            "actual_amount",
            "qty_deviation",
            "amt_deviation",
            "fill_ratio",
            "slippage_pct",
            "position_deviation_pct",
        ]
        for col in numeric_cols:
            out[col] = pd.to_numeric(out[col], errors="coerce")

        text_cols = [
            "plan_action",
            "actual_action",
            "match_status",
            "source_file",
            "source_stage",
            "attribution_reason",
            "suggested_action",
        ]
        for col in text_cols:
            out[col] = out[col].fillna("").astype(str).str.strip()

        out["priority_order"] = out["priority"].map(self.PRIORITY_ORDER).fillna(99).astype(int)
        out = out[out["code"].notna()].copy()
        out = out.sort_values(
            by=["priority_order", "priority_score", "amt_deviation", "qty_deviation", "code"],
            ascending=[True, False, False, False, True],
            na_position="last",
        ).reset_index(drop=True)
        return out

    def _empty_input_df(self) -> pd.DataFrame:
        return pd.DataFrame(columns=[
            "trade_date",
            "code",
            "anomaly_type",
            "priority",
            "priority_score",
            "plan_action",
            "actual_action",
            "planned_shares",
            "actual_shares",
            "planned_price",
            "actual_price",
            "planned_amount",
            "actual_amount",
            "qty_deviation",
            "amt_deviation",
            "fill_ratio",
            "slippage_pct",
            "position_deviation_pct",
            "match_status",
            "source_file",
            "source_stage",
            "attribution_reason",
            "suggested_action",
            "priority_order",
        ])

    # =========================
    # review build
    # =========================
    def _build_review_detail_df(self, df: pd.DataFrame) -> pd.DataFrame:
        columns = [
            "review_id",
            "trade_date",
            "code",
            "anomaly_type",
            "priority",
            "priority_score",
            "responsibility_owner",
            "root_cause_category",
            "root_cause_judgement",
            "repair_action",
            "strategy_revision_needed",
            "strategy_revision_note",
            "execution_fix_needed",
            "execution_fix_note",
            "whitelist_blacklist_action",
            "next_day_track",
            "next_day_track_level",
            "next_day_track_note",
            "closure_status",
            "review_comment",
            "plan_action",
            "actual_action",
            "planned_shares",
            "actual_shares",
            "planned_price",
            "actual_price",
            "planned_amount",
            "actual_amount",
            "qty_deviation",
            "amt_deviation",
            "fill_ratio",
            "slippage_pct",
            "position_deviation_pct",
            "match_status",
            "source_file",
            "source_stage",
            "attribution_reason",
            "suggested_action",
        ]
        if df.empty:
            return pd.DataFrame(columns=columns)

        records: List[Dict[str, Any]] = []
        for idx, row in df.iterrows():
            profile = self._build_review_profile(row)
            records.append({
                "review_id": f"RR{self.trade_date.replace('-', '')}-{idx + 1:04d}",
                "trade_date": row["trade_date"],
                "code": row["code"],
                "anomaly_type": row["anomaly_type"],
                "priority": row["priority"],
                "priority_score": int(row["priority_score"]),
                "responsibility_owner": profile["responsibility_owner"],
                "root_cause_category": profile["root_cause_category"],
                "root_cause_judgement": profile["root_cause_judgement"],
                "repair_action": profile["repair_action"],
                "strategy_revision_needed": profile["strategy_revision_needed"],
                "strategy_revision_note": profile["strategy_revision_note"],
                "execution_fix_needed": profile["execution_fix_needed"],
                "execution_fix_note": profile["execution_fix_note"],
                "whitelist_blacklist_action": profile["whitelist_blacklist_action"],
                "next_day_track": profile["next_day_track"],
                "next_day_track_level": profile["next_day_track_level"],
                "next_day_track_note": profile["next_day_track_note"],
                "closure_status": profile["closure_status"],
                "review_comment": profile["review_comment"],
                "plan_action": row["plan_action"],
                "actual_action": row["actual_action"],
                "planned_shares": row["planned_shares"],
                "actual_shares": row["actual_shares"],
                "planned_price": row["planned_price"],
                "actual_price": row["actual_price"],
                "planned_amount": row["planned_amount"],
                "actual_amount": row["actual_amount"],
                "qty_deviation": row["qty_deviation"],
                "amt_deviation": row["amt_deviation"],
                "fill_ratio": row["fill_ratio"],
                "slippage_pct": row["slippage_pct"],
                "position_deviation_pct": row["position_deviation_pct"],
                "match_status": row["match_status"],
                "source_file": row["source_file"],
                "source_stage": row["source_stage"],
                "attribution_reason": row["attribution_reason"],
                "suggested_action": row["suggested_action"],
            })

        out = pd.DataFrame(records)
        out["priority_order"] = out["priority"].map(self.PRIORITY_ORDER).fillna(99).astype(int)
        out["next_day_track_order"] = out["next_day_track"].map({"是": 1, "否": 2}).fillna(9)
        out["track_level_order"] = out["next_day_track_level"].map({"A": 1, "B": 2, "C": 3, "": 9}).fillna(9)

        out = out.sort_values(
            by=["priority_order", "priority_score", "next_day_track_order", "track_level_order", "amt_deviation", "code"],
            ascending=[True, False, True, True, False, True],
            na_position="last",
        ).reset_index(drop=True)

        out = out.drop(columns=["priority_order", "next_day_track_order", "track_level_order"])
        return out[columns]

    def _build_review_profile(self, row: pd.Series) -> Dict[str, str]:
        anomaly_type = str(row.get("anomaly_type", "") or "").strip()
        priority = str(row.get("priority", "") or "").strip().upper()
        priority_score = int(pd.to_numeric(row.get("priority_score"), errors="coerce") if pd.notna(row.get("priority_score")) else 0)
        slippage_pct = self._safe_float(row.get("slippage_pct"))
        position_deviation_pct = self._safe_float(row.get("position_deviation_pct"))
        fill_ratio = self._safe_float(row.get("fill_ratio"))
        amt_deviation = self._safe_float(row.get("amt_deviation"))

        base_map = {
            "计划漏发": {
                "responsibility_owner": "执行侧",
                "root_cause_category": "流程漏发",
                "root_cause_judgement": "上游计划存在，但未进入委托/对账环节，属于执行链路缺口。",
                "repair_action": "复核计划生成、执行计划落地、委托触发与对账入表全链路，补齐漏发校验。",
                "strategy_revision_needed": "否",
                "strategy_revision_note": "",
                "execution_fix_needed": "是",
                "execution_fix_note": "增加计划到委托的字段级校验与断点告警。",
                "whitelist_blacklist_action": "否",
                "closure_status": "待复核",
            },
            "委托未成交": {
                "responsibility_owner": "执行侧",
                "root_cause_category": "委托约束",
                "root_cause_judgement": "系统已下达计划，但未形成有效成交，常见于限价过严、盘口流动性不足或撤单逻辑触发。",
                "repair_action": "回看盘口与委托日志，评估是否放宽限价、增加重试、补单或尾盘兜底逻辑。",
                "strategy_revision_needed": "否",
                "strategy_revision_note": "",
                "execution_fix_needed": "是",
                "execution_fix_note": "优化限价偏移、补单重试与未成交兜底规则。",
                "whitelist_blacklist_action": "否",
                "closure_status": "待复核",
            },
            "部分成交": {
                "responsibility_owner": "执行侧",
                "root_cause_category": "流动性不足",
                "root_cause_judgement": "实际成交数量低于计划数量，属于成交效率不足或拆单/撤单节奏问题。",
                "repair_action": "复核盘口深度、委托拆分、撤补单逻辑与时间窗设置。",
                "strategy_revision_needed": "否",
                "strategy_revision_note": "",
                "execution_fix_needed": "是",
                "execution_fix_note": "优化拆单节奏与补单策略。",
                "whitelist_blacklist_action": "否",
                "closure_status": "待复核",
            },
            "手工临时交易": {
                "responsibility_owner": "人工操作侧",
                "root_cause_category": "人工干预",
                "root_cause_judgement": "存在系统外人工临时处理，需确认是否属于授权内干预。",
                "repair_action": "核对人工干预登记、聊天记录与券商终端操作留痕，必要时补录白名单。",
                "strategy_revision_needed": "否",
                "strategy_revision_note": "",
                "execution_fix_needed": "否",
                "execution_fix_note": "",
                "whitelist_blacklist_action": "人工白名单评估",
                "closure_status": "待复核",
            },
            "价格滑点超阈值": {
                "responsibility_owner": "执行侧",
                "root_cause_category": "价格冲击",
                "root_cause_judgement": "成交均价相对计划价发生不利偏移，超过预设滑点阈值。",
                "repair_action": "复核成交时段、盘口冲击与限价偏移，必要时调整交易时机或分批执行逻辑。",
                "strategy_revision_needed": "观察",
                "strategy_revision_note": "若连续出现，需评估策略信号触发时点与可交易性假设。",
                "execution_fix_needed": "是",
                "execution_fix_note": "优化委托偏移、分批节奏与流动性约束。",
                "whitelist_blacklist_action": "否",
                "closure_status": "待复核",
            },
            "仓位执行偏差": {
                "responsibility_owner": "风控侧",
                "root_cause_category": "仓位约束",
                "root_cause_judgement": "实际执行股数偏离目标仓位，可能由风控拦截、可用资金、整手约束或补单失败导致。",
                "repair_action": "核对最小交易单位、可用资金、风控阈值与目标仓位换算逻辑。",
                "strategy_revision_needed": "否",
                "strategy_revision_note": "",
                "execution_fix_needed": "是",
                "execution_fix_note": "完善股数取整、资金占用预估与风控回传。",
                "whitelist_blacklist_action": "否",
                "closure_status": "待复核",
            },
            "非系统交易": {
                "responsibility_owner": "通道侧",
                "root_cause_category": "通道越权",
                "root_cause_judgement": "券商侧存在非系统来源成交，需优先排查外部终端或其他策略通道。",
                "repair_action": "核对账户权限、终端登录来源与交易通道映射，必要时临时收紧权限。",
                "strategy_revision_needed": "否",
                "strategy_revision_note": "",
                "execution_fix_needed": "是",
                "execution_fix_note": "增加外部来源识别与通道权限告警。",
                "whitelist_blacklist_action": "黑名单/权限复核",
                "closure_status": "待复核",
            },
            "方向执行异常": {
                "responsibility_owner": "执行侧",
                "root_cause_category": "方向映射错误",
                "root_cause_judgement": "计划方向与实际执行方向不一致，属于高优先级执行风险。",
                "repair_action": "立即复核买卖方向映射、下单指令生成与券商接口方向字段映射。",
                "strategy_revision_needed": "否",
                "strategy_revision_note": "",
                "execution_fix_needed": "是",
                "execution_fix_note": "增加方向字段双向校验与强拦截。",
                "whitelist_blacklist_action": "否",
                "closure_status": "待复核",
            },
        }

        profile = base_map.get(anomaly_type, {
            "responsibility_owner": "数据侧",
            "root_cause_category": "待判定",
            "root_cause_judgement": "当前异常类型未纳入标准模板，需人工补充根因判断。",
            "repair_action": "补充字段映射与异常模板，确认归因口径后再闭环。",
            "strategy_revision_needed": "观察",
            "strategy_revision_note": "",
            "execution_fix_needed": "否",
            "execution_fix_note": "",
            "whitelist_blacklist_action": "否",
            "closure_status": "待复核",
        }).copy()

        next_day_track, next_day_track_level, next_day_track_note = self._decide_next_day_track(
            anomaly_type=anomaly_type,
            priority=priority,
            priority_score=priority_score,
            slippage_pct=slippage_pct,
            position_deviation_pct=position_deviation_pct,
            fill_ratio=fill_ratio,
            amt_deviation=amt_deviation,
        )

        review_comment = self._build_review_comment(
            anomaly_type=anomaly_type,
            priority=priority,
            slippage_pct=slippage_pct,
            position_deviation_pct=position_deviation_pct,
            fill_ratio=fill_ratio,
            amt_deviation=amt_deviation,
        )

        profile["next_day_track"] = next_day_track
        profile["next_day_track_level"] = next_day_track_level
        profile["next_day_track_note"] = next_day_track_note
        profile["review_comment"] = review_comment

        return profile

    def _decide_next_day_track(
        self,
        anomaly_type: str,
        priority: str,
        priority_score: int,
        slippage_pct: float,
        position_deviation_pct: float,
        fill_ratio: float,
        amt_deviation: float,
    ) -> tuple[str, str, str]:
        severe_types = {"方向执行异常", "非系统交易", "计划漏发"}
        track_types = {"委托未成交", "部分成交", "手工临时交易", "价格滑点超阈值", "仓位执行偏差"}

        if anomaly_type in severe_types:
            return "是", "A", "次日开盘前必须复核，盘中纳入重点盯单。"

        if priority in self.config.watchlist_priorities:
            if anomaly_type in track_types:
                return "是", "A", "次日作为重点跟踪标的，复核是否复发。"

        if anomaly_type == "价格滑点超阈值":
            if pd.notna(slippage_pct) and slippage_pct >= 0.02:
                return "是", "A", "滑点偏大，次日需重点观察成交冲击。"
            return "是", "B", "次日观察成交质量与委托偏移。"

        if anomaly_type == "仓位执行偏差":
            if pd.notna(position_deviation_pct) and position_deviation_pct >= 0.20:
                return "是", "A", "仓位偏差较大，次日需优先复核仓位修复。"
            return "是", "B", "次日观察目标仓位修复情况。"

        if anomaly_type == "部分成交":
            if pd.notna(fill_ratio) and fill_ratio < 0.50:
                return "是", "A", "成交完成度偏低，次日优先检查流动性与补单策略。"
            return "是", "B", "次日观察成交完成度是否恢复。"

        if anomaly_type == "委托未成交":
            return "是", "A", "次日重点观察未成交是否再次发生。"

        if anomaly_type == "手工临时交易":
            return "是", "B", "次日核对是否仍需人工干预或纳入白名单。"

        if pd.notna(amt_deviation) and amt_deviation >= 100000:
            return "是", "B", "金额偏差较大，建议次日持续观察。"

        if priority_score >= self.config.severe_priority_score_threshold:
            return "是", "B", "优先级较高，建议次日继续跟踪。"

        return "否", "", ""

    def _build_review_comment(
        self,
        anomaly_type: str,
        priority: str,
        slippage_pct: float,
        position_deviation_pct: float,
        fill_ratio: float,
        amt_deviation: float,
    ) -> str:
        parts: List[str] = [f"{anomaly_type}，当前优先级 {priority}。"]

        if pd.notna(fill_ratio):
            parts.append(f"成交完成度 {fill_ratio:.2%}。")
        if pd.notna(slippage_pct):
            parts.append(f"不利滑点 {slippage_pct:.2%}。")
        if pd.notna(position_deviation_pct):
            parts.append(f"仓位偏差 {position_deviation_pct:.2%}。")
        if pd.notna(amt_deviation):
            parts.append(f"金额偏差 {amt_deviation:.2f}。")

        return "".join(parts)

    # =========================
    # summary / watchlist
    # =========================
    def _build_review_summary_df(self, detail_df: pd.DataFrame) -> pd.DataFrame:
        columns = [
            "responsibility_owner",
            "anomaly_count",
            "code_count",
            "p1_count",
            "p2_count",
            "p3_count",
            "p4_count",
            "next_day_track_count",
            "strategy_revision_count",
            "execution_fix_count",
            "pending_count",
            "confirmed_count",
            "fixed_count",
            "waived_count",
            "next_day_follow_count",
        ]
        if detail_df.empty:
            return pd.DataFrame(columns=columns)

        tmp = detail_df.copy()
        tmp["p1"] = (tmp["priority"] == "P1").astype(int)
        tmp["p2"] = (tmp["priority"] == "P2").astype(int)
        tmp["p3"] = (tmp["priority"] == "P3").astype(int)
        tmp["p4"] = (tmp["priority"] == "P4").astype(int)
        tmp["next_day_track_flag"] = (tmp["next_day_track"] == "是").astype(int)
        tmp["strategy_revision_flag"] = tmp["strategy_revision_needed"].isin(["是", "观察"]).astype(int)
        tmp["execution_fix_flag"] = (tmp["execution_fix_needed"] == "是").astype(int)
        tmp["pending_flag"] = (tmp["closure_status"] == "待复核").astype(int)
        tmp["confirmed_flag"] = (tmp["closure_status"] == "已确认").astype(int)
        tmp["fixed_flag"] = (tmp["closure_status"] == "已修复").astype(int)
        tmp["waived_flag"] = (tmp["closure_status"] == "已豁免").astype(int)
        tmp["next_day_follow_flag"] = (tmp["closure_status"] == "次日跟踪").astype(int)

        summary = tmp.groupby("responsibility_owner", dropna=False).agg(
            anomaly_count=("responsibility_owner", "size"),
            code_count=("code", "nunique"),
            p1_count=("p1", "sum"),
            p2_count=("p2", "sum"),
            p3_count=("p3", "sum"),
            p4_count=("p4", "sum"),
            next_day_track_count=("next_day_track_flag", "sum"),
            strategy_revision_count=("strategy_revision_flag", "sum"),
            execution_fix_count=("execution_fix_flag", "sum"),
            pending_count=("pending_flag", "sum"),
            confirmed_count=("confirmed_flag", "sum"),
            fixed_count=("fixed_flag", "sum"),
            waived_count=("waived_flag", "sum"),
            next_day_follow_count=("next_day_follow_flag", "sum"),
        ).reset_index()

        owner_order = {
            "执行侧": 1,
            "风控侧": 2,
            "通道侧": 3,
            "人工操作侧": 4,
            "策略侧": 5,
            "数据侧": 6,
        }
        summary["owner_order"] = summary["responsibility_owner"].map(owner_order).fillna(99)
        summary = summary.sort_values(
            by=["owner_order", "p1_count", "anomaly_count"],
            ascending=[True, False, False],
        ).drop(columns=["owner_order"]).reset_index(drop=True)

        return summary[columns]

    def _build_watchlist_df(self, detail_df: pd.DataFrame) -> pd.DataFrame:
        columns = [
            "review_id",
            "trade_date",
            "code",
            "anomaly_type",
            "priority",
            "priority_score",
            "responsibility_owner",
            "next_day_track_level",
            "next_day_track_note",
            "repair_action",
            "closure_status",
            "plan_action",
            "actual_action",
            "planned_shares",
            "actual_shares",
            "planned_price",
            "actual_price",
            "fill_ratio",
            "slippage_pct",
            "position_deviation_pct",
            "amt_deviation",
        ]
        if detail_df.empty:
            return pd.DataFrame(columns=columns)

        watchlist = detail_df[detail_df["next_day_track"] == "是"].copy()
        if watchlist.empty:
            return pd.DataFrame(columns=columns)

        level_order = {"A": 1, "B": 2, "C": 3, "": 9}
        watchlist["priority_order"] = watchlist["priority"].map(self.PRIORITY_ORDER).fillna(99)
        watchlist["track_level_order"] = watchlist["next_day_track_level"].map(level_order).fillna(9)
        watchlist = watchlist.sort_values(
            by=["track_level_order", "priority_order", "priority_score", "amt_deviation", "code"],
            ascending=[True, True, False, False, True],
            na_position="last",
        ).reset_index(drop=True)

        watchlist = watchlist[columns].copy()
        return watchlist

    def _build_summary_text(
        self,
        input_rows: int,
        detail_df: pd.DataFrame,
        summary_df: pd.DataFrame,
        watchlist_df: pd.DataFrame,
    ) -> str:
        lines: List[str] = []
        lines.append("=" * 60)
        lines.append("对账异常闭环复盘摘要")
        lines.append("=" * 60)
        lines.append(f"交易日: {self.trade_date}")
        lines.append(f"归因明细记录数: {input_rows}")
        lines.append(f"闭环复盘记录数: {len(detail_df)}")
        lines.append(f"次日重点跟踪数: {len(watchlist_df)}")

        if detail_df.empty:
            lines.append("责任归口分布: 执行侧=0 | 风控侧=0 | 通道侧=0 | 人工操作侧=0 | 策略侧=0 | 数据侧=0")
            lines.append("")
            lines.append("结论: 当前无异常闭环事项。")
            lines.append("=" * 60)
            return "\n".join(lines)

        lines.append(
            "责任归口分布: "
            f"执行侧={int((detail_df['responsibility_owner'] == '执行侧').sum())} | "
            f"风控侧={int((detail_df['responsibility_owner'] == '风控侧').sum())} | "
            f"通道侧={int((detail_df['responsibility_owner'] == '通道侧').sum())} | "
            f"人工操作侧={int((detail_df['responsibility_owner'] == '人工操作侧').sum())} | "
            f"策略侧={int((detail_df['responsibility_owner'] == '策略侧').sum())} | "
            f"数据侧={int((detail_df['responsibility_owner'] == '数据侧').sum())}"
        )
        lines.append("")
        lines.append("按责任归口汇总:")

        for _, row in summary_df.iterrows():
            lines.append(
                "- "
                f"{row['responsibility_owner']}: "
                f"{int(row['anomaly_count'])}条 | "
                f"{int(row['code_count'])}只 | "
                f"P1={int(row['p1_count'])}/P2={int(row['p2_count'])}/P3={int(row['p3_count'])}/P4={int(row['p4_count'])} | "
                f"次日跟踪={int(row['next_day_track_count'])} | "
                f"策略修订={int(row['strategy_revision_count'])} | "
                f"执行修复={int(row['execution_fix_count'])}"
            )

        lines.append("")
        lines.append("次日重点跟踪 TOP5:")
        if watchlist_df.empty:
            lines.append("- 无")
        else:
            top5 = watchlist_df.head(5)
            for _, row in top5.iterrows():
                lines.append(
                    "- "
                    f"[{row['next_day_track_level']}/{row['priority']}/{int(row['priority_score'])}] "
                    f"{row['code']} {row['anomaly_type']} | "
                    f"责任={row['responsibility_owner']} | "
                    f"计划={self._display_action(row['plan_action'])} {self._fmt_shares(row['planned_shares'])} @ {self._fmt_price(row['planned_price'])} | "
                    f"实际={self._display_action(row['actual_action'])} {self._fmt_shares(row['actual_shares'])} @ {self._fmt_price(row['actual_price'])} | "
                    f"跟踪要点={row['next_day_track_note']}"
                )

        lines.append("")
        lines.append("闭环动作建议:")
        owner_actions = {
            "执行侧": "优先复核委托日志、补单重试、方向映射与成交质量控制。",
            "风控侧": "复核仓位换算、资金占用、最小交易单位与拦截回传。",
            "通道侧": "排查账户权限、外部终端与通道来源识别。",
            "人工操作侧": "核对人工干预登记，并评估白名单或留痕机制。",
            "策略侧": "复盘是否需修订触发时点、流动性约束与信号可交易性。",
            "数据侧": "检查字段映射、状态口径与对账源数据完整性。",
        }
        for owner in summary_df["responsibility_owner"].tolist():
            lines.append(f"- {owner}: {owner_actions.get(owner, '按责任归口补充修复动作。')}")

        lines.append("=" * 60)
        return "\n".join(lines)

    # =========================
    # helper
    # =========================
    def _read_csv_required(self, path: Path) -> pd.DataFrame:
        if not path.exists():
            raise FileNotFoundError(f"文件不存在: {path}")

        encodings = ["utf-8-sig", "utf-8", "gbk", "gb18030"]
        last_error: Optional[Exception] = None
        for enc in encodings:
            try:
                return pd.read_csv(path, encoding=enc)
            except Exception as e:
                last_error = e

        raise RuntimeError(f"读取文件失败: {path} | {last_error}")

    def _write_csv(self, df: pd.DataFrame, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(path, index=False, encoding="utf-8-sig")

    def _safe_float(self, value: Any) -> float:
        if pd.isna(value):
            return np.nan
        try:
            return float(value)
        except Exception:
            return np.nan

    def _fmt_price(self, value: Any) -> str:
        v = self._safe_float(value)
        if pd.isna(v):
            return "0.000"
        return f"{v:.3f}"

    def _fmt_shares(self, value: Any) -> str:
        v = self._safe_float(value)
        if pd.isna(v):
            return "nan股"
        return f"{int(round(v))}股"

    def _display_action(self, action: Any) -> str:
        s = str(action or "").strip().lower()
        if s == "buy":
            return "buy"
        if s == "sell":
            return "sell"
        return s or ""