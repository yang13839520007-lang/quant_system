from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd


@dataclass
class ReplayConfig:
    slippage_threshold: float = 0.01
    position_deviation_threshold: float = 0.10
    full_fill_threshold: float = 0.98


class ReconciliationReplayValidationManager:
    """
    第13.4段：异常注入回放验证层

    核心目标：
    - 构造 synthetic 对账样本
    - 回放 13.2 对账异常归因层
    - 回放 13.3 异常闭环复盘层
    - 统计各异常类型命中率与遗漏情况

    输出（默认）:
    - reports/daily_trade_reconciliation_replay_injected_detail.csv
    - reports/daily_trade_reconciliation_replay_expected_cases.csv
    - reports/daily_trade_reconciliation_replay_attribution_detail.csv
    - reports/daily_trade_reconciliation_replay_attribution_summary.csv
    - reports/daily_trade_reconciliation_replay_attribution_priority.csv
    - reports/daily_trade_reconciliation_replay_review_detail.csv
    - reports/daily_trade_reconciliation_replay_review_summary.csv
    - reports/daily_trade_reconciliation_replay_review_watchlist.csv
    - reports/daily_trade_reconciliation_replay_validation_detail.csv
    - reports/daily_trade_reconciliation_replay_validation_summary.csv
    - reports/daily_trade_reconciliation_replay_summary.txt
    """

    def __init__(
        self,
        project_root: str | Path,
        trade_date: str,
        attribution_manager_class,
        review_manager_class,
        slippage_threshold: float = 0.01,
        position_deviation_threshold: float = 0.10,
        full_fill_threshold: float = 0.98,
    ) -> None:
        self.project_root = Path(project_root)
        self.reports_dir = self.project_root / "reports"
        self.trade_date = trade_date
        self.attribution_manager_class = attribution_manager_class
        self.review_manager_class = review_manager_class
        self.config = ReplayConfig(
            slippage_threshold=slippage_threshold,
            position_deviation_threshold=position_deviation_threshold,
            full_fill_threshold=full_fill_threshold,
        )

        self.temp_project_root = self.project_root / "temp" / "reconciliation_replay_validation"
        self.temp_reports_dir = self.temp_project_root / "reports"

        self.output_injected_detail_path = self.reports_dir / "daily_trade_reconciliation_replay_injected_detail.csv"
        self.output_expected_cases_path = self.reports_dir / "daily_trade_reconciliation_replay_expected_cases.csv"
        self.output_attr_detail_path = self.reports_dir / "daily_trade_reconciliation_replay_attribution_detail.csv"
        self.output_attr_summary_path = self.reports_dir / "daily_trade_reconciliation_replay_attribution_summary.csv"
        self.output_attr_priority_path = self.reports_dir / "daily_trade_reconciliation_replay_attribution_priority.csv"
        self.output_review_detail_path = self.reports_dir / "daily_trade_reconciliation_replay_review_detail.csv"
        self.output_review_summary_path = self.reports_dir / "daily_trade_reconciliation_replay_review_summary.csv"
        self.output_review_watchlist_path = self.reports_dir / "daily_trade_reconciliation_replay_review_watchlist.csv"
        self.output_validation_detail_path = self.reports_dir / "daily_trade_reconciliation_replay_validation_detail.csv"
        self.output_validation_summary_path = self.reports_dir / "daily_trade_reconciliation_replay_validation_summary.csv"
        self.output_text_path = self.reports_dir / "daily_trade_reconciliation_replay_summary.txt"

    # =========================
    # public
    # =========================
    def run(self) -> Dict[str, Any]:
        self._reset_temp_workspace()

        injected_detail_df, expected_cases_df, execution_plan_df = self._build_replay_cases()
        self._write_csv(injected_detail_df, self.temp_reports_dir / "daily_trade_reconciliation_detail.csv")
        self._write_csv(execution_plan_df, self.temp_reports_dir / "daily_execution_plan.csv")
        self._write_csv(expected_cases_df, self.temp_reports_dir / "daily_trade_reconciliation_replay_expected_cases.csv")

        attr_result = self._run_attribution_layer()
        review_result = self._run_review_layer()

        replay_attr_detail = self._read_csv_optional(self.temp_reports_dir / "daily_trade_reconciliation_attribution_detail.csv")
        replay_attr_summary = self._read_csv_optional(self.temp_reports_dir / "daily_trade_reconciliation_attribution_summary.csv")
        replay_attr_priority = self._read_csv_optional(self.temp_reports_dir / "daily_trade_reconciliation_attribution_priority.csv")
        replay_review_detail = self._read_csv_optional(self.temp_reports_dir / "daily_trade_reconciliation_review_detail.csv")
        replay_review_summary = self._read_csv_optional(self.temp_reports_dir / "daily_trade_reconciliation_review_summary.csv")
        replay_review_watchlist = self._read_csv_optional(self.temp_reports_dir / "daily_trade_reconciliation_review_watchlist.csv")

        validation_detail_df = self._build_validation_detail_df(
            expected_cases_df=expected_cases_df,
            attr_detail_df=replay_attr_detail,
            review_detail_df=replay_review_detail,
        )
        validation_summary_df = self._build_validation_summary_df(validation_detail_df)
        summary_text = self._build_summary_text(
            injected_detail_df=injected_detail_df,
            expected_cases_df=expected_cases_df,
            attr_detail_df=replay_attr_detail,
            review_detail_df=replay_review_detail,
            validation_detail_df=validation_detail_df,
            validation_summary_df=validation_summary_df,
        )

        self._write_csv(injected_detail_df, self.output_injected_detail_path)
        self._write_csv(expected_cases_df, self.output_expected_cases_path)
        self._write_csv(replay_attr_detail, self.output_attr_detail_path)
        self._write_csv(replay_attr_summary, self.output_attr_summary_path)
        self._write_csv(replay_attr_priority, self.output_attr_priority_path)
        self._write_csv(replay_review_detail, self.output_review_detail_path)
        self._write_csv(replay_review_summary, self.output_review_summary_path)
        self._write_csv(replay_review_watchlist, self.output_review_watchlist_path)
        self._write_csv(validation_detail_df, self.output_validation_detail_path)
        self._write_csv(validation_summary_df, self.output_validation_summary_path)
        self.output_text_path.write_text(summary_text, encoding="utf-8")

        return {
            "injected_rows": int(len(injected_detail_df)),
            "expected_cases": int(len(expected_cases_df)),
            "attribution_rows": int(len(replay_attr_detail)),
            "review_rows": int(len(replay_review_detail)),
            "validation_rows": int(len(validation_detail_df)),
            "hit_count": int(validation_detail_df["hit_flag"].sum()) if not validation_detail_df.empty else 0,
            "miss_count": int((1 - validation_detail_df["hit_flag"]).sum()) if not validation_detail_df.empty else 0,
            "normal_case_pass": self._normal_case_pass(validation_detail_df),
            "summary_text": summary_text,
            "injected_detail_path": str(self.output_injected_detail_path),
            "expected_cases_path": str(self.output_expected_cases_path),
            "attr_detail_path": str(self.output_attr_detail_path),
            "attr_summary_path": str(self.output_attr_summary_path),
            "attr_priority_path": str(self.output_attr_priority_path),
            "review_detail_path": str(self.output_review_detail_path),
            "review_summary_path": str(self.output_review_summary_path),
            "review_watchlist_path": str(self.output_review_watchlist_path),
            "validation_detail_path": str(self.output_validation_detail_path),
            "validation_summary_path": str(self.output_validation_summary_path),
            "text_path": str(self.output_text_path),
            "attribution_summary_text": attr_result.get("summary_text", ""),
            "review_summary_text": review_result.get("summary_text", ""),
        }

    # =========================
    # replay cases
    # =========================
    def _build_replay_cases(self) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        cases: List[Dict[str, Any]] = []
        plans: List[Dict[str, Any]] = []
        injected_rows: List[Dict[str, Any]] = []

        def add_plan(code: str, action: str, shares: int, price: float) -> None:
            plans.append({
                "code": code,
                "plan_action": action,
                "planned_shares": shares,
                "planned_price": price,
                "planned_amount": shares * price,
            })

        def add_case(
            case_id: str,
            code: str,
            expected_anomaly_type: str,
            row: Optional[Dict[str, Any]],
            expected_priority_hint: str,
            note: str,
            plan_row: Optional[Dict[str, Any]] = None,
            expected_review_owner: Optional[str] = None,
            should_track_next_day: str = "是",
            expected_hit: int = 1,
        ) -> None:
            cases.append({
                "case_id": case_id,
                "code": code,
                "expected_anomaly_type": expected_anomaly_type,
                "expected_priority_hint": expected_priority_hint,
                "expected_review_owner": expected_review_owner or "",
                "should_track_next_day": should_track_next_day,
                "expected_hit": expected_hit,
                "case_note": note,
                "row_type": "recon_detail" if row is not None else "missing_in_recon",
            })

            if row is not None:
                injected_rows.append(row)

            if plan_row is not None:
                add_plan(**plan_row)

        # CASE00 正常样本
        add_case(
            case_id="CASE00",
            code="sh.600010",
            expected_anomaly_type="正常样本",
            row={
                "code": "sh.600010",
                "plan_action": "buy",
                "actual_action": "buy",
                "planned_shares": 1000,
                "actual_shares": 1000,
                "planned_price": 10.00,
                "actual_price": 10.00,
                "planned_amount": 10000.00,
                "actual_amount": 10000.00,
                "match_status": "完全匹配",
                "manual_flag": 0,
                "system_flag": 1,
                "source": "system",
                "case_id": "CASE00",
                "expected_anomaly_type": "正常样本",
            },
            expected_priority_hint="",
            note="正常样本，不应产生任何异常。",
            plan_row={"code": "sh.600010", "action": "buy", "shares": 1000, "price": 10.00},
            expected_review_owner="",
            should_track_next_day="否",
            expected_hit=1,
        )

        add_case(
            case_id="CASE01",
            code="sh.600011",
            expected_anomaly_type="计划漏发",
            row=None,
            expected_priority_hint="P1",
            note="计划存在，但对账明细缺失。",
            plan_row={"code": "sh.600011", "action": "buy", "shares": 1200, "price": 12.50},
            expected_review_owner="执行侧",
            should_track_next_day="是",
        )

        add_case(
            case_id="CASE02",
            code="sh.600012",
            expected_anomaly_type="委托未成交",
            row={
                "code": "sh.600012",
                "plan_action": "buy",
                "actual_action": "",
                "planned_shares": 2000,
                "actual_shares": np.nan,
                "planned_price": 8.20,
                "actual_price": np.nan,
                "planned_amount": 16400.00,
                "actual_amount": np.nan,
                "match_status": "未成交",
                "manual_flag": 0,
                "system_flag": 1,
                "source": "system",
                "case_id": "CASE02",
                "expected_anomaly_type": "委托未成交",
            },
            expected_priority_hint="P1",
            note="存在计划，但无有效成交。",
            plan_row={"code": "sh.600012", "action": "buy", "shares": 2000, "price": 8.20},
            expected_review_owner="执行侧",
            should_track_next_day="是",
        )

        add_case(
            case_id="CASE03",
            code="sh.600013",
            expected_anomaly_type="部分成交",
            row={
                "code": "sh.600013",
                "plan_action": "buy",
                "actual_action": "buy",
                "planned_shares": 1000,
                "actual_shares": 400,
                "planned_price": 15.00,
                "actual_price": 15.00,
                "planned_amount": 15000.00,
                "actual_amount": 6000.00,
                "match_status": "部分成交",
                "manual_flag": 0,
                "system_flag": 1,
                "source": "system",
                "case_id": "CASE03",
                "expected_anomaly_type": "部分成交",
            },
            expected_priority_hint="P2",
            note="成交完成度不足。",
            plan_row={"code": "sh.600013", "action": "buy", "shares": 1000, "price": 15.00},
            expected_review_owner="执行侧",
            should_track_next_day="是",
        )

        add_case(
            case_id="CASE04",
            code="sz.002701",
            expected_anomaly_type="手工临时交易",
            row={
                "code": "sz.002701",
                "plan_action": "",
                "actual_action": "buy",
                "planned_shares": np.nan,
                "actual_shares": 600,
                "planned_price": np.nan,
                "actual_price": 23.50,
                "planned_amount": np.nan,
                "actual_amount": 14100.00,
                "match_status": "人工成交",
                "manual_flag": 1,
                "system_flag": 1,
                "source": "manual_terminal",
                "case_id": "CASE04",
                "expected_anomaly_type": "手工临时交易",
            },
            expected_priority_hint="P1",
            note="无系统计划但存在人工临时成交。",
            expected_review_owner="人工操作侧",
            should_track_next_day="是",
        )

        add_case(
            case_id="CASE05",
            code="sz.002702",
            expected_anomaly_type="价格滑点超阈值",
            row={
                "code": "sz.002702",
                "plan_action": "buy",
                "actual_action": "buy",
                "planned_shares": 1000,
                "actual_shares": 1000,
                "planned_price": 10.00,
                "actual_price": 10.30,
                "planned_amount": 10000.00,
                "actual_amount": 10300.00,
                "match_status": "价格异常",
                "manual_flag": 0,
                "system_flag": 1,
                "source": "system",
                "case_id": "CASE05",
                "expected_anomaly_type": "价格滑点超阈值",
            },
            expected_priority_hint="P2",
            note="价格不利滑点超阈值。",
            plan_row={"code": "sz.002702", "action": "buy", "shares": 1000, "price": 10.00},
            expected_review_owner="执行侧",
            should_track_next_day="是",
        )

        add_case(
            case_id="CASE06",
            code="sz.002703",
            expected_anomaly_type="仓位执行偏差",
            row={
                "code": "sz.002703",
                "plan_action": "buy",
                "actual_action": "buy",
                "planned_shares": 1000,
                "actual_shares": 1200,
                "planned_price": 12.00,
                "actual_price": 12.00,
                "planned_amount": 12000.00,
                "actual_amount": 14400.00,
                "match_status": "仓位偏差",
                "manual_flag": 0,
                "system_flag": 1,
                "source": "system",
                "case_id": "CASE06",
                "expected_anomaly_type": "仓位执行偏差",
            },
            expected_priority_hint="P2",
            note="实际股数偏离目标仓位。",
            plan_row={"code": "sz.002703", "action": "buy", "shares": 1000, "price": 12.00},
            expected_review_owner="风控侧",
            should_track_next_day="是",
        )

        add_case(
            case_id="CASE07",
            code="sz.002704",
            expected_anomaly_type="非系统交易",
            row={
                "code": "sz.002704",
                "plan_action": "",
                "actual_action": "sell",
                "planned_shares": np.nan,
                "actual_shares": 800,
                "planned_price": np.nan,
                "actual_price": 18.60,
                "planned_amount": np.nan,
                "actual_amount": 14880.00,
                "match_status": "外部成交",
                "manual_flag": 0,
                "system_flag": 0,
                "source": "external_terminal",
                "case_id": "CASE07",
                "expected_anomaly_type": "非系统交易",
            },
            expected_priority_hint="P1",
            note="无系统计划但存在外部来源成交。",
            expected_review_owner="通道侧",
            should_track_next_day="是",
        )

        add_case(
            case_id="CASE08",
            code="sh.600014",
            expected_anomaly_type="方向执行异常",
            row={
                "code": "sh.600014",
                "plan_action": "buy",
                "actual_action": "sell",
                "planned_shares": 900,
                "actual_shares": 900,
                "planned_price": 21.00,
                "actual_price": 21.00,
                "planned_amount": 18900.00,
                "actual_amount": 18900.00,
                "match_status": "方向异常",
                "manual_flag": 0,
                "system_flag": 1,
                "source": "system",
                "case_id": "CASE08",
                "expected_anomaly_type": "方向执行异常",
            },
            expected_priority_hint="P1",
            note="计划方向与实际方向不一致。",
            plan_row={"code": "sh.600014", "action": "buy", "shares": 900, "price": 21.00},
            expected_review_owner="执行侧",
            should_track_next_day="是",
        )

        injected_detail_df = pd.DataFrame(injected_rows)
        expected_cases_df = pd.DataFrame(cases)
        execution_plan_df = pd.DataFrame(plans).drop_duplicates(subset=["code"], keep="first").reset_index(drop=True)

        injected_detail_df = injected_detail_df.sort_values(by=["code", "case_id"], ascending=[True, True]).reset_index(drop=True)
        expected_cases_df = expected_cases_df.sort_values(by=["case_id"], ascending=[True]).reset_index(drop=True)
        execution_plan_df = execution_plan_df.sort_values(by=["code"], ascending=[True]).reset_index(drop=True)
        return injected_detail_df, expected_cases_df, execution_plan_df
    def _run_attribution_layer(self) -> Dict[str, Any]:
        manager = self.attribution_manager_class(
            project_root=self.temp_project_root,
            trade_date=self.trade_date,
            slippage_threshold=self.config.slippage_threshold,
            position_deviation_threshold=self.config.position_deviation_threshold,
            full_fill_threshold=self.config.full_fill_threshold,
        )
        return manager.run()

    def _run_review_layer(self) -> Dict[str, Any]:
        manager = self.review_manager_class(
            project_root=self.temp_project_root,
            trade_date=self.trade_date,
        )
        return manager.run()

    # =========================
    # validation
    # =========================
    def _build_validation_detail_df(
        self,
        expected_cases_df: pd.DataFrame,
        attr_detail_df: pd.DataFrame,
        review_detail_df: pd.DataFrame,
    ) -> pd.DataFrame:
        columns = [
            "case_id",
            "code",
            "expected_anomaly_type",
            "expected_priority_hint",
            "expected_review_owner",
            "should_track_next_day",
            "case_note",
            "captured_anomaly_types",
            "captured_priorities",
            "captured_review_owners",
            "review_track_levels",
            "hit_flag",
            "priority_hint_match_flag",
            "review_owner_match_flag",
            "next_day_track_match_flag",
            "false_positive_flag",
            "validation_comment",
        ]
        if expected_cases_df.empty:
            return pd.DataFrame(columns=columns)

        attr_detail_df = attr_detail_df.copy() if not attr_detail_df.empty else pd.DataFrame(columns=["code", "anomaly_type", "priority"])
        review_detail_df = review_detail_df.copy() if not review_detail_df.empty else pd.DataFrame(columns=["code", "responsibility_owner", "next_day_track", "next_day_track_level"])

        records: List[Dict[str, Any]] = []
        for _, row in expected_cases_df.iterrows():
            code = str(row.get("code", "") or "").strip()
            expected_type = str(row.get("expected_anomaly_type", "") or "").strip()
            expected_priority_hint = str(row.get("expected_priority_hint", "") or "").strip()
            expected_review_owner = str(row.get("expected_review_owner", "") or "").strip()
            should_track_next_day = str(row.get("should_track_next_day", "") or "").strip()

            attr_rows = attr_detail_df[attr_detail_df.get("code", pd.Series(dtype=str)).astype(str).str.strip() == code].copy() if not attr_detail_df.empty else pd.DataFrame()
            review_rows = review_detail_df[review_detail_df.get("code", pd.Series(dtype=str)).astype(str).str.strip() == code].copy() if not review_detail_df.empty else pd.DataFrame()

            captured_anomaly_types = sorted(set(attr_rows.get("anomaly_type", pd.Series(dtype=str)).fillna("").astype(str).tolist())) if not attr_rows.empty else []
            captured_priorities = sorted(set(attr_rows.get("priority", pd.Series(dtype=str)).fillna("").astype(str).tolist())) if not attr_rows.empty else []
            captured_review_owners = sorted(set(review_rows.get("responsibility_owner", pd.Series(dtype=str)).fillna("").astype(str).tolist())) if not review_rows.empty else []
            review_track_levels = sorted(set(review_rows.get("next_day_track_level", pd.Series(dtype=str)).fillna("").astype(str).tolist())) if not review_rows.empty else []
            review_track_flags = sorted(set(review_rows.get("next_day_track", pd.Series(dtype=str)).fillna("").astype(str).tolist())) if not review_rows.empty else []

            if expected_type == "正常样本":
                hit_flag = int(len(captured_anomaly_types) == 0)
                false_positive_flag = int(len(captured_anomaly_types) > 0)
                priority_hint_match_flag = 1
                review_owner_match_flag = 1
                next_day_track_match_flag = int(len(review_track_flags) == 0)
                validation_comment = "正常样本未产生异常，验证通过。" if hit_flag == 1 else f"正常样本出现误报: {captured_anomaly_types}"
            else:
                hit_flag = int(expected_type in captured_anomaly_types)
                false_positive_flag = 0

                if expected_priority_hint == "":
                    priority_hint_match_flag = 1
                else:
                    priority_hint_match_flag = int(expected_priority_hint in captured_priorities) if captured_priorities else 0

                review_owner_match_flag = int((expected_review_owner == "") or (expected_review_owner in captured_review_owners))

                if should_track_next_day == "是":
                    next_day_track_match_flag = int(len([x for x in review_track_levels if x]) > 0)
                else:
                    next_day_track_match_flag = int(len(review_track_levels) == 0)

                validation_comment = self._build_validation_comment(
                    expected_type=expected_type,
                    hit_flag=hit_flag,
                    captured_anomaly_types=captured_anomaly_types,
                    priority_hint_match_flag=priority_hint_match_flag,
                    review_owner_match_flag=review_owner_match_flag,
                    next_day_track_match_flag=next_day_track_match_flag,
                )

            records.append({
                "case_id": row.get("case_id"),
                "code": code,
                "expected_anomaly_type": expected_type,
                "expected_priority_hint": expected_priority_hint,
                "expected_review_owner": expected_review_owner,
                "should_track_next_day": should_track_next_day,
                "case_note": row.get("case_note", ""),
                "captured_anomaly_types": " | ".join([x for x in captured_anomaly_types if x]),
                "captured_priorities": " | ".join([x for x in captured_priorities if x]),
                "captured_review_owners": " | ".join([x for x in captured_review_owners if x]),
                "review_track_levels": " | ".join([x for x in review_track_levels if x]),
                "hit_flag": hit_flag,
                "priority_hint_match_flag": priority_hint_match_flag,
                "review_owner_match_flag": review_owner_match_flag,
                "next_day_track_match_flag": next_day_track_match_flag,
                "false_positive_flag": false_positive_flag,
                "validation_comment": validation_comment,
            })

        out = pd.DataFrame(records)
        if out.empty:
            return pd.DataFrame(columns=columns)

        out["sort_order"] = out["expected_anomaly_type"].map({
            "计划漏发": 1,
            "方向执行异常": 2,
            "非系统交易": 3,
            "手工临时交易": 4,
            "委托未成交": 5,
            "部分成交": 6,
            "价格滑点超阈值": 7,
            "仓位执行偏差": 8,
            "正常样本": 9,
        }).fillna(99)
        out = out.sort_values(by=["sort_order", "case_id"], ascending=[True, True]).drop(columns=["sort_order"]).reset_index(drop=True)
        return out[columns]
    def _build_validation_summary_df(self, detail_df: pd.DataFrame) -> pd.DataFrame:
        columns = [
            "expected_anomaly_type",
            "case_count",
            "hit_count",
            "miss_count",
            "hit_rate",
            "priority_hint_match_count",
            "review_owner_match_count",
            "next_day_track_match_count",
            "false_positive_count",
        ]
        if detail_df.empty:
            return pd.DataFrame(columns=columns)

        tmp = detail_df.copy()
        summary = tmp.groupby("expected_anomaly_type", dropna=False).agg(
            case_count=("expected_anomaly_type", "size"),
            hit_count=("hit_flag", "sum"),
            priority_hint_match_count=("priority_hint_match_flag", "sum"),
            review_owner_match_count=("review_owner_match_flag", "sum"),
            next_day_track_match_count=("next_day_track_match_flag", "sum"),
            false_positive_count=("false_positive_flag", "sum"),
        ).reset_index()
        summary["miss_count"] = summary["case_count"] - summary["hit_count"]
        summary["hit_rate"] = np.where(summary["case_count"] > 0, summary["hit_count"] / summary["case_count"], np.nan)
        summary["sort_order"] = summary["expected_anomaly_type"].map({
            "计划漏发": 1,
            "委托未成交": 2,
            "部分成交": 3,
            "手工临时交易": 4,
            "价格滑点超阈值": 5,
            "仓位执行偏差": 6,
            "非系统交易": 7,
            "方向执行异常": 8,
            "正常样本": 9,
        }).fillna(99)
        summary = summary.sort_values(by=["sort_order", "expected_anomaly_type"], ascending=[True, True]).drop(columns=["sort_order"]).reset_index(drop=True)
        return summary[columns]

    def _build_validation_comment(
        self,
        expected_type: str,
        hit_flag: int,
        captured_anomaly_types: List[str],
        priority_hint_match_flag: int,
        review_owner_match_flag: int,
        next_day_track_match_flag: int,
    ) -> str:
        parts: List[str] = []
        if hit_flag == 1:
            parts.append(f"异常命中: {expected_type}。")
        else:
            if captured_anomaly_types:
                parts.append(f"未命中预期异常，实际捕获: {captured_anomaly_types}。")
            else:
                parts.append("未捕获到任何异常。")

        if priority_hint_match_flag == 0:
            parts.append("优先级提示未命中。")
        if review_owner_match_flag == 0:
            parts.append("责任归口未命中。")
        if next_day_track_match_flag == 0:
            parts.append("次日跟踪标记未命中。")
        return "".join(parts)

    def _normal_case_pass(self, validation_detail_df: pd.DataFrame) -> int:
        if validation_detail_df.empty:
            return 0
        normal_rows = validation_detail_df[validation_detail_df["expected_anomaly_type"] == "正常样本"]
        if normal_rows.empty:
            return 0
        return int((normal_rows["hit_flag"] == 1).all())

    # =========================
    # summary text
    # =========================
    def _build_summary_text(
        self,
        injected_detail_df: pd.DataFrame,
        expected_cases_df: pd.DataFrame,
        attr_detail_df: pd.DataFrame,
        review_detail_df: pd.DataFrame,
        validation_detail_df: pd.DataFrame,
        validation_summary_df: pd.DataFrame,
    ) -> str:
        lines: List[str] = []
        lines.append("=" * 60)
        lines.append("对账异常注入回放验证摘要")
        lines.append("=" * 60)
        lines.append(f"交易日: {self.trade_date}")
        lines.append(f"注入对账明细行数: {len(injected_detail_df)}")
        lines.append(f"预期验证案例数: {len(expected_cases_df)}")
        lines.append(f"归因层输出异常数: {len(attr_detail_df)}")
        lines.append(f"闭环复盘输出数: {len(review_detail_df)}")

        if validation_detail_df.empty:
            lines.append("验证结论: 无验证案例。")
            lines.append("=" * 60)
            return "\n".join(lines)

        effective_cases = validation_detail_df[validation_detail_df["expected_anomaly_type"] != "正常样本"].copy()
        hit_count = int(effective_cases["hit_flag"].sum()) if not effective_cases.empty else 0
        miss_count = int((1 - effective_cases["hit_flag"]).sum()) if not effective_cases.empty else 0
        overall_hit_rate = (hit_count / len(effective_cases)) if len(effective_cases) > 0 else np.nan
        normal_pass = self._normal_case_pass(validation_detail_df)

        lines.append(
            f"总体命中: {hit_count}/{len(effective_cases)} | 命中率={self._fmt_pct(overall_hit_rate)} | 漏判={miss_count} | 正常样本误报校验={'通过' if normal_pass == 1 else '失败'}"
        )
        lines.append("")
        lines.append("按异常类型命中统计:")
        for _, row in validation_summary_df.iterrows():
            lines.append(
                "- "
                f"{row['expected_anomaly_type']}: "
                f"案例={int(row['case_count'])} | 命中={int(row['hit_count'])} | 漏判={int(row['miss_count'])} | 命中率={self._fmt_pct(row['hit_rate'])} | "
                f"优先级匹配={int(row['priority_hint_match_count'])} | 责任归口匹配={int(row['review_owner_match_count'])} | 次日跟踪匹配={int(row['next_day_track_match_count'])} | 误报={int(row['false_positive_count'])}"
            )

        lines.append("")
        lines.append("案例验证明细:")
        for _, row in validation_detail_df.iterrows():
            lines.append(
                "- "
                f"{row['case_id']} {row['code']} 预期={row['expected_anomaly_type']} | 命中={int(row['hit_flag'])} | "
                f"实际={row['captured_anomaly_types'] or '无'} | 责任={row['captured_review_owners'] or '无'} | 跟踪级别={row['review_track_levels'] or '无'}"
            )

        lines.append("")
        if miss_count == 0 and normal_pass == 1:
            lines.append("结论: 第13.2~13.3链路在当前异常样本下验证通过，可作为后续字段变更后的回归测试基线。")
        else:
            lines.append("结论: 存在漏判或误报，需优先检查字段映射、对账状态口径与责任归口模板。")
        lines.append("=" * 60)
        return "\n".join(lines)
    # =========================
    # helper
    # =========================
    def _reset_temp_workspace(self) -> None:
        if self.temp_project_root.exists():
            shutil.rmtree(self.temp_project_root, ignore_errors=True)
        self.temp_reports_dir.mkdir(parents=True, exist_ok=True)

    def _read_csv_optional(self, path: Path) -> pd.DataFrame:
        if not path.exists():
            return pd.DataFrame()
        return self._read_csv_required(path)

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

    def _fmt_pct(self, value: Any) -> str:
        try:
            if pd.isna(value):
                return ""
            return f"{float(value):.2%}"
        except Exception:
            return ""
