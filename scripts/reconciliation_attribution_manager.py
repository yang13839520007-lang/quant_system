from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


@dataclass
class AttributionConfig:
    project_root: Path
    reports_dir: Path
    trade_date: Optional[str] = None
    slippage_threshold: float = 0.01
    position_deviation_threshold: float = 0.10
    full_fill_threshold: float = 0.98


class ReconciliationAttributionManager:
    """
    第13.2段：对账异常归因层

    输入：
    - reports/daily_trade_reconciliation_detail.csv（必需）
    - reports/daily_execution_plan.csv（可选）
    - reports/daily_portfolio_plan_risk_checked.csv（可选）
    - reports/daily_open_execution_decision.csv（可选）
    - reports/daily_intraday_recheck_decision.csv（可选）

    输出：
    - reports/daily_trade_reconciliation_attribution_detail.csv
    - reports/daily_trade_reconciliation_attribution_summary.csv
    - reports/daily_trade_reconciliation_attribution_priority.csv
    - reports/daily_trade_reconciliation_attribution_summary.txt
    """

    OUTPUT_DETAIL = "daily_trade_reconciliation_attribution_detail.csv"
    OUTPUT_SUMMARY = "daily_trade_reconciliation_attribution_summary.csv"
    OUTPUT_PRIORITY = "daily_trade_reconciliation_attribution_priority.csv"
    OUTPUT_TEXT = "daily_trade_reconciliation_attribution_summary.txt"

    RECON_DETAIL_NAME = "daily_trade_reconciliation_detail.csv"

    OPTIONAL_UPSTREAM_FILES = {
        "execution_plan": "daily_execution_plan.csv",
        "portfolio_risk_checked": "daily_portfolio_plan_risk_checked.csv",
        "open_execution_decision": "daily_open_execution_decision.csv",
        "intraday_recheck_decision": "daily_intraday_recheck_decision.csv",
    }

    COL_ALIASES = {
        "trade_date": ["trade_date", "date", "交易日期", "成交日期", "business_date"],
        "code": ["code", "ts_code", "symbol", "stock_code", "证券代码", "股票代码"],
        "name": ["name", "stock_name", "security_name", "证券名称", "股票名称"],
        "planned_action": [
            "planned_action", "plan_action", "target_action", "action_planned",
            "planned_side", "计划方向", "计划动作", "计划买卖", "system_action"
        ],
        "actual_action": [
            "actual_action", "executed_action", "trade_action", "filled_action",
            "actual_side", "成交方向", "实际方向", "实际动作", "买卖方向", "side"
        ],
        "planned_shares": [
            "planned_shares", "target_shares", "order_shares", "plan_shares",
            "计划股数", "计划数量", "计划委托数量", "应交易数量", "拟交易数量"
        ],
        "actual_shares": [
            "actual_shares", "filled_shares", "executed_shares", "trade_shares",
            "deal_shares", "actual_qty", "成交股数", "成交数量", "实际数量"
        ],
        "planned_price": [
            "planned_price", "plan_price", "order_price", "target_price",
            "entry_price", "planned_entry_price", "计划价格", "委托价格", "计划委托价"
        ],
        "actual_price": [
            "actual_price", "avg_fill_price", "fill_price", "deal_price",
            "avg_price", "trade_price", "成交均价", "成交价格", "实际价格"
        ],
        "match_status": [
            "match_status", "reconciliation_status", "execution_status",
            "对账状态", "匹配状态", "状态"
        ],
        "trade_source": [
            "trade_source", "source", "execution_source", "actual_source",
            "order_source", "成交来源", "来源", "source_type"
        ],
        "remarks": ["remarks", "remark", "note", "notes", "备注", "说明"],
        "plan_exists_flag": ["plan_exists", "has_plan", "in_plan", "计划存在标记"],
        "actual_exists_flag": ["actual_exists", "has_actual", "has_trade", "实际存在标记"],
    }

    UPSTREAM_COL_ALIASES = {
        "trade_date": ["trade_date", "date", "交易日期", "business_date"],
        "code": ["code", "ts_code", "symbol", "stock_code", "证券代码", "股票代码"],
        "name": ["name", "stock_name", "security_name", "证券名称", "股票名称"],
        "action": [
            "action", "planned_action", "plan_action", "next_day_action", "trade_action",
            "建议动作", "动作", "计划动作", "买卖方向"
        ],
        "shares": [
            "shares", "planned_shares", "target_shares", "suggested_shares",
            "order_shares", "position_shares", "数量", "股数", "计划股数", "建议股数"
        ],
        "price": [
            "price", "planned_price", "entry_price", "order_price",
            "计划价格", "委托价格", "买入价", "卖出价"
        ],
        "status": ["status", "decision", "执行决策", "状态", "action_status"],
        "remarks": ["remarks", "remark", "note", "notes", "备注", "说明"],
    }

    BOOLEAN_TRUE_SET = {"1", "true", "yes", "y", "是", "有", "已生成", "已下发", "存在"}
    MANUAL_KEYWORDS = ("manual", "hand", "人工", "手工", "临时", "盘中临时", "discretionary")
    SYSTEM_KEYWORDS = ("system", "algo", "自动", "系统", "策略", "quant", "程序")

    ACTION_BUY = {"buy", "b", "long", "open_long", "加仓", "买入", "开仓", "建仓"}
    ACTION_SELL = {"sell", "s", "short", "close_long", "减仓", "卖出", "平仓", "清仓"}

    ATTR_BASE_SCORE = {
        "方向执行异常": 95,
        "非系统交易": 92,
        "手工临时交易": 90,
        "计划漏发": 88,
        "委托未成交": 82,
        "仓位执行偏差": 76,
        "部分成交": 72,
        "价格滑点超阈值": 68,
    }

    ROOT_CAUSE_HINT = {
        "计划漏发": "上游文件存在交易意图，但未进入最终对账计划，优先检查字段级合并、过滤条件、代码映射与计划下发链路。",
        "委托未成交": "有计划无成交，优先检查委托价格偏离、涨跌停约束、流动性不足、风控拦截或委托未发出。",
        "部分成交": "计划已执行但成交不完整，优先检查盘口流动性、分笔委托、撤单逻辑与成交回报回写。",
        "手工临时交易": "存在成交但无系统计划，且来源指向人工/临时处理，应核查是否为人工干预或应急交易。",
        "价格滑点超阈值": "实际成交价偏离计划价超过阈值，优先检查委托价设置、撮合深度、开盘跳空与追价策略。",
        "仓位执行偏差": "实际成交数量与计划数量偏离超阈值，优先检查拆单策略、成交回报、最小交易单位与风控截断。",
        "非系统交易": "存在成交但无系统计划，且来源无法归于正常系统链路，应重点排查券商侧/人工侧额外交易。",
        "方向执行异常": "计划方向与实际成交方向相反或不一致，属于高优先级执行异常，需优先排查委托方向映射。",
    }

    SUGGESTED_ACTION = {
        "计划漏发": "复核 execution_plan 与 reconciliation_detail 的字段级合并，补查漏发标的并回溯链路。",
        "委托未成交": "回看委托日志与盘口，评估是否需要放宽限价、增加重试或增加未成交兜底逻辑。",
        "部分成交": "检查拆单与撤单阈值，必要时补充剩余量跟踪与成交完成率约束。",
        "手工临时交易": "将人工成交单独沉淀到人工交易台账，并加入事后审批/备注字段。",
        "价格滑点超阈值": "按标的与时段统计滑点，分离开盘/盘中场景，单独优化价格追踪与限价策略。",
        "仓位执行偏差": "核对 planned_shares 生成逻辑与最小买卖单位对齐，防止截断误差扩散。",
        "非系统交易": "建立非系统成交强提醒，要求逐笔补录原因与责任人。",
        "方向执行异常": "立即检查买卖方向映射与券商接口字段，阻断同类异常再次发生。",
    }

    def __init__(
        self,
        project_root: str | Path = r"C:\quant_system",
        trade_date: Optional[str] = None,
        slippage_threshold: float = 0.01,
        position_deviation_threshold: float = 0.10,
        full_fill_threshold: float = 0.98,
    ) -> None:
        self.config = AttributionConfig(
            project_root=Path(project_root),
            reports_dir=Path(project_root) / "reports",
            trade_date=trade_date,
            slippage_threshold=slippage_threshold,
            position_deviation_threshold=position_deviation_threshold,
            full_fill_threshold=full_fill_threshold,
        )

    def run(self) -> Dict[str, object]:
        recon_path = self.config.reports_dir / self.RECON_DETAIL_NAME
        if not recon_path.exists():
            raise FileNotFoundError(f"未找到对账明细文件: {recon_path}")

        recon_raw = self._read_table(recon_path)
        recon = self._normalize_recon_detail(recon_raw)

        upstream_dict = self._load_upstream_inputs()
        missing_plan_df = self._detect_missing_plan_from_upstreams(recon, upstream_dict)

        detail_rows: List[Dict[str, object]] = []
        detail_rows.extend(self._build_missing_plan_rows(missing_plan_df))
        detail_rows.extend(self._build_row_level_anomalies(recon))

        attribution_detail = pd.DataFrame(detail_rows)
        attribution_detail = self._finalize_detail(attribution_detail)

        summary_df = self._build_summary(attribution_detail)
        priority_df = self._build_priority(attribution_detail)
        summary_text = self._build_summary_text(recon, attribution_detail, summary_df, priority_df)

        output_paths = self._save_outputs(attribution_detail, summary_df, priority_df, summary_text)

        result = {
            "recon_rows": int(len(recon)),
            "anomaly_rows": int(len(attribution_detail)),
            "anomaly_codes": int(attribution_detail["code"].nunique()) if not attribution_detail.empty else 0,
            "detail_path": str(output_paths["detail"]),
            "summary_path": str(output_paths["summary"]),
            "priority_path": str(output_paths["priority"]),
            "text_path": str(output_paths["text"]),
            "summary_text": summary_text,
        }
        return result

    def _read_table(self, path: Path) -> pd.DataFrame:
        suffix = path.suffix.lower()
        if suffix == ".xlsx":
            return pd.read_excel(path)

        last_error = None
        for enc in ("utf-8-sig", "utf-8", "gb18030", "gbk"):
            try:
                return pd.read_csv(path, encoding=enc)
            except Exception as exc:
                last_error = exc
        raise ValueError(f"无法读取文件: {path} | 最后错误: {last_error}")

    def _normalize_recon_detail(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        out.columns = [str(c).strip() for c in out.columns]

        for canonical, aliases in self.COL_ALIASES.items():
            src = self._first_existing_col(out, aliases)
            if src is not None and src != canonical:
                out[canonical] = out[src]
            elif src is None and canonical not in out.columns:
                out[canonical] = np.nan

        out["code"] = out["code"].apply(self._normalize_code)
        out["name"] = out["name"].fillna("").astype(str).str.strip()
        out["planned_action"] = out["planned_action"].apply(self._normalize_action_text)
        out["actual_action"] = out["actual_action"].apply(self._normalize_action_text)
        out["planned_shares"] = self._to_numeric(out["planned_shares"])
        out["actual_shares"] = self._to_numeric(out["actual_shares"])
        out["planned_price"] = self._to_numeric(out["planned_price"])
        out["actual_price"] = self._to_numeric(out["actual_price"])
        out["trade_source"] = out["trade_source"].fillna("").astype(str).str.strip()
        out["remarks"] = out["remarks"].fillna("").astype(str).str.strip()
        out["match_status"] = out["match_status"].fillna("").astype(str).str.strip()

        out["planned_exists"] = out.apply(self._infer_plan_exists, axis=1)
        out["actual_exists"] = out.apply(self._infer_actual_exists, axis=1)

        if out["trade_date"].isna().all() and self.config.trade_date:
            out["trade_date"] = self.config.trade_date
        elif not out["trade_date"].isna().all():
            out["trade_date"] = out["trade_date"].astype(str).str.strip()

        out["planned_notional"] = (out["planned_shares"].abs() * out["planned_price"]).fillna(0.0)
        out["actual_notional"] = (out["actual_shares"].abs() * out["actual_price"]).fillna(0.0)
        out["share_gap"] = (out["actual_shares"].fillna(0.0) - out["planned_shares"].fillna(0.0)).abs()
        out["notional_gap"] = (out["actual_notional"] - out["planned_notional"]).abs()

        out["direction_match"] = out.apply(self._direction_match, axis=1)
        out["slippage_pct"] = out.apply(self._calc_slippage_pct, axis=1)
        out["execution_ratio"] = out.apply(self._calc_execution_ratio, axis=1)
        out["position_deviation_pct"] = out["execution_ratio"].apply(
            lambda x: np.nan if pd.isna(x) else abs(x - 1.0)
        )

        return out

    def _normalize_upstream_df(self, df: pd.DataFrame, source_name: str) -> pd.DataFrame:
        out = df.copy()
        out.columns = [str(c).strip() for c in out.columns]

        for canonical, aliases in self.UPSTREAM_COL_ALIASES.items():
            src = self._first_existing_col(out, aliases)
            if src is not None and src != canonical:
                out[canonical] = out[src]
            elif src is None and canonical not in out.columns:
                out[canonical] = np.nan

        out["source_file"] = source_name
        out["code"] = out["code"].apply(self._normalize_code)
        out["name"] = out["name"].fillna("").astype(str).str.strip()
        out["action"] = out["action"].apply(self._normalize_action_text)
        out["shares"] = self._to_numeric(out["shares"])
        out["price"] = self._to_numeric(out["price"])
        out["status"] = out["status"].fillna("").astype(str).str.strip()
        out["remarks"] = out["remarks"].fillna("").astype(str).str.strip()

        if out["trade_date"].isna().all() and self.config.trade_date:
            out["trade_date"] = self.config.trade_date
        elif not out["trade_date"].isna().all():
            out["trade_date"] = out["trade_date"].astype(str).str.strip()

        out["upstream_trade_intent"] = out.apply(self._infer_upstream_trade_intent, axis=1)
        out = out[out["code"] != ""].copy()
        return out

    def _load_upstream_inputs(self) -> Dict[str, pd.DataFrame]:
        result: Dict[str, pd.DataFrame] = {}
        for key, filename in self.OPTIONAL_UPSTREAM_FILES.items():
            path = self.config.reports_dir / filename
            if not path.exists():
                continue
            raw = self._read_table(path)
            result[key] = self._normalize_upstream_df(raw, filename)
        return result

    def _build_row_level_anomalies(self, recon: pd.DataFrame) -> List[Dict[str, object]]:
        rows: List[Dict[str, object]] = []

        for _, r in recon.iterrows():
            planned_exists = bool(r["planned_exists"])
            actual_exists = bool(r["actual_exists"])
            manual_trade = self._is_manual_trade(r)

            if planned_exists and actual_exists and (r["direction_match"] is False):
                rows.append(self._make_detail_row(r, "方向执行异常"))

            if planned_exists and not actual_exists:
                rows.append(self._make_detail_row(r, "委托未成交"))

            if planned_exists and actual_exists:
                exe_ratio = r["execution_ratio"]
                if not pd.isna(exe_ratio) and 0 < exe_ratio < self.config.full_fill_threshold:
                    rows.append(self._make_detail_row(r, "部分成交"))

            if actual_exists and not planned_exists:
                if manual_trade:
                    rows.append(self._make_detail_row(r, "手工临时交易"))
                else:
                    rows.append(self._make_detail_row(r, "非系统交易"))

            slip = r["slippage_pct"]
            if actual_exists and planned_exists and not pd.isna(slip):
                if abs(slip) >= self.config.slippage_threshold:
                    rows.append(self._make_detail_row(r, "价格滑点超阈值"))

            dev = r["position_deviation_pct"]
            if planned_exists and actual_exists and not pd.isna(dev):
                if dev >= self.config.position_deviation_threshold:
                    rows.append(self._make_detail_row(r, "仓位执行偏差"))

        return rows

    def _detect_missing_plan_from_upstreams(
        self,
        recon: pd.DataFrame,
        upstream_dict: Dict[str, pd.DataFrame],
    ) -> pd.DataFrame:
        if not upstream_dict:
            return pd.DataFrame(columns=["trade_date", "code", "name", "action", "shares", "price", "source_file", "remarks"])

        upstream_all = pd.concat(list(upstream_dict.values()), ignore_index=True)
        upstream_all = upstream_all[upstream_all["upstream_trade_intent"]].copy()
        if upstream_all.empty:
            return upstream_all

        priority_rank = {
            self.OPTIONAL_UPSTREAM_FILES["intraday_recheck_decision"]: 4,
            self.OPTIONAL_UPSTREAM_FILES["open_execution_decision"]: 3,
            self.OPTIONAL_UPSTREAM_FILES["execution_plan"]: 2,
            self.OPTIONAL_UPSTREAM_FILES["portfolio_risk_checked"]: 1,
        }
        upstream_all["source_rank"] = upstream_all["source_file"].map(priority_rank).fillna(0).astype(int)
        upstream_all = upstream_all.sort_values(
            by=["code", "source_rank", "shares"],
            ascending=[True, False, False],
        )

        latest_intent = upstream_all.groupby("code", as_index=False).first()

        planned_codes = set(recon.loc[recon["planned_exists"], "code"].dropna().tolist())
        latest_intent = latest_intent[~latest_intent["code"].isin(planned_codes)].copy()

        if latest_intent.empty:
            return latest_intent

        return latest_intent

    def _build_missing_plan_rows(self, missing_plan_df: pd.DataFrame) -> List[Dict[str, object]]:
        rows: List[Dict[str, object]] = []
        if missing_plan_df is None or missing_plan_df.empty:
            return rows

        for _, r in missing_plan_df.iterrows():
            payload = {
                "trade_date": r.get("trade_date", self.config.trade_date),
                "code": r.get("code", ""),
                "name": r.get("name", ""),
                "anomaly_type": "计划漏发",
                "priority_score": self._calc_priority_score(
                    anomaly_type="计划漏发",
                    planned_shares=r.get("shares", 0),
                    actual_shares=0,
                    planned_price=r.get("price", 0),
                    actual_price=0,
                    slippage_pct=np.nan,
                    position_deviation_pct=np.nan,
                ),
                "priority_level": "",
                "planned_action": r.get("action", ""),
                "actual_action": "",
                "planned_shares": float(r.get("shares", 0) or 0),
                "actual_shares": 0.0,
                "planned_price": float(r.get("price", 0) or 0),
                "actual_price": 0.0,
                "planned_notional": float((r.get("shares", 0) or 0) * (r.get("price", 0) or 0)),
                "actual_notional": 0.0,
                "share_gap": float(abs(r.get("shares", 0) or 0)),
                "notional_gap": float(abs((r.get("shares", 0) or 0) * (r.get("price", 0) or 0))),
                "execution_ratio": np.nan,
                "position_deviation_pct": np.nan,
                "slippage_pct": np.nan,
                "match_status": "计划漏发",
                "trade_source": "",
                "upstream_source": r.get("source_file", ""),
                "root_cause_hint": self.ROOT_CAUSE_HINT["计划漏发"],
                "suggested_action": self.SUGGESTED_ACTION["计划漏发"],
                "severity_rank": self.ATTR_BASE_SCORE["计划漏发"],
                "remarks": r.get("remarks", ""),
                "evidence": f"上游文件 {r.get('source_file', '')} 存在交易意图，但 reconciliation_detail 未发现该 code 的计划记录。",
            }
            rows.append(payload)
        return rows

    def _make_detail_row(self, r: pd.Series, anomaly_type: str) -> Dict[str, object]:
        payload = {
            "trade_date": r.get("trade_date", self.config.trade_date),
            "code": r.get("code", ""),
            "name": r.get("name", ""),
            "anomaly_type": anomaly_type,
            "priority_score": self._calc_priority_score(
                anomaly_type=anomaly_type,
                planned_shares=r.get("planned_shares", 0),
                actual_shares=r.get("actual_shares", 0),
                planned_price=r.get("planned_price", 0),
                actual_price=r.get("actual_price", 0),
                slippage_pct=r.get("slippage_pct", np.nan),
                position_deviation_pct=r.get("position_deviation_pct", np.nan),
            ),
            "priority_level": "",
            "planned_action": r.get("planned_action", ""),
            "actual_action": r.get("actual_action", ""),
            "planned_shares": float(r.get("planned_shares", 0) or 0),
            "actual_shares": float(r.get("actual_shares", 0) or 0),
            "planned_price": float(r.get("planned_price", 0) or 0),
            "actual_price": float(r.get("actual_price", 0) or 0),
            "planned_notional": float(r.get("planned_notional", 0) or 0),
            "actual_notional": float(r.get("actual_notional", 0) or 0),
            "share_gap": float(r.get("share_gap", 0) or 0),
            "notional_gap": float(r.get("notional_gap", 0) or 0),
            "execution_ratio": r.get("execution_ratio", np.nan),
            "position_deviation_pct": r.get("position_deviation_pct", np.nan),
            "slippage_pct": r.get("slippage_pct", np.nan),
            "match_status": r.get("match_status", ""),
            "trade_source": r.get("trade_source", ""),
            "upstream_source": "",
            "root_cause_hint": self.ROOT_CAUSE_HINT.get(anomaly_type, ""),
            "suggested_action": self.SUGGESTED_ACTION.get(anomaly_type, ""),
            "severity_rank": self.ATTR_BASE_SCORE.get(anomaly_type, 50),
            "remarks": r.get("remarks", ""),
            "evidence": self._build_evidence_text(r, anomaly_type),
        }
        return payload

    def _finalize_detail(self, df: pd.DataFrame) -> pd.DataFrame:
        expected_cols = [
            "trade_date", "code", "name", "anomaly_type",
            "priority_score", "priority_level", "severity_rank",
            "planned_action", "actual_action",
            "planned_shares", "actual_shares", "share_gap", "execution_ratio", "position_deviation_pct",
            "planned_price", "actual_price", "slippage_pct",
            "planned_notional", "actual_notional", "notional_gap",
            "match_status", "trade_source", "upstream_source",
            "root_cause_hint", "suggested_action", "evidence", "remarks",
        ]
        if df.empty:
            return pd.DataFrame(columns=expected_cols)

        out = df.copy()
        out["priority_score"] = self._to_numeric(out["priority_score"]).fillna(0).round(0).astype(int)
        out["priority_level"] = out["priority_score"].apply(self._priority_level)
        out["trade_date"] = out["trade_date"].fillna(self.config.trade_date if self.config.trade_date else "").astype(str)

        numeric_cols = [
            "planned_shares", "actual_shares", "share_gap", "execution_ratio", "position_deviation_pct",
            "planned_price", "actual_price", "slippage_pct",
            "planned_notional", "actual_notional", "notional_gap",
        ]
        for c in numeric_cols:
            out[c] = self._to_numeric(out[c])

        out = out.sort_values(
            by=["priority_score", "severity_rank", "notional_gap", "share_gap", "code", "anomaly_type"],
            ascending=[False, False, False, False, True, True],
        ).reset_index(drop=True)

        out.insert(0, "priority_rank", np.arange(1, len(out) + 1))
        out = out[["priority_rank"] + expected_cols]
        return out

    def _build_summary(self, detail_df: pd.DataFrame) -> pd.DataFrame:
        columns = [
            "anomaly_type", "anomaly_count", "code_count",
            "p1_count", "p2_count", "p3_count", "p4_count",
            "planned_shares_sum", "actual_shares_sum", "share_gap_sum",
            "planned_notional_sum", "actual_notional_sum", "notional_gap_sum",
            "avg_execution_ratio", "avg_position_deviation_pct", "avg_slippage_pct",
            "max_priority_score",
        ]
        if detail_df.empty:
            return pd.DataFrame(columns=columns)

        tmp = detail_df.copy()
        tmp["p1_count"] = (tmp["priority_level"] == "P1").astype(int)
        tmp["p2_count"] = (tmp["priority_level"] == "P2").astype(int)
        tmp["p3_count"] = (tmp["priority_level"] == "P3").astype(int)
        tmp["p4_count"] = (tmp["priority_level"] == "P4").astype(int)

        summary = (
            tmp.groupby("anomaly_type", as_index=False)
            .agg(
                anomaly_count=("code", "size"),
                code_count=("code", "nunique"),
                p1_count=("p1_count", "sum"),
                p2_count=("p2_count", "sum"),
                p3_count=("p3_count", "sum"),
                p4_count=("p4_count", "sum"),
                planned_shares_sum=("planned_shares", "sum"),
                actual_shares_sum=("actual_shares", "sum"),
                share_gap_sum=("share_gap", "sum"),
                planned_notional_sum=("planned_notional", "sum"),
                actual_notional_sum=("actual_notional", "sum"),
                notional_gap_sum=("notional_gap", "sum"),
                avg_execution_ratio=("execution_ratio", "mean"),
                avg_position_deviation_pct=("position_deviation_pct", "mean"),
                avg_slippage_pct=("slippage_pct", "mean"),
                max_priority_score=("priority_score", "max"),
            )
        )

        summary = summary.sort_values(
            by=["max_priority_score", "anomaly_count", "notional_gap_sum"],
            ascending=[False, False, False],
        ).reset_index(drop=True)
        return summary[columns]

    def _build_priority(self, detail_df: pd.DataFrame) -> pd.DataFrame:
        if detail_df.empty:
            return detail_df.copy()

        cols = [
            "priority_rank", "priority_level", "priority_score", "anomaly_type",
            "trade_date", "code", "name",
            "planned_action", "actual_action",
            "planned_shares", "actual_shares", "share_gap",
            "planned_price", "actual_price", "slippage_pct",
            "execution_ratio", "position_deviation_pct",
            "planned_notional", "actual_notional", "notional_gap",
            "trade_source", "upstream_source", "match_status",
            "root_cause_hint", "suggested_action", "evidence", "remarks",
        ]
        return detail_df[cols].copy()

    def _build_summary_text(
        self,
        recon_df: pd.DataFrame,
        detail_df: pd.DataFrame,
        summary_df: pd.DataFrame,
        priority_df: pd.DataFrame,
    ) -> str:
        trade_date = self.config.trade_date
        if not trade_date and "trade_date" in recon_df.columns and not recon_df["trade_date"].isna().all():
            trade_date = str(recon_df["trade_date"].dropna().iloc[0])

        total_recon = len(recon_df)
        anomaly_count = len(detail_df)
        anomaly_codes = detail_df["code"].nunique() if not detail_df.empty else 0
        p1 = int((detail_df["priority_level"] == "P1").sum()) if not detail_df.empty else 0
        p2 = int((detail_df["priority_level"] == "P2").sum()) if not detail_df.empty else 0
        p3 = int((detail_df["priority_level"] == "P3").sum()) if not detail_df.empty else 0
        p4 = int((detail_df["priority_level"] == "P4").sum()) if not detail_df.empty else 0

        lines = []
        lines.append("============================================================")
        lines.append("对账异常归因摘要")
        lines.append("============================================================")
        lines.append(f"交易日: {trade_date or ''}")
        lines.append(f"对账明细记录数: {total_recon}")
        lines.append(f"异常条数: {anomaly_count}")
        lines.append(f"涉及异常标的数: {anomaly_codes}")
        lines.append(f"优先级分布: P1={p1} | P2={p2} | P3={p3} | P4={p4}")
        lines.append("")

        if detail_df.empty:
            lines.append("结论: 未识别到对账异常。")
            lines.append("============================================================")
            return "\n".join(lines)

        lines.append("异常类型汇总:")
        for _, r in summary_df.iterrows():
            avg_slip = self._fmt_pct(r["avg_slippage_pct"])
            avg_dev = self._fmt_pct(r["avg_position_deviation_pct"])
            lines.append(
                f"- {r['anomaly_type']}: {int(r['anomaly_count'])}条 | "
                f"{int(r['code_count'])}只 | "
                f"P1={int(r['p1_count'])}/P2={int(r['p2_count'])}/P3={int(r['p3_count'])}/P4={int(r['p4_count'])} | "
                f"数量偏差={r['share_gap_sum']:.0f}股 | "
                f"金额偏差={r['notional_gap_sum']:.2f} | "
                f"均值滑点={avg_slip} | 均值仓位偏差={avg_dev}"
            )

        top_n = min(10, len(priority_df))
        lines.append("")
        lines.append(f"优先处理清单 TOP{top_n}:")
        top_df = priority_df.head(top_n)
        for _, r in top_df.iterrows():
            slip = self._fmt_pct(r["slippage_pct"])
            dev = self._fmt_pct(r["position_deviation_pct"])
            lines.append(
                f"- [{r['priority_level']}/{int(r['priority_score'])}] {r['code']} {r['anomaly_type']} | "
                f"计划={r['planned_action']} {r['planned_shares']:.0f}股 @{self._fmt_float(r['planned_price'], 3, '0.000')} | "
                f"实际={r['actual_action']} {r['actual_shares']:.0f}股 @{self._fmt_float(r['actual_price'], 3, '0.000')} | "
                f"滑点={slip} | 仓位偏差={dev}"
            )

        lines.append("")
        lines.append("归因建议:")
        unique_types = priority_df["anomaly_type"].dropna().astype(str).unique().tolist()
        for anomaly_type in unique_types:
            lines.append(f"- {anomaly_type}: {self.SUGGESTED_ACTION.get(anomaly_type, '')}")

        lines.append("============================================================")
        return "\n".join(lines)

    def _save_outputs(
        self,
        detail_df: pd.DataFrame,
        summary_df: pd.DataFrame,
        priority_df: pd.DataFrame,
        summary_text: str,
    ) -> Dict[str, Path]:
        self.config.reports_dir.mkdir(parents=True, exist_ok=True)

        detail_path = self.config.reports_dir / self.OUTPUT_DETAIL
        summary_path = self.config.reports_dir / self.OUTPUT_SUMMARY
        priority_path = self.config.reports_dir / self.OUTPUT_PRIORITY
        text_path = self.config.reports_dir / self.OUTPUT_TEXT

        detail_df.to_csv(detail_path, index=False, encoding="utf-8-sig")
        summary_df.to_csv(summary_path, index=False, encoding="utf-8-sig")
        priority_df.to_csv(priority_path, index=False, encoding="utf-8-sig")
        text_path.write_text(summary_text, encoding="utf-8")

        return {
            "detail": detail_path,
            "summary": summary_path,
            "priority": priority_path,
            "text": text_path,
        }

    def _first_existing_col(self, df: pd.DataFrame, aliases: List[str]) -> Optional[str]:
        lower_map = {str(c).strip().lower(): c for c in df.columns}
        for alias in aliases:
            if alias in df.columns:
                return alias
            if str(alias).strip().lower() in lower_map:
                return lower_map[str(alias).strip().lower()]
        return None

    def _to_numeric(self, s) -> pd.Series:
        return pd.to_numeric(s, errors="coerce")

    def _normalize_code(self, x) -> str:
        if pd.isna(x):
            return ""
        val = str(x).strip().lower().replace(" ", "")
        if not val:
            return ""

        if "." in val:
            left, right = val.split(".", 1)
            if left in {"sh", "sz", "bj"} and right.isdigit():
                return f"{left}.{right.zfill(6)}"
            if right in {"sh", "sz", "bj"} and left.isdigit():
                return f"{right}.{left.zfill(6)}"

        digits = "".join(ch for ch in val if ch.isdigit())
        if len(digits) == 6:
            if digits.startswith(("60", "68", "90")):
                return f"sh.{digits}"
            if digits.startswith(("00", "30")):
                return f"sz.{digits}"
            if digits.startswith(("83", "87", "43", "92")):
                return f"bj.{digits}"
            return digits
        return val

    def _normalize_action_text(self, x) -> str:
        if pd.isna(x):
            return ""
        val = str(x).strip().lower()
        if not val or val in {"nan", "none", "null"}:
            return ""
        if val in self.ACTION_BUY:
            return "buy"
        if val in self.ACTION_SELL:
            return "sell"
        if any(k in val for k in ("买", "加仓", "开仓", "建仓")):
            return "buy"
        if any(k in val for k in ("卖", "减仓", "平仓", "清仓")):
            return "sell"
        if val in {"hold", "正常跟踪", "观察", "watch"}:
            return "hold"
        return val

    def _is_true(self, x) -> bool:
        if pd.isna(x):
            return False
        val = str(x).strip().lower()
        if val in self.BOOLEAN_TRUE_SET:
            return True
        try:
            return float(val) != 0
        except Exception:
            return False

    def _infer_plan_exists(self, row: pd.Series) -> bool:
        if self._is_true(row.get("plan_exists_flag", np.nan)):
            return True
        planned_shares = row.get("planned_shares", np.nan)
        planned_action = row.get("planned_action", "")
        planned_price = row.get("planned_price", np.nan)

        if not pd.isna(planned_shares) and abs(float(planned_shares)) > 0:
            return True
        if planned_action in {"buy", "sell"}:
            return True
        if not pd.isna(planned_price) and float(planned_price) > 0:
            return True
        return False

    def _infer_actual_exists(self, row: pd.Series) -> bool:
        if self._is_true(row.get("actual_exists_flag", np.nan)):
            return True
        actual_shares = row.get("actual_shares", np.nan)
        actual_price = row.get("actual_price", np.nan)

        if not pd.isna(actual_shares) and abs(float(actual_shares)) > 0:
            return True
        if not pd.isna(actual_price) and float(actual_price) > 0:
            return True
        return False

    def _infer_upstream_trade_intent(self, row: pd.Series) -> bool:
        action = row.get("action", "")
        shares = row.get("shares", np.nan)
        status = str(row.get("status", "")).strip().lower()
        remarks = str(row.get("remarks", "")).strip().lower()

        if action in {"buy", "sell"}:
            return True
        if not pd.isna(shares) and abs(float(shares)) > 0:
            return True
        text = " ".join([status, remarks])
        for kw in ("buy", "sell", "买入", "卖出", "执行", "下单", "交易", "减仓", "加仓"):
            if kw in text:
                return True
        return False

    def _action_sign(self, action: str) -> int:
        if action == "buy":
            return 1
        if action == "sell":
            return -1
        return 0

    def _direction_match(self, row: pd.Series) -> Optional[bool]:
        p = self._action_sign(row.get("planned_action", ""))
        a = self._action_sign(row.get("actual_action", ""))
        if p == 0 or a == 0:
            return np.nan
        return p == a

    def _calc_slippage_pct(self, row: pd.Series) -> float:
        planned_price = row.get("planned_price", np.nan)
        actual_price = row.get("actual_price", np.nan)
        if pd.isna(planned_price) or pd.isna(actual_price) or float(planned_price) == 0:
            return np.nan
        return (float(actual_price) - float(planned_price)) / float(planned_price)

    def _calc_execution_ratio(self, row: pd.Series) -> float:
        planned_shares = row.get("planned_shares", np.nan)
        actual_shares = row.get("actual_shares", np.nan)
        if pd.isna(planned_shares) or float(planned_shares) == 0:
            return np.nan
        if pd.isna(actual_shares):
            return 0.0
        return abs(float(actual_shares)) / abs(float(planned_shares))

    def _is_manual_trade(self, row: pd.Series) -> bool:
        text = " ".join(
            [
                str(row.get("trade_source", "") or ""),
                str(row.get("remarks", "") or ""),
                str(row.get("match_status", "") or ""),
            ]
        ).lower()
        return any(k in text for k in self.MANUAL_KEYWORDS)

    def _calc_priority_score(
        self,
        anomaly_type: str,
        planned_shares: float,
        actual_shares: float,
        planned_price: float,
        actual_price: float,
        slippage_pct: float,
        position_deviation_pct: float,
    ) -> int:
        base = int(self.ATTR_BASE_SCORE.get(anomaly_type, 50))
        planned_shares = 0 if pd.isna(planned_shares) else float(planned_shares)
        actual_shares = 0 if pd.isna(actual_shares) else float(actual_shares)
        planned_price = 0 if pd.isna(planned_price) else float(planned_price)
        actual_price = 0 if pd.isna(actual_price) else float(actual_price)

        planned_notional = abs(planned_shares) * planned_price
        actual_notional = abs(actual_shares) * actual_price
        notional_gap = abs(actual_notional - planned_notional)

        score = base
        if notional_gap >= 200000:
            score += 8
        elif notional_gap >= 100000:
            score += 6
        elif notional_gap >= 50000:
            score += 4
        elif notional_gap >= 10000:
            score += 2

        if not pd.isna(slippage_pct):
            slip_abs = abs(float(slippage_pct))
            if slip_abs >= self.config.slippage_threshold * 3:
                score += 6
            elif slip_abs >= self.config.slippage_threshold * 2:
                score += 4
            elif slip_abs >= self.config.slippage_threshold:
                score += 2

        if not pd.isna(position_deviation_pct):
            dev_abs = abs(float(position_deviation_pct))
            if dev_abs >= self.config.position_deviation_threshold * 3:
                score += 6
            elif dev_abs >= self.config.position_deviation_threshold * 2:
                score += 4
            elif dev_abs >= self.config.position_deviation_threshold:
                score += 2

        return int(min(score, 100))

    def _priority_level(self, score: int) -> str:
        if score >= 90:
            return "P1"
        if score >= 80:
            return "P2"
        if score >= 70:
            return "P3"
        return "P4"

    def _fmt_float(self, x, digits: int = 3, default: str = "") -> str:
        if pd.isna(x):
            return default
        return f"{float(x):.{digits}f}"

    def _fmt_pct(self, x, default: str = "") -> str:
        if pd.isna(x):
            return default
        return f"{float(x):.2%}"

    def _build_evidence_text(self, row: pd.Series, anomaly_type: str) -> str:
        planned_action = row.get("planned_action", "")
        actual_action = row.get("actual_action", "")
        planned_shares = row.get("planned_shares", np.nan)
        actual_shares = row.get("actual_shares", np.nan)
        planned_price = row.get("planned_price", np.nan)
        actual_price = row.get("actual_price", np.nan)
        slip = row.get("slippage_pct", np.nan)
        dev = row.get("position_deviation_pct", np.nan)

        slip_text = "" if pd.isna(slip) else f"{float(slip):.2%}"
        dev_text = "" if pd.isna(dev) else f"{float(dev):.2%}"

        if anomaly_type == "方向执行异常":
            return (
                f"计划方向={planned_action}，实际方向={actual_action}；"
                f"计划数量={0 if pd.isna(planned_shares) else float(planned_shares):.0f}，"
                f"实际数量={0 if pd.isna(actual_shares) else float(actual_shares):.0f}。"
            )
        if anomaly_type == "委托未成交":
            return (
                f"存在计划但无实际成交；计划={planned_action} "
                f"{0 if pd.isna(planned_shares) else float(planned_shares):.0f}股 "
                f"@ {0 if pd.isna(planned_price) else float(planned_price):.3f}。"
            )
        if anomaly_type == "部分成交":
            exe_ratio = row.get("execution_ratio", np.nan)
            exe_text = "" if pd.isna(exe_ratio) else f"{float(exe_ratio):.2%}"
            return (
                f"计划={planned_action} {0 if pd.isna(planned_shares) else float(planned_shares):.0f}股，"
                f"实际成交={actual_action} {0 if pd.isna(actual_shares) else float(actual_shares):.0f}股；"
                f"成交完成率={exe_text}。"
            )
        if anomaly_type in {"手工临时交易", "非系统交易"}:
            return (
                f"实际成交存在但系统计划缺失；实际={actual_action} "
                f"{0 if pd.isna(actual_shares) else float(actual_shares):.0f}股 "
                f"@ {0 if pd.isna(actual_price) else float(actual_price):.3f}；"
                f"来源={row.get('trade_source', '')}。"
            )
        if anomaly_type == "价格滑点超阈值":
            return (
                f"计划价={0 if pd.isna(planned_price) else float(planned_price):.3f}，"
                f"实际均价={0 if pd.isna(actual_price) else float(actual_price):.3f}，"
                f"滑点={slip_text}。"
            )
        if anomaly_type == "仓位执行偏差":
            return (
                f"计划数量={0 if pd.isna(planned_shares) else float(planned_shares):.0f}股，"
                f"实际数量={0 if pd.isna(actual_shares) else float(actual_shares):.0f}股，"
                f"仓位偏差={dev_text}。"
            )
        return ""
