# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from core.a_share_execution_audit import AShareExecutionAuditor
from core.market_risk_guard import MarketRiskDecision, MarketRiskGuard


class OpenExecutionManager:
    """
    Stage 06: 开盘动态执行层
    接收执行计划与当日快照，先做 Route C 大盘风控外层开关，再做 A 股执行真实性审计。
    """

    def __init__(self, base_dir: str = r"C:\quant_system") -> None:
        self.base_dir = Path(base_dir)
        self.reports_dir = self.base_dir / "reports"
        self.reports_dir.mkdir(parents=True, exist_ok=True)
        self.usable_capital = 1_000_000.0
        self.market_risk_guard = MarketRiskGuard(base_dir=self.base_dir)
        self.execution_auditor = AShareExecutionAuditor()

    def run(self, trading_date: str) -> Dict[str, Any]:
        print("============================================================")
        print("开盘动态执行开始")
        print(f"目标交易日  : {trading_date}")
        print(f"执行计划文件: {self.reports_dir / 'daily_execution_plan.csv'}")
        print(f"快照文件    : {self.reports_dir / 'market_signal_snapshot.csv'}")
        print(f"输出目录    : {self.reports_dir}")
        print("入口类型    : function")
        print("调用入口    : core.open_execution_manager.main")
        print("============================================================")

        plan_path = self.reports_dir / "daily_execution_plan.csv"
        snapshot_path = self.reports_dir / "market_signal_snapshot.csv"
        if not plan_path.exists():
            raise FileNotFoundError("缺少执行计划文件 daily_execution_plan.csv")
        if not snapshot_path.exists():
            raise FileNotFoundError("缺少快照文件 market_signal_snapshot.csv")

        df_plan = self._read_csv_with_fallback(plan_path)
        df_snapshot = self._read_csv_with_fallback(snapshot_path)
        if df_plan.empty:
            raise ValueError("执行计划文件为空，无票可执行")
        df_plan = self._attach_route_a_fields(df_plan)

        market_risk_decision = self.market_risk_guard.evaluate_route_c(trading_date=trading_date, snapshot_df=df_snapshot)
        self._write_market_risk_artifacts(trading_date=trading_date, decision=market_risk_decision)

        df_decision, preliminary_orders_df, actual_used_capital = self._build_preliminary_execution(
            trading_date=trading_date,
            df_plan=df_plan,
            df_snapshot=df_snapshot,
            market_risk_decision=market_risk_decision,
        )

        positions_df = self._load_positions_for_audit()
        audit_df, audit_summary = self.execution_auditor.audit_orders(
            trading_date=trading_date,
            orders_df=preliminary_orders_df,
            snapshot_df=df_snapshot,
            positions_df=positions_df,
        )
        final_orders_df, blocked_by_audit = self._apply_audit_results(df_decision=df_decision, audit_df=audit_df)
        self._write_audit_artifacts(trading_date=trading_date, audit_df=audit_df, audit_summary=audit_summary)

        decision_path = self.reports_dir / "daily_open_execution_decision.csv"
        order_path = self.reports_dir / "daily_open_execution_orders.csv"
        summary_path = self.reports_dir / "daily_open_execution_summary.txt"

        df_decision.to_csv(decision_path, index=False, encoding="utf-8-sig")
        final_orders_df.to_csv(order_path, index=False, encoding="utf-8-sig")
        self._write_summary(
            path=summary_path,
            trading_date=trading_date,
            df_decision=df_decision,
            df_orders=final_orders_df,
            used_capital=actual_used_capital,
            market_risk_decision=market_risk_decision,
            audit_summary=audit_summary,
            blocked_by_audit=blocked_by_audit,
        )

        print("============================================================")
        print("开盘动态执行完成")
        print(f"目标交易日: {trading_date}")
        print(f"Route C 状态: {market_risk_decision.route_status}")
        print(f"决策标的数: {len(df_decision)}")
        print(f"审计后委托数: {len(final_orders_df)}")
        print(f"审计阻断数: {blocked_by_audit}")
        print(f"决策文件  : {decision_path}")
        print(f"委托文件  : {order_path}")
        print(f"摘要文件  : {summary_path}")
        print("============================================================")

        return {
            "stage_status": "SUCCESS_EXECUTED",
            "success": True,
            "trading_date": trading_date,
            "decision_count": int(len(df_decision)),
            "order_count": int(len(final_orders_df)),
            "blocked_by_audit": int(blocked_by_audit),
            "used_capital": float(actual_used_capital),
            "market_risk_status": market_risk_decision.route_status,
        }

    def _build_preliminary_execution(
        self,
        trading_date: str,
        df_plan: pd.DataFrame,
        df_snapshot: pd.DataFrame,
        market_risk_decision: MarketRiskDecision,
    ) -> tuple[pd.DataFrame, pd.DataFrame, float]:
        df_merged = pd.merge(df_plan, df_snapshot, on="code", how="left")

        shares_col = self._pick_col(
            df_merged,
            ["review_planned_shares", "suggested_shares", "planned_shares", "order_shares", "shares", "target_shares", "order_qty"],
        )
        if not shares_col:
            raise ValueError(
                "缺少必要字段，候选别名: ['review_planned_shares', 'suggested_shares', 'planned_shares', 'order_shares', 'shares']"
            )

        entry_price_col = self._pick_col(df_merged, ["entry_price"])
        open_price_col = self._pick_col(df_merged, ["open_price", "close_price", "latest_price", "open", "price"])
        low_price_col = self._pick_col(df_merged, ["low_price", "low"])
        high_price_col = self._pick_col(df_merged, ["high_price", "high"])
        low_limit_col = self._pick_col(df_merged, ["low_limit", "down_limit", "limit_down"])
        skip_lt_col = self._pick_col(df_merged, ["skip_if_open_lt"])
        chase_limit_col = self._pick_col(df_merged, ["chase_limit_price"])

        if "execution_rank" in df_merged.columns:
            df_merged = df_merged.sort_values("execution_rank").reset_index(drop=True)

        decisions: list[dict[str, Any]] = []
        orders: list[dict[str, Any]] = []
        actual_used_capital = 0.0

        for _, row in df_merged.iterrows():
            code = row["code"]
            plan_shares = int(row.get(shares_col, 0))
            entry_price = float(row.get(entry_price_col, 0.0)) if entry_price_col else 0.0
            open_price = float(row.get(open_price_col, entry_price)) if open_price_col else entry_price
            low_price = float(row.get(low_price_col, open_price)) if low_price_col else open_price
            high_price = float(row.get(high_price_col, open_price)) if high_price_col else open_price
            low_limit = float(row.get(low_limit_col, 0.0)) if low_limit_col else 0.0
            skip_lt = float(row.get(skip_lt_col, 0.0)) if skip_lt_col else 0.0
            chase_limit = float(row.get(chase_limit_col, 99999.0)) if chase_limit_col else 99999.0
            route_entry_mode = str(row.get("entry_mode", "")).strip().upper()
            route_entry_valid = self._safe_bool(row.get("entry_valid", False))
            target_buy_price_base = self._safe_float(row.get("target_buy_price_base"), default=entry_price)

            final_action = "正常买入"
            final_shares = plan_shares
            drop_reason = ""
            keep_flag = 1
            planned_buy_price = round(open_price, 4) if open_price > 0 else 0.0
            executed_buy_price = round(open_price, 4) if open_price > 0 else 0.0
            buy_trigger_hit = False
            buy_block_reason = ""

            if route_entry_mode == "ROUTE_A_LEFT_CATCH":
                planned_buy_price = round(min(open_price, target_buy_price_base), 4) if open_price > 0 and target_buy_price_base > 0 else 0.0
                final_action, final_shares, drop_reason, keep_flag, executed_buy_price, buy_trigger_hit, buy_block_reason = self._evaluate_route_a_buy(
                    market_risk_decision=market_risk_decision,
                    route_entry_valid=route_entry_valid,
                    open_price=open_price,
                    low_price=low_price,
                    high_price=high_price,
                    low_limit=low_limit,
                    planned_buy_price=planned_buy_price,
                    plan_shares=plan_shares,
                )
            else:
                if not market_risk_decision.route_enabled:
                    final_action = "大盘风控阻断"
                    final_shares = 0
                    drop_reason = market_risk_decision.reason
                    keep_flag = 0
                    buy_block_reason = "MARKET_RISK_BLOCKED"
                elif pd.isna(open_price) or open_price <= 0:
                    final_action = "无行情放弃"
                    final_shares = 0
                    drop_reason = "无法获取有效开盘价"
                    keep_flag = 0
                    buy_block_reason = "INVALID_OPEN_PRICE"
                elif open_price < skip_lt:
                    final_action = "低开放弃"
                    final_shares = 0
                    drop_reason = f"开盘价 {open_price} 低于底线 {skip_lt}"
                    keep_flag = 0
                    buy_block_reason = "OPEN_BELOW_SKIP_LINE"
                elif open_price > chase_limit:
                    final_action = "高开放弃"
                    final_shares = 0
                    drop_reason = f"开盘价 {open_price} 高于追高上限 {chase_limit}"
                    keep_flag = 0
                    buy_block_reason = "OPEN_ABOVE_CHASE_LIMIT"
                elif open_price < entry_price:
                    final_action = "低吸买入"
                    buy_trigger_hit = True
                elif open_price > entry_price:
                    final_action = "谨慎追价"
                    buy_trigger_hit = True

            cost = final_shares * executed_buy_price
            if cost > 0 and (actual_used_capital + cost) > self.usable_capital:
                final_action = "资金不足放弃"
                final_shares = 0
                drop_reason = "剩余资金不足以覆盖该笔订单"
                keep_flag = 0
                buy_block_reason = "INSUFFICIENT_CAPITAL"
                executed_buy_price = 0.0
                buy_trigger_hit = False
                cost = 0.0

            actual_used_capital += cost
            decision = {
                "execution_rank": row.get("execution_rank", 99),
                "code": code,
                "name": row.get("name", ""),
                "open_price": round(open_price, 2),
                "low_price": round(low_price, 2),
                "high_price": round(high_price, 2),
                "low_limit": round(low_limit, 2) if low_limit else 0.0,
                "signal_date": row.get("signal_date", ""),
                "entry_mode": row.get("entry_mode", ""),
                "target_buy_price_base": round(target_buy_price_base, 4) if target_buy_price_base else 0.0,
                "entry_valid": bool(route_entry_valid) if route_entry_mode == "ROUTE_A_LEFT_CATCH" else row.get("entry_valid", ""),
                "stop_mode": row.get("stop_mode", ""),
                "target_profit_pct": row.get("target_profit_pct", ""),
                "shadow_threshold": row.get("shadow_threshold", ""),
                "entry_price": round(entry_price, 2),
                "planned_buy_price": round(planned_buy_price, 4),
                "executed_buy_price": round(executed_buy_price, 4) if executed_buy_price else 0.0,
                "buy_trigger_hit": bool(buy_trigger_hit),
                "buy_block_reason": buy_block_reason,
                "final_action": final_action,
                "order_ratio": 1.0 if keep_flag else 0.0,
                "final_order_shares": final_shares,
                "filled_shares": final_shares,
                "avg_fill_price": round(executed_buy_price, 4) if executed_buy_price else 0.0,
                "final_order_price": round(executed_buy_price, 2) if executed_buy_price else 0.0,
                "final_order_capital": round(cost, 2),
                "final_keep_flag": keep_flag,
                "final_drop_reason": drop_reason,
                "stop_loss": row.get("stop_loss", ""),
                "target_price": row.get("target_price", ""),
                "route_a_signal": self._safe_bool(row.get("route_a_signal", False)),
                "market_risk_status": market_risk_decision.route_status,
            }
            decisions.append(decision)

            if keep_flag:
                orders.append(
                    {
                        "trading_date": trading_date,
                        "code": code,
                        "name": row.get("name", ""),
                        "action": "BUY",
                        "order_shares": final_shares,
                        "order_price": round(executed_buy_price, 4),
                        "order_type": "LIMIT",
                        "status": "PENDING",
                        "planned_buy_price": round(planned_buy_price, 4),
                        "executed_buy_price": round(executed_buy_price, 4),
                        "buy_trigger_hit": bool(buy_trigger_hit),
                        "buy_block_reason": buy_block_reason,
                        "signal_date": row.get("signal_date", ""),
                        "entry_mode": row.get("entry_mode", ""),
                        "target_buy_price_base": round(target_buy_price_base, 4) if target_buy_price_base else 0.0,
                        "entry_valid": bool(route_entry_valid) if route_entry_mode == "ROUTE_A_LEFT_CATCH" else row.get("entry_valid", ""),
                        "stop_mode": row.get("stop_mode", ""),
                        "target_profit_pct": row.get("target_profit_pct", ""),
                        "shadow_threshold": row.get("shadow_threshold", ""),
                        "stop_loss": row.get("stop_loss", ""),
                        "target_price": row.get("target_price", ""),
                        "route_a_signal": self._safe_bool(row.get("route_a_signal", False)),
                    }
                )

        return pd.DataFrame(decisions), pd.DataFrame(orders), actual_used_capital

    def _apply_audit_results(self, df_decision: pd.DataFrame, audit_df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
        if audit_df.empty:
            empty_orders = pd.DataFrame(
                columns=[
                    "trading_date",
                    "code",
                    "name",
                    "action",
                    "order_shares",
                    "order_price",
                    "order_type",
                    "status",
                    "audit_status",
                    "audit_violation_codes",
                    "estimated_total_fee",
                    "estimated_slippage_amount",
                    "estimated_fill_price",
                ]
            )
            return empty_orders, 0

        blocked = audit_df[audit_df["audit_status"] == "BLOCKED"].copy()
        if not blocked.empty and not df_decision.empty:
            block_reason_map = (
                blocked.assign(
                    block_reason=blocked["violation_codes"].astype(str) + ":" + blocked["violation_message"].astype(str)
                )
                .groupby("code", as_index=False)["block_reason"]
                .first()
            )
            df_decision.merge(block_reason_map, on="code", how="left", copy=False)
            for _, blocked_row in block_reason_map.iterrows():
                mask = df_decision["code"].astype(str) == str(blocked_row["code"])
                df_decision.loc[mask, "final_action"] = "审计阻断"
                df_decision.loc[mask, "final_order_shares"] = 0
                df_decision.loc[mask, "final_order_capital"] = 0.0
                df_decision.loc[mask, "final_keep_flag"] = 0
                df_decision.loc[mask, "final_drop_reason"] = blocked_row["block_reason"]
                df_decision.loc[mask, "buy_block_reason"] = "STAGE1_AUDIT_BLOCKED"

        passed = audit_df[audit_df["audit_status"] == "PASS"].copy()
        if passed.empty:
            return (
                pd.DataFrame(
                    columns=[
                        "trading_date",
                        "code",
                        "name",
                        "action",
                        "order_shares",
                        "order_price",
                        "order_type",
                        "status",
                        "audit_status",
                        "audit_violation_codes",
                        "estimated_total_fee",
                        "estimated_slippage_amount",
                        "estimated_fill_price",
                    ]
                ),
                int(len(blocked)),
            )

        orders = pd.DataFrame(
            {
                "trading_date": passed["trading_date"],
                "code": passed["code"],
                "name": passed["name"],
                "action": passed["action"],
                "order_shares": passed["order_shares"],
                "order_price": passed["order_price"],
                "order_type": passed["order_type"],
                "status": "PENDING",
                "audit_status": passed["audit_status"],
                "audit_violation_codes": passed["violation_codes"],
                "estimated_total_fee": passed["total_fee"],
                "estimated_slippage_amount": passed["slippage_amount"],
                "estimated_fill_price": passed["audited_fill_price"],
            }
        )
        decision_extra_cols = [
            "code",
            "planned_buy_price",
            "executed_buy_price",
            "buy_trigger_hit",
            "buy_block_reason",
            "signal_date",
            "entry_mode",
            "target_buy_price_base",
            "entry_valid",
            "stop_mode",
            "target_profit_pct",
            "shadow_threshold",
            "stop_loss",
            "target_price",
            "route_a_signal",
        ]
        available_extra_cols = [col for col in decision_extra_cols if col in df_decision.columns]
        if available_extra_cols:
            extra_df = df_decision[available_extra_cols].drop_duplicates(subset=["code"], keep="first")
            orders = orders.merge(extra_df, on="code", how="left")
        return orders, int(len(blocked))

    def _read_csv_with_fallback(self, path: Path) -> pd.DataFrame:
        try:
            return pd.read_csv(path, encoding="utf-8-sig")
        except UnicodeDecodeError:
            return pd.read_csv(path, encoding="gbk")

    def _attach_route_a_fields(self, df_plan: pd.DataFrame) -> pd.DataFrame:
        route_fields = [
            "signal_date",
            "entry_mode",
            "pre_low",
            "pre_ma20",
            "target_buy_price_base",
            "entry_valid",
            "stop_mode",
            "stop_loss",
            "target_profit_pct",
            "shadow_threshold",
            "plan_reason",
            "route_a_signal",
            "target_price",
        ]
        if any(field in df_plan.columns for field in route_fields):
            return df_plan

        trade_plan_path = self.reports_dir / "daily_trade_plan_all.csv"
        if not trade_plan_path.exists():
            return df_plan
        try:
            trade_plan_df = self._read_csv_with_fallback(trade_plan_path)
        except Exception:
            return df_plan
        if trade_plan_df.empty or "code" not in trade_plan_df.columns:
            return df_plan

        available_fields = [field for field in ["code", *route_fields] if field in trade_plan_df.columns]
        if len(available_fields) <= 1:
            return df_plan
        trade_plan_df = trade_plan_df[available_fields].copy()
        return df_plan.merge(trade_plan_df, on="code", how="left", suffixes=("", "_route_plan"))

    def _pick_col(self, df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
        lower_map = {str(c).strip().lower(): c for c in df.columns}
        for candidate in candidates:
            if candidate.lower() in lower_map:
                return lower_map[candidate.lower()]
        return None

    def _evaluate_route_a_buy(
        self,
        market_risk_decision: MarketRiskDecision,
        route_entry_valid: bool,
        open_price: float,
        low_price: float,
        high_price: float,
        low_limit: float,
        planned_buy_price: float,
        plan_shares: int,
    ) -> tuple[str, int, str, int, float, bool, str]:
        if not market_risk_decision.route_enabled:
            return "大盘风控阻断", 0, market_risk_decision.reason, 0, 0.0, False, "MARKET_RISK_BLOCKED"
        if not route_entry_valid:
            return "计划无效", 0, "Route A 计划缺少有效入场字段", 0, 0.0, False, "ENTRY_INVALID"
        if pd.isna(open_price) or open_price <= 0 or pd.isna(low_price) or low_price <= 0:
            return "无行情放弃", 0, "缺少有效开盘/最低价，无法判断 Route A 承接", 0, 0.0, False, "INVALID_DAY_PRICE"
        if low_limit > 0 and open_price <= low_limit + 0.01 and high_price <= low_limit + 0.01:
            return "跌停锁死放弃", 0, "执行日跌停锁死，Route A 不买入", 0, 0.0, False, "LIMIT_DOWN_LOCKED"
        if low_price <= planned_buy_price:
            return "RouteA 承接买入", plan_shares, "", 1, planned_buy_price, True, ""
        return "RouteA 未触价", 0, "执行日最低价未触及承接价", 0, 0.0, False, "TARGET_PRICE_NOT_TOUCHED"

    def _safe_float(self, value: Any, default: float = 0.0) -> float:
        try:
            out = float(value)
            if pd.isna(out):
                return default
            return out
        except (TypeError, ValueError):
            return default

    def _safe_bool(self, value: Any) -> bool:
        if isinstance(value, bool):
            return value
        text = str(value).strip().lower()
        return text in {"1", "true", "yes", "y"}

    def _load_positions_for_audit(self) -> pd.DataFrame:
        for filename in ("daily_next_day_management.csv", "broker_positions.csv", "close_positions.csv"):
            path = self.reports_dir / filename
            if not path.exists():
                continue
            try:
                return self._read_csv_with_fallback(path)
            except Exception:
                continue
        return pd.DataFrame(columns=["code", "available_qty"])

    def _write_market_risk_artifacts(self, trading_date: str, decision: MarketRiskDecision) -> None:
        json_path = self.reports_dir / "daily_market_risk_route_c.json"
        txt_path = self.reports_dir / "daily_market_risk_route_c.txt"
        payload = decision.to_dict()
        with json_path.open("w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2)
        lines = [
            "============================================================",
            "Route C 大盘风控外层开关摘要",
            f"目标交易日: {trading_date}",
            f"路由名称: {decision.route_name}",
            f"是否放行: {decision.route_enabled}",
            f"状态码: {decision.route_status}",
            f"风险分: {decision.risk_score}",
            f"信号来源: {decision.source_path or '未提供'}",
            f"来源模式: {decision.source_mode}",
            f"说明: {decision.reason}",
            "============================================================",
        ]
        txt_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    def _write_audit_artifacts(self, trading_date: str, audit_df: pd.DataFrame, audit_summary: dict[str, Any]) -> None:
        csv_path = self.reports_dir / "daily_open_execution_audit.csv"
        json_path = self.reports_dir / "daily_open_execution_audit.json"
        txt_path = self.reports_dir / "daily_open_execution_audit_summary.txt"

        audit_df.to_csv(csv_path, index=False, encoding="utf-8-sig")
        with json_path.open("w", encoding="utf-8") as fh:
            json.dump(audit_summary, fh, ensure_ascii=False, indent=2, default=str)

        lines = [
            "============================================================",
            "A股执行真实性审计摘要",
            f"目标交易日: {trading_date}",
            f"总委托数: {audit_summary.get('total_orders', 0)}",
            f"通过数: {audit_summary.get('passed_orders', 0)}",
            f"阻断数: {audit_summary.get('blocked_orders', 0)}",
            f"预计总费用: {audit_summary.get('estimated_total_fee', 0.0):.2f}",
            f"预计总滑点: {audit_summary.get('estimated_total_slippage', 0.0):.2f}",
            f"阻断统计: {audit_summary.get('blocked_by_code', {})}",
            "============================================================",
        ]
        txt_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    def _write_summary(
        self,
        path: Path,
        trading_date: str,
        df_decision: pd.DataFrame,
        df_orders: pd.DataFrame,
        used_capital: float,
        market_risk_decision: MarketRiskDecision,
        audit_summary: dict[str, Any],
        blocked_by_audit: int,
    ) -> None:
        lines = [
            "============================================================",
            "开盘动态执行摘要",
            f"目标交易日: {trading_date}",
            f"Route C 状态: {market_risk_decision.route_status}",
            f"Route C 说明: {market_risk_decision.reason}",
            f"决策标的数: {len(df_decision)}",
            f"审计后委托数: {len(df_orders)}",
            f"审计阻断数: {blocked_by_audit}",
            f"实际占用资金: {used_capital:.2f}",
            f"预计总费用: {audit_summary.get('estimated_total_fee', 0.0):.2f}",
            f"预计总滑点: {audit_summary.get('estimated_total_slippage', 0.0):.2f}",
            "============================================================",
        ]
        path.write_text("\n".join(lines), encoding="utf-8")


def run(trading_date: str, base_dir: str = r"C:\quant_system") -> Dict[str, Any]:
    manager = OpenExecutionManager(base_dir=base_dir)
    return manager.run(trading_date)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--trading-date", required=True)
    parser.add_argument("--base-dir", default=r"C:\quant_system")
    args, _ = parser.parse_known_args()

    res = run(args.trading_date, args.base_dir)
    if res.get("stage_status") == "FAILED":
        print(f"ERROR: {res.get('error')}")
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
