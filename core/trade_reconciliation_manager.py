from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd


@dataclass
class ReconciliationSummary:
    trade_date: str
    planned_trade_count: int
    actual_trade_count: int
    matched_count: int
    partial_fill_count: int
    missing_trade_count: int
    extra_trade_count: int
    manual_trade_count: int
    side_mismatch_count: int
    price_anomaly_count: int
    position_anomaly_count: int
    planned_trade_amount: float
    actual_trade_amount: float
    adverse_slippage_bps_avg: float


class TradeReconciliationManager:
    """
    第13段：真实交易流水对账层

    关键修正：
    1) 计划文件按字段级合并，而不是整行覆盖
    2) 后置文件没有 shares 时，不会把前置文件的 shares 覆盖成 0
    3) 扩展数量/价格/动作字段识别口径
    """

    def __init__(
        self,
        project_root: str | Path,
        trade_date: str,
        planned_paths: Optional[List[str | Path]] = None,
        actual_fills_path: Optional[str | Path] = None,
        previous_position_path: Optional[str | Path] = None,
        actual_position_path: Optional[str | Path] = None,
        reports_dir: Optional[str | Path] = None,
        qty_tolerance: int = 100,
        price_slippage_bps_threshold: float = 50.0,
    ) -> None:
        self.project_root = Path(project_root)
        self.reports_dir = Path(reports_dir) if reports_dir else self.project_root / "reports"
        self.trade_date = pd.Timestamp(trade_date).normalize()

        if planned_paths is None:
            planned_paths = [
                self.reports_dir / "daily_execution_plan.csv",
                self.reports_dir / "daily_open_execution_decision.csv",
                self.reports_dir / "daily_intraday_recheck_decision.csv",
            ]
        self.planned_paths = [Path(p) for p in planned_paths]

        self.actual_fills_path = (
            Path(actual_fills_path) if actual_fills_path else self.reports_dir / "real_trade_fills.csv"
        )
        self.previous_position_path = (
            Path(previous_position_path)
            if previous_position_path
            else self.reports_dir / "daily_close_review.csv"
        )
        self.actual_position_path = Path(actual_position_path) if actual_position_path else None

        self.qty_tolerance = int(qty_tolerance)
        self.price_slippage_bps_threshold = float(price_slippage_bps_threshold)

        self.output_detail_path = self.reports_dir / "daily_trade_reconciliation_detail.csv"
        self.output_anomaly_path = self.reports_dir / "daily_trade_reconciliation_anomalies.csv"
        self.output_position_path = self.reports_dir / "daily_trade_reconciliation_position_check.csv"
        self.output_summary_path = self.reports_dir / "daily_trade_reconciliation_summary.txt"

    @staticmethod
    def _read_csv_auto(path: Path) -> pd.DataFrame:
        encodings = ["utf-8-sig", "utf-8", "gbk", "gb2312", "utf-16", "latin1"]
        last_error = None
        for encoding in encodings:
            try:
                return pd.read_csv(path, encoding=encoding)
            except Exception as exc:
                last_error = exc
        raise ValueError(f"CSV 读取失败: {path} | {last_error}")

    @staticmethod
    def _first_existing_column(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
        for col in candidates:
            if col in df.columns:
                return col
        return None

    @staticmethod
    def _infer_market(code_digits: str) -> str:
        if not code_digits or len(code_digits) != 6:
            return ""
        return "sh" if code_digits.startswith(("5", "6", "9")) else "sz"

    def _normalize_code(self, code: str) -> str:
        code = str(code).strip().lower()
        code = code.replace("_", ".").replace("-", ".").replace(" ", "")
        code = code.replace("shse.", "sh.").replace("szse.", "sz.")
        code = code.replace(".xshe", ".sz").replace(".xshg", ".sh")
        code = code.replace("xshe.", "sz.").replace("xshg.", "sh.")

        if code.startswith(("sh", "sz")) and "." not in code:
            code = f"{code[:2]}.{code[2:]}"
        if code.endswith((".sh", ".sz")) and len(code) >= 8:
            digits, market = code[:6], code[-2:]
            code = f"{market}.{digits}"

        digits = "".join(ch for ch in code if ch.isdigit())
        if len(digits) == 6:
            if code.startswith(("sh.", "sz.")):
                return f"{code[:2]}.{digits}"
            market = self._infer_market(digits)
            return f"{market}.{digits}"
        return code

    def _classify_side(self, action_text: str) -> str:
        text = str(action_text).strip().lower()
        if text in {"nan", "none", ""}:
            return "hold"

        buy_keywords = ["买", "买入", "建仓", "加仓", "补仓", "开仓", "buy", "long", "b"]
        sell_keywords = ["卖", "卖出", "减仓", "清仓", "止盈", "止损", "平仓", "sell", "short", "s"]
        hold_keywords = ["持有", "观察", "跟踪", "续持", "hold", "watch", "不动"]

        if any(k == text or k in text for k in sell_keywords):
            return "sell"
        if any(k == text or k in text for k in buy_keywords):
            return "buy"
        if any(k in text for k in hold_keywords):
            return "hold"
        return "hold"

    @staticmethod
    def _last_valid(series: pd.Series):
        s = series.dropna()
        if s.empty:
            return np.nan
        return s.iloc[-1]

    @staticmethod
    def _last_valid_text(series: pd.Series, invalid_values: Optional[List[str]] = None):
        if invalid_values is None:
            invalid_values = ["", "nan", "none", "未计划"]
        for value in reversed(series.tolist()):
            if pd.isna(value):
                continue
            txt = str(value).strip()
            if txt.lower() in invalid_values:
                continue
            return txt
        return ""

    @staticmethod
    def _to_num_series(series: pd.Series) -> pd.Series:
        return pd.to_numeric(series, errors="coerce")

    def _standardize_plan_df(self, raw_df: pd.DataFrame, source_name: str, source_priority: int) -> pd.DataFrame:
        df = raw_df.copy()
        df.columns = [str(c).strip() for c in df.columns]

        code_col = self._first_existing_column(
            df, ["code", "ts_code", "symbol", "stock_code", "证券代码", "股票代码", "代码"]
        )
        action_col = self._first_existing_column(
            df,
            [
                "action",
                "final_action",
                "trade_action",
                "decision",
                "open_decision",
                "intraday_decision",
                "management_action",
                "执行动作",
                "交易动作",
                "操作建议",
                "操作",
                "signal_action",
            ],
        )
        shares_col = self._first_existing_column(
            df,
            [
                "planned_shares",
                "execution_shares",
                "order_shares",
                "final_shares",
                "allocated_shares",
                "suggested_shares",
                "shares_to_trade",
                "trade_shares",
                "filled_shares",
                "actual_shares",
                "target_shares",
                "position_shares",
                "shares",
                "委托数量",
                "计划数量",
                "建议股数",
                "下单股数",
                "交易股数",
                "成交股数",
                "成交数量",
                "目标股数",
                "数量",
                "股数",
            ],
        )
        price_col = self._first_existing_column(
            df,
            [
                "planned_price",
                "order_price",
                "execution_price",
                "entry_price",
                "limit_price",
                "trigger_price",
                "price",
                "avg_fill_price",
                "filled_price",
                "委托价格",
                "计划价格",
                "下单价格",
                "买入价格",
                "卖出价格",
                "成交均价",
                "成交价格",
                "价格",
            ],
        )
        rank_col = self._first_existing_column(
            df, ["execution_rank", "rank", "priority_rank", "priority", "排序", "执行优先级"]
        )
        position_pct_col = self._first_existing_column(
            df,
            [
                "target_position_pct",
                "planned_position_pct",
                "suggested_position_pct",
                "allocated_position_pct",
                "position_pct",
                "仓位占比",
                "目标仓位",
                "建议仓位",
                "持仓占比",
            ],
        )

        if code_col is None:
            raise ValueError(f"计划文件缺少代码字段: {source_name}")

        result = pd.DataFrame()
        result["code"] = df[code_col].astype(str).map(self._normalize_code)

        if action_col is not None:
            result["planned_action"] = df[action_col].astype(str)
        else:
            result["planned_action"] = np.nan

        result["planned_side"] = result["planned_action"].map(self._classify_side)

        # 这里不能默认填 0，否则后置文件会把前置有效数量冲掉
        result["planned_shares"] = (
            pd.to_numeric(df[shares_col], errors="coerce") if shares_col else np.nan
        )
        result["planned_price"] = (
            pd.to_numeric(df[price_col], errors="coerce") if price_col else np.nan
        )
        result["execution_rank"] = (
            pd.to_numeric(df[rank_col], errors="coerce") if rank_col else np.nan
        )
        result["planned_position_pct"] = (
            pd.to_numeric(df[position_pct_col], errors="coerce") if position_pct_col else np.nan
        )

        result["plan_source"] = source_name
        result["plan_priority"] = int(source_priority)

        result = result.dropna(subset=["code"]).drop_duplicates(subset=["code"], keep="last")
        return result.reset_index(drop=True)

    def _load_final_plan(self) -> pd.DataFrame:
        plan_frames = []
        for idx, path in enumerate(self.planned_paths, start=1):
            if not path.exists():
                continue
            raw_df = self._read_csv_auto(path)
            std_df = self._standardize_plan_df(raw_df, path.name, idx)
            plan_frames.append(std_df)

        if not plan_frames:
            raise FileNotFoundError(
                "未找到可用计划文件，请至少提供一个：daily_execution_plan.csv / "
                "daily_open_execution_decision.csv / daily_intraday_recheck_decision.csv"
            )

        combined = pd.concat(plan_frames, ignore_index=True)
        combined = combined.sort_values(["code", "plan_priority"]).reset_index(drop=True)

        rows = []
        for code, g in combined.groupby("code", as_index=False):
            g = g.sort_values("plan_priority").reset_index(drop=True)

            planned_action = self._last_valid_text(g["planned_action"])
            planned_side = self._classify_side(planned_action)

            planned_shares = self._last_valid(self._to_num_series(g["planned_shares"]))
            planned_price = self._last_valid(self._to_num_series(g["planned_price"]))
            execution_rank = self._last_valid(self._to_num_series(g["execution_rank"]))
            planned_position_pct = self._last_valid(self._to_num_series(g["planned_position_pct"]))

            # 对有交易动作但 shares 仍缺失的情况，尝试从更早文件中找最后一个 >0 的数量
            positive_shares = self._to_num_series(g["planned_shares"])
            positive_shares = positive_shares[positive_shares > 0]
            if pd.isna(planned_shares) and not positive_shares.empty:
                planned_shares = positive_shares.iloc[-1]

            # 对有交易动作但 price 仍缺失的情况，尝试从更早文件中找最后一个 >0 的价格
            positive_prices = self._to_num_series(g["planned_price"])
            positive_prices = positive_prices[positive_prices > 0]
            if pd.isna(planned_price) and not positive_prices.empty:
                planned_price = positive_prices.iloc[-1]

            source_trace = " -> ".join(g["plan_source"].astype(str).tolist())

            rows.append(
                {
                    "code": code,
                    "planned_action": planned_action if planned_action else "未计划",
                    "planned_side": planned_side,
                    "planned_shares": 0 if pd.isna(planned_shares) else int(round(float(planned_shares))),
                    "planned_price": np.nan if pd.isna(planned_price) else float(planned_price),
                    "execution_rank": np.nan if pd.isna(execution_rank) else float(execution_rank),
                    "planned_position_pct": np.nan if pd.isna(planned_position_pct) else float(planned_position_pct),
                    "plan_source": g["plan_source"].iloc[-1],
                    "plan_source_trace": source_trace,
                }
            )

        final_plan = pd.DataFrame(rows)
        final_plan["planned_signed_shares"] = np.select(
            [
                final_plan["planned_side"].eq("buy"),
                final_plan["planned_side"].eq("sell"),
            ],
            [
                final_plan["planned_shares"],
                -final_plan["planned_shares"],
            ],
            default=0,
        )
        final_plan = final_plan.sort_values(["execution_rank", "code"], na_position="last").reset_index(drop=True)
        return final_plan

    def _standardize_fills_df(self, raw_df: pd.DataFrame) -> pd.DataFrame:
        df = raw_df.copy()
        df.columns = [str(c).strip() for c in df.columns]

        code_col = self._first_existing_column(
            df, ["code", "ts_code", "symbol", "stock_code", "证券代码", "股票代码", "代码"]
        )
        side_col = self._first_existing_column(
            df, ["side", "bs_flag", "买卖标志", "买卖方向", "方向", "操作"]
        )
        qty_col = self._first_existing_column(
            df, ["filled_shares", "成交数量", "成交股数", "数量", "成交数", "股数"]
        )
        price_col = self._first_existing_column(
            df, ["filled_price", "成交均价", "成交价格", "均价", "价格", "price"]
        )
        amount_col = self._first_existing_column(
            df, ["filled_amount", "成交金额", "金额", "发生金额", "amount"]
        )
        date_col = self._first_existing_column(
            df, ["trade_date", "date", "成交日期", "日期"]
        )
        time_col = self._first_existing_column(
            df, ["trade_time", "time", "成交时间", "时间"]
        )
        fee_col = self._first_existing_column(
            df, ["commission", "fee", "手续费", "佣金", "交易费用"]
        )
        order_id_col = self._first_existing_column(
            df, ["order_id", "委托编号", "委托序号", "订单编号"]
        )
        deal_id_col = self._first_existing_column(
            df, ["deal_id", "contract_id", "成交编号", "合同编号", "成交序号"]
        )

        if code_col is None or side_col is None or qty_col is None or price_col is None:
            raise ValueError("真实成交文件缺少必要字段：代码 / 买卖方向 / 成交数量 / 成交价格")

        result = pd.DataFrame()
        result["code"] = df[code_col].astype(str).map(self._normalize_code)
        result["actual_side"] = df[side_col].astype(str).map(self._classify_side)
        result["filled_shares"] = pd.to_numeric(df[qty_col], errors="coerce").fillna(0).astype(int)
        result["filled_price"] = pd.to_numeric(df[price_col], errors="coerce")

        if amount_col:
            result["filled_amount"] = pd.to_numeric(df[amount_col], errors="coerce")
        else:
            result["filled_amount"] = result["filled_shares"] * result["filled_price"]

        result["trade_date"] = (
            pd.to_datetime(df[date_col], errors="coerce").dt.normalize()
            if date_col else self.trade_date
        )
        result["trade_time"] = df[time_col].astype(str) if time_col else ""
        result["commission"] = pd.to_numeric(df[fee_col], errors="coerce") if fee_col else 0.0
        result["order_id"] = df[order_id_col].astype(str) if order_id_col else ""
        result["deal_id"] = df[deal_id_col].astype(str) if deal_id_col else ""

        result = result[result["trade_date"].fillna(self.trade_date) == self.trade_date].copy()
        result = result[result["filled_shares"] > 0].copy()

        result["signed_shares"] = np.select(
            [
                result["actual_side"].eq("buy"),
                result["actual_side"].eq("sell"),
            ],
            [
                result["filled_shares"],
                -result["filled_shares"],
            ],
            default=0,
        )
        return result.reset_index(drop=True)

    def _aggregate_actual_fills(self, fills_df: pd.DataFrame) -> pd.DataFrame:
        if fills_df.empty:
            return pd.DataFrame(
                columns=[
                    "code",
                    "actual_buy_qty",
                    "actual_buy_amount",
                    "actual_buy_vwap",
                    "actual_sell_qty",
                    "actual_sell_amount",
                    "actual_sell_vwap",
                    "actual_net_shares",
                    "actual_total_fee",
                    "actual_trade_count",
                ]
            )

        def _agg_one(group: pd.DataFrame) -> pd.Series:
            buy_df = group[group["actual_side"] == "buy"]
            sell_df = group[group["actual_side"] == "sell"]

            buy_qty = int(buy_df["filled_shares"].sum()) if not buy_df.empty else 0
            sell_qty = int(sell_df["filled_shares"].sum()) if not sell_df.empty else 0
            buy_amount = float((buy_df["filled_shares"] * buy_df["filled_price"]).sum()) if not buy_df.empty else 0.0
            sell_amount = float((sell_df["filled_shares"] * sell_df["filled_price"]).sum()) if not sell_df.empty else 0.0

            buy_vwap = buy_amount / buy_qty if buy_qty > 0 else np.nan
            sell_vwap = sell_amount / sell_qty if sell_qty > 0 else np.nan

            return pd.Series(
                {
                    "actual_buy_qty": buy_qty,
                    "actual_buy_amount": buy_amount,
                    "actual_buy_vwap": buy_vwap,
                    "actual_sell_qty": sell_qty,
                    "actual_sell_amount": sell_amount,
                    "actual_sell_vwap": sell_vwap,
                    "actual_net_shares": int(group["signed_shares"].sum()),
                    "actual_total_fee": float(group["commission"].fillna(0).sum()),
                    "actual_trade_count": int(len(group)),
                }
            )

        aggregated = fills_df.groupby("code", as_index=False).apply(_agg_one)
        aggregated = aggregated.reset_index(drop=True)
        return aggregated

    def _standardize_position_df(self, raw_df: pd.DataFrame, actual: bool = False) -> pd.DataFrame:
        df = raw_df.copy()
        df.columns = [str(c).strip() for c in df.columns]

        code_col = self._first_existing_column(
            df, ["code", "ts_code", "symbol", "stock_code", "证券代码", "股票代码", "代码"]
        )
        shares_col = self._first_existing_column(
            df,
            [
                "filled_shares",
                "hold_shares",
                "position_shares",
                "available_shares",
                "current_shares",
                "shares",
                "股份",
                "持仓股数",
                "持仓数量",
                "证券数量",
                "股数",
                "数量",
            ],
        )
        close_col = self._first_existing_column(
            df, ["close_price", "close", "last_price", "latest_price", "收盘价", "最新价"]
        )
        market_value_col = self._first_existing_column(
            df, ["market_value", "position_value", "市值", "持仓市值"]
        )
        position_pct_col = self._first_existing_column(
            df, ["position_pct", "仓位占比", "持仓占比", "weight"]
        )

        if code_col is None or shares_col is None:
            raise ValueError("持仓文件至少需要：代码 + 持仓股数")

        result = pd.DataFrame()
        result["code"] = df[code_col].astype(str).map(self._normalize_code)
        shares_series = pd.to_numeric(df[shares_col], errors="coerce").fillna(0).astype(int)

        if actual:
            result["actual_end_shares_from_file"] = shares_series
            result["actual_end_market_value"] = (
                pd.to_numeric(df[market_value_col], errors="coerce") if market_value_col else np.nan
            )
            result["actual_position_pct"] = (
                pd.to_numeric(df[position_pct_col], errors="coerce") if position_pct_col else np.nan
            )
            result["actual_reference_price"] = (
                pd.to_numeric(df[close_col], errors="coerce") if close_col else np.nan
            )
        else:
            result["prev_shares"] = shares_series
            result["prev_close_price"] = (
                pd.to_numeric(df[close_col], errors="coerce") if close_col else np.nan
            )

        result = result.drop_duplicates(subset=["code"], keep="last").reset_index(drop=True)
        return result

    @staticmethod
    def _anomaly_label(row: pd.Series) -> str:
        labels = []
        if row.get("missing_trade_flag", 0) == 1:
            labels.append("漏成交")
        if row.get("manual_trade_flag", 0) == 1:
            labels.append("手工成交")
        if row.get("extra_trade_flag", 0) == 1:
            labels.append("额外成交")
        if row.get("side_mismatch_flag", 0) == 1:
            labels.append("方向异常")
        if row.get("price_deviation_flag", 0) == 1:
            labels.append("价格偏差异常")
        if row.get("position_deviation_flag", 0) == 1:
            labels.append("仓位偏差")
        return "|".join(labels)

    def _build_reconciliation(self) -> Dict[str, pd.DataFrame | ReconciliationSummary]:
        final_plan = self._load_final_plan()

        if not self.actual_fills_path.exists():
            raise FileNotFoundError(f"未找到真实成交文件: {self.actual_fills_path}")
        fills_raw = self._read_csv_auto(self.actual_fills_path)
        fills_df = self._standardize_fills_df(fills_raw)
        actual_agg = self._aggregate_actual_fills(fills_df)

        if self.previous_position_path.exists():
            prev_raw = self._read_csv_auto(self.previous_position_path)
            prev_pos = self._standardize_position_df(prev_raw, actual=False)
        else:
            prev_pos = pd.DataFrame(columns=["code", "prev_shares", "prev_close_price"])

        if self.actual_position_path and self.actual_position_path.exists():
            actual_pos_raw = self._read_csv_auto(self.actual_position_path)
            actual_pos = self._standardize_position_df(actual_pos_raw, actual=True)
        else:
            actual_pos = pd.DataFrame(
                columns=[
                    "code",
                    "actual_end_shares_from_file",
                    "actual_end_market_value",
                    "actual_position_pct",
                    "actual_reference_price",
                ]
            )

        all_codes = sorted(
            set(final_plan["code"].dropna())
            | set(actual_agg["code"].dropna())
            | set(prev_pos["code"].dropna())
            | set(actual_pos["code"].dropna())
        )
        detail = pd.DataFrame({"code": all_codes})

        detail = detail.merge(final_plan, on="code", how="left")
        detail = detail.merge(actual_agg, on="code", how="left")
        detail = detail.merge(prev_pos, on="code", how="left")
        detail = detail.merge(actual_pos, on="code", how="left")

        numeric_fill_zero_cols = [
            "planned_shares",
            "planned_signed_shares",
            "actual_buy_qty",
            "actual_buy_amount",
            "actual_sell_qty",
            "actual_sell_amount",
            "actual_net_shares",
            "actual_total_fee",
            "actual_trade_count",
            "prev_shares",
        ]
        for col in numeric_fill_zero_cols:
            if col in detail.columns:
                detail[col] = pd.to_numeric(detail[col], errors="coerce").fillna(0)

        detail["planned_side"] = detail["planned_side"].fillna("hold")
        detail["planned_action"] = detail["planned_action"].fillna("未计划")
        detail["plan_source"] = detail["plan_source"].fillna("none")
        detail["plan_source_trace"] = detail["plan_source_trace"].fillna("")

        detail["relevant_actual_qty"] = np.select(
            [
                detail["planned_side"].eq("buy"),
                detail["planned_side"].eq("sell"),
            ],
            [
                detail["actual_buy_qty"],
                detail["actual_sell_qty"],
            ],
            default=0,
        )
        detail["relevant_actual_vwap"] = np.select(
            [
                detail["planned_side"].eq("buy"),
                detail["planned_side"].eq("sell"),
            ],
            [
                detail.get("actual_buy_vwap", np.nan),
                detail.get("actual_sell_vwap", np.nan),
            ],
            default=np.nan,
        )
        detail["relevant_actual_amount"] = np.select(
            [
                detail["planned_side"].eq("buy"),
                detail["planned_side"].eq("sell"),
            ],
            [
                detail["actual_buy_amount"],
                detail["actual_sell_amount"],
            ],
            default=0.0,
        )

        detail["planned_trade_amount"] = detail["planned_shares"] * pd.to_numeric(detail["planned_price"], errors="coerce").fillna(0)
        detail["actual_trade_amount"] = detail["relevant_actual_amount"].fillna(0)
        detail["trade_qty_diff"] = detail["relevant_actual_qty"] - detail["planned_shares"]

        detail["fill_rate"] = np.where(
            detail["planned_shares"] > 0,
            detail["relevant_actual_qty"] / detail["planned_shares"],
            np.nan,
        )

        detail["price_diff"] = detail["relevant_actual_vwap"] - detail["planned_price"]
        detail["price_deviation_pct"] = np.where(
            pd.to_numeric(detail["planned_price"], errors="coerce").fillna(0) > 0,
            detail["price_diff"] / detail["planned_price"] * 100,
            np.nan,
        )

        detail["adverse_slippage_bps"] = np.select(
            [
                detail["planned_side"].eq("buy") & (detail["planned_price"] > 0),
                detail["planned_side"].eq("sell") & (detail["planned_price"] > 0),
            ],
            [
                (detail["relevant_actual_vwap"] - detail["planned_price"]) / detail["planned_price"] * 10000,
                (detail["planned_price"] - detail["relevant_actual_vwap"]) / detail["planned_price"] * 10000,
            ],
            default=np.nan,
        )

        detail["planned_end_shares"] = detail["prev_shares"] + detail["planned_signed_shares"]
        detail["actual_end_shares_inferred"] = detail["prev_shares"] + detail["actual_net_shares"]
        detail["actual_end_shares"] = np.where(
            detail["actual_end_shares_from_file"].notna(),
            detail["actual_end_shares_from_file"],
            detail["actual_end_shares_inferred"],
        )
        detail["position_diff_shares"] = detail["actual_end_shares"] - detail["planned_end_shares"]

        detail["reference_price"] = detail["actual_reference_price"]
        detail["reference_price"] = detail["reference_price"].fillna(detail["prev_close_price"])
        detail["reference_price"] = detail["reference_price"].fillna(detail["planned_price"])
        detail["reference_price"] = detail["reference_price"].fillna(detail["relevant_actual_vwap"])

        detail["planned_end_market_value"] = detail["planned_end_shares"] * detail["reference_price"]
        detail["actual_end_market_value"] = np.where(
            detail["actual_end_market_value"].fillna(0) > 0,
            detail["actual_end_market_value"],
            detail["actual_end_shares"] * detail["reference_price"],
        )
        detail["position_value_diff"] = detail["actual_end_market_value"] - detail["planned_end_market_value"]

        detail["manual_trade_flag"] = np.where(
            (detail["planned_side"] == "hold") & ((detail["actual_buy_qty"] > 0) | (detail["actual_sell_qty"] > 0)),
            1,
            0,
        )

        detail["missing_trade_flag"] = np.where(
            detail["planned_shares"] > 0,
            np.where(
                detail["relevant_actual_qty"] <= np.maximum(detail["planned_shares"] - self.qty_tolerance, 0),
                1,
                0,
            ),
            0,
        )

        detail["extra_trade_flag"] = np.where(
            (
                (detail["planned_shares"] == 0)
                & ((detail["actual_buy_qty"] > 0) | (detail["actual_sell_qty"] > 0))
            )
            | (
                (detail["planned_shares"] > 0)
                & (
                    (detail["relevant_actual_qty"] >= detail["planned_shares"] + self.qty_tolerance)
                    | ((detail["planned_side"] == "buy") & (detail["actual_sell_qty"] > 0))
                    | ((detail["planned_side"] == "sell") & (detail["actual_buy_qty"] > 0))
                )
            ),
            1,
            0,
        )

        detail["side_mismatch_flag"] = np.where(
            (
                (detail["planned_side"] == "buy") & (detail["actual_sell_qty"] > 0) & (detail["actual_buy_qty"] == 0)
            )
            | (
                (detail["planned_side"] == "sell") & (detail["actual_buy_qty"] > 0) & (detail["actual_sell_qty"] == 0)
            ),
            1,
            0,
        )

        detail["price_deviation_flag"] = np.where(
            detail["adverse_slippage_bps"] >= self.price_slippage_bps_threshold,
            1,
            0,
        )

        detail["position_deviation_flag"] = np.where(
            detail["position_diff_shares"].abs() > self.qty_tolerance,
            1,
            0,
        )

        detail["match_status"] = np.select(
            [
                (detail["planned_shares"] == 0) & ((detail["actual_buy_qty"] > 0) | (detail["actual_sell_qty"] > 0)),
                (detail["planned_shares"] > 0) & (detail["relevant_actual_qty"] == 0),
                (detail["planned_shares"] > 0)
                & (detail["relevant_actual_qty"] > 0)
                & (detail["trade_qty_diff"].abs() <= self.qty_tolerance),
                (detail["planned_shares"] > 0) & (detail["relevant_actual_qty"] > 0),
            ],
            [
                "计划外成交",
                "漏成交",
                "完全匹配",
                "部分成交",
            ],
            default="无交易",
        )

        detail["anomaly_type"] = detail.apply(self._anomaly_label, axis=1)
        detail["trade_date"] = self.trade_date.strftime("%Y-%m-%d")

        detail = detail.sort_values(
            ["match_status", "execution_rank", "code"],
            ascending=[True, True, True],
            na_position="last",
        ).reset_index(drop=True)

        anomaly_df = detail[
            (
                detail["manual_trade_flag"]
                + detail["missing_trade_flag"]
                + detail["extra_trade_flag"]
                + detail["side_mismatch_flag"]
                + detail["price_deviation_flag"]
                + detail["position_deviation_flag"]
            ) > 0
        ].copy()

        position_check_df = detail[
            [
                "trade_date",
                "code",
                "plan_source_trace",
                "prev_shares",
                "planned_action",
                "planned_side",
                "planned_shares",
                "planned_end_shares",
                "actual_buy_qty",
                "actual_sell_qty",
                "actual_net_shares",
                "actual_end_shares",
                "position_diff_shares",
                "reference_price",
                "planned_end_market_value",
                "actual_end_market_value",
                "position_value_diff",
                "planned_position_pct",
                "actual_position_pct",
                "position_deviation_flag",
            ]
        ].copy()

        summary = ReconciliationSummary(
            trade_date=self.trade_date.strftime("%Y-%m-%d"),
            planned_trade_count=int((detail["planned_shares"] > 0).sum()),
            actual_trade_count=int(((detail["actual_buy_qty"] > 0) | (detail["actual_sell_qty"] > 0)).sum()),
            matched_count=int((detail["match_status"] == "完全匹配").sum()),
            partial_fill_count=int((detail["match_status"] == "部分成交").sum()),
            missing_trade_count=int((detail["missing_trade_flag"] == 1).sum()),
            extra_trade_count=int((detail["extra_trade_flag"] == 1).sum()),
            manual_trade_count=int((detail["manual_trade_flag"] == 1).sum()),
            side_mismatch_count=int((detail["side_mismatch_flag"] == 1).sum()),
            price_anomaly_count=int((detail["price_deviation_flag"] == 1).sum()),
            position_anomaly_count=int((detail["position_deviation_flag"] == 1).sum()),
            planned_trade_amount=float(detail["planned_trade_amount"].sum()),
            actual_trade_amount=float(detail["actual_trade_amount"].sum()),
            adverse_slippage_bps_avg=float(detail["adverse_slippage_bps"].dropna().mean())
            if detail["adverse_slippage_bps"].dropna().shape[0] > 0
            else 0.0,
        )

        return {
            "detail": detail,
            "anomalies": anomaly_df,
            "positions": position_check_df,
            "summary": summary,
        }

    def _write_summary_txt(self, summary: ReconciliationSummary, detail: pd.DataFrame, anomaly_df: pd.DataFrame) -> None:
        lines = [
            "=" * 60,
            "真实交易流水对账汇总",
            "=" * 60,
            f"交易日期: {summary.trade_date}",
            f"计划交易标的数: {summary.planned_trade_count}",
            f"实际交易标的数: {summary.actual_trade_count}",
            f"完全匹配数: {summary.matched_count}",
            f"部分成交数: {summary.partial_fill_count}",
            f"漏成交数: {summary.missing_trade_count}",
            f"额外成交数: {summary.extra_trade_count}",
            f"手工成交数: {summary.manual_trade_count}",
            f"方向异常数: {summary.side_mismatch_count}",
            f"价格偏差异常数: {summary.price_anomaly_count}",
            f"仓位偏差异常数: {summary.position_anomaly_count}",
            f"计划成交金额: {summary.planned_trade_amount:.2f}",
            f"实际成交金额: {summary.actual_trade_amount:.2f}",
            f"平均不利滑点(bps): {summary.adverse_slippage_bps_avg:.2f}",
            "-" * 60,
        ]

        if not anomaly_df.empty:
            lines.append("异常清单：")
            display_cols = [
                "code",
                "planned_action",
                "planned_shares",
                "relevant_actual_qty",
                "trade_qty_diff",
                "adverse_slippage_bps",
                "position_diff_shares",
                "anomaly_type",
            ]
            preview = anomaly_df[display_cols].head(20).fillna("")
            lines.append(preview.to_string(index=False))
        else:
            lines.append("异常清单：无")

        lines.append("=" * 60)

        with open(self.output_summary_path, "w", encoding="utf-8-sig") as f:
            f.write("\n".join(lines))

    def run(self) -> Dict[str, pd.DataFrame | ReconciliationSummary]:
        result = self._build_reconciliation()

        detail = result["detail"]
        anomalies = result["anomalies"]
        positions = result["positions"]
        summary = result["summary"]

        detail.to_csv(self.output_detail_path, index=False, encoding="utf-8-sig")
        anomalies.to_csv(self.output_anomaly_path, index=False, encoding="utf-8-sig")
        positions.to_csv(self.output_position_path, index=False, encoding="utf-8-sig")
        self._write_summary_txt(summary, detail, anomalies)

        return result