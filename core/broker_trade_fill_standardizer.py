# -*- coding: utf-8 -*-
"""
Created on Wed Mar 18 01:07:04 2026

@author: DELL
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Optional, Dict, Any

import numpy as np
import pandas as pd


class BrokerTradeFillStandardizer:
    """
    第13.1段：券商原始流水标准化适配器

    目标：
    将券商原始导出成交文件（csv/xlsx/xls）标准化为系统统一口径：
    reports/real_trade_fills.csv

    标准输出字段：
    - trade_date
    - trade_time
    - code
    - side
    - filled_shares
    - filled_price
    - filled_amount
    - commission
    - order_id
    - deal_id
    """

    def __init__(
        self,
        trade_date: str,
        input_path: str | Path,
        output_path: str | Path,
        audit_output_path: str | Path,
        summary_output_path: str | Path,
    ) -> None:
        self.trade_date = pd.Timestamp(trade_date).normalize()
        self.input_path = Path(input_path)
        self.output_path = Path(output_path)
        self.audit_output_path = Path(audit_output_path)
        self.summary_output_path = Path(summary_output_path)

    @staticmethod
    def _read_table_auto(path: Path) -> pd.DataFrame:
        if not path.exists():
            raise FileNotFoundError(f"未找到文件: {path}")

        suffix = path.suffix.lower()

        if suffix in {".xlsx", ".xls"}:
            return pd.read_excel(path)

        if suffix == ".csv":
            encodings = ["utf-8-sig", "utf-8", "gbk", "gb2312", "utf-16", "latin1"]
            last_error = None
            for encoding in encodings:
                try:
                    return pd.read_csv(path, encoding=encoding)
                except Exception as exc:
                    last_error = exc
            raise ValueError(f"CSV 读取失败: {path} | {last_error}")

        raise ValueError(f"暂不支持的文件格式: {path.suffix}")

    @staticmethod
    def _first_existing_column(df: pd.DataFrame, candidates: list[str]) -> Optional[str]:
        for col in candidates:
            if col in df.columns:
                return col
        return None

    @staticmethod
    def _infer_market(code_digits: str) -> str:
        if not code_digits or len(code_digits) != 6:
            return ""
        return "sh" if code_digits.startswith(("5", "6", "9")) else "sz"

    def _normalize_code(self, code: Any) -> str:
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

    @staticmethod
    def _normalize_side(side_value: Any) -> str:
        text = str(side_value).strip().lower()

        buy_keywords = [
            "买", "买入", "证券买入", "买入成交", "普通买入", "融资买入",
            "buy", "b", "long"
        ]
        sell_keywords = [
            "卖", "卖出", "证券卖出", "卖出成交", "普通卖出", "融券卖出",
            "sell", "s", "short"
        ]

        if any(k == text or k in text for k in sell_keywords):
            return "sell"
        if any(k == text or k in text for k in buy_keywords):
            return "buy"
        return ""

    @staticmethod
    def _parse_date_value(x: Any) -> pd.Timestamp:
        if pd.isna(x):
            return pd.NaT

        # Excel serial date
        if isinstance(x, (int, float)) and 30000 < float(x) < 60000:
            try:
                return pd.Timestamp("1899-12-30") + pd.to_timedelta(float(x), unit="D")
            except Exception:
                pass

        s = str(x).strip()
        if s == "" or s.lower() in {"nan", "nat", "none"}:
            return pd.NaT

        s = s.replace("/", "-").replace(".", "-")

        if re.fullmatch(r"\d{8}", s):
            return pd.to_datetime(s, format="%Y%m%d", errors="coerce")

        return pd.to_datetime(s, errors="coerce")

    def _parse_date_series(self, series: pd.Series) -> pd.Series:
        return series.map(self._parse_date_value).dt.normalize()

    @staticmethod
    def _normalize_time_value(x: Any) -> str:
        if pd.isna(x):
            return ""

        if isinstance(x, pd.Timestamp):
            return x.strftime("%H:%M:%S")

        s = str(x).strip()
        if s == "" or s.lower() in {"nan", "nat", "none"}:
            return ""

        # 93015 -> 09:30:15
        digits = re.sub(r"\D", "", s)
        if len(digits) == 6:
            return f"{digits[0:2]}:{digits[2:4]}:{digits[4:6]}"
        if len(digits) == 5:
            return f"0{digits[0]}:{digits[1:3]}:{digits[3:5]}"
        if len(digits) == 4:
            return f"{digits[0:2]}:{digits[2:4]}:00"

        try:
            ts = pd.to_datetime(s, errors="coerce")
            if pd.notna(ts):
                return ts.strftime("%H:%M:%S")
        except Exception:
            pass

        return s

    def _detect_columns(self, df: pd.DataFrame) -> Dict[str, Optional[str]]:
        return {
            "code": self._first_existing_column(
                df, ["code", "ts_code", "symbol", "stock_code", "证券代码", "股票代码", "代码", "证券"]
            ),
            "side": self._first_existing_column(
                df, ["side", "bs_flag", "买卖标志", "买卖方向", "方向", "操作", "业务名称", "交易类型"]
            ),
            "qty": self._first_existing_column(
                df, ["filled_shares", "成交数量", "成交股数", "数量", "成交数", "股数", "成交数量(股)"]
            ),
            "price": self._first_existing_column(
                df, ["filled_price", "成交均价", "成交价格", "均价", "价格", "price", "成交价"]
            ),
            "amount": self._first_existing_column(
                df, ["filled_amount", "成交金额", "金额", "发生金额", "amount", "成交额"]
            ),
            "date": self._first_existing_column(
                df, ["trade_date", "date", "成交日期", "日期", "业务日期", "发生日期"]
            ),
            "time": self._first_existing_column(
                df, ["trade_time", "time", "成交时间", "时间", "发生时间"]
            ),
            "commission": self._first_existing_column(
                df, ["commission", "fee", "手续费", "佣金", "交易费用", "总费用", "费用合计"]
            ),
            "order_id": self._first_existing_column(
                df, ["order_id", "委托编号", "委托序号", "订单编号", "委托合同号"]
            ),
            "deal_id": self._first_existing_column(
                df, ["deal_id", "contract_id", "成交编号", "合同编号", "成交序号", "成交合同号"]
            ),
        }

    def _standardize(self, raw_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
        df = raw_df.copy()
        df.columns = [str(c).strip() for c in df.columns]

        colmap = self._detect_columns(df)

        required_core = ["code", "side", "qty", "price"]
        missing_required = [k for k in required_core if colmap[k] is None]
        if missing_required:
            raise ValueError(
                "券商原始文件缺少必要字段，至少需要识别到：代码 / 买卖方向 / 成交数量 / 成交价格"
            )

        std = pd.DataFrame()
        std["trade_date"] = (
            self._parse_date_series(df[colmap["date"]])
            if colmap["date"] is not None
            else self.trade_date
        )
        std["trade_time"] = (
            df[colmap["time"]].map(self._normalize_time_value)
            if colmap["time"] is not None
            else ""
        )
        std["code"] = df[colmap["code"]].map(self._normalize_code)
        std["side"] = df[colmap["side"]].map(self._normalize_side)
        std["filled_shares"] = pd.to_numeric(df[colmap["qty"]], errors="coerce")
        std["filled_price"] = pd.to_numeric(df[colmap["price"]], errors="coerce")

        if colmap["amount"] is not None:
            std["filled_amount"] = pd.to_numeric(df[colmap["amount"]], errors="coerce")
        else:
            std["filled_amount"] = std["filled_shares"] * std["filled_price"]

        if colmap["commission"] is not None:
            std["commission"] = pd.to_numeric(df[colmap["commission"]], errors="coerce")
        else:
            std["commission"] = 0.0

        std["order_id"] = df[colmap["order_id"]].astype(str) if colmap["order_id"] is not None else ""
        std["deal_id"] = df[colmap["deal_id"]].astype(str) if colmap["deal_id"] is not None else ""

        std["trade_date"] = std["trade_date"].fillna(self.trade_date)
        std["filled_shares"] = std["filled_shares"].fillna(0)
        std["filled_price"] = std["filled_price"].fillna(np.nan)
        std["filled_amount"] = std["filled_amount"].fillna(std["filled_shares"] * std["filled_price"])
        std["commission"] = std["commission"].fillna(0.0)

        std["row_status"] = "有效"
        std.loc[std["code"].isna() | (std["code"].astype(str).str.len() < 6), "row_status"] = "无效-代码异常"
        std.loc[~std["side"].isin(["buy", "sell"]), "row_status"] = "无效-方向异常"
        std.loc[std["filled_shares"] <= 0, "row_status"] = "无效-数量异常"
        std.loc[std["filled_price"].isna() | (std["filled_price"] <= 0), "row_status"] = "无效-价格异常"

        std["trade_date_match_flag"] = np.where(std["trade_date"] == self.trade_date, 1, 0)

        audit_df = std.copy()

        valid_df = std[
            (std["row_status"] == "有效") &
            (std["trade_date"] == self.trade_date)
        ].copy()

        valid_df = valid_df[
            [
                "trade_date",
                "trade_time",
                "code",
                "side",
                "filled_shares",
                "filled_price",
                "filled_amount",
                "commission",
                "order_id",
                "deal_id",
            ]
        ].reset_index(drop=True)

        summary = {
            "input_rows": int(len(df)),
            "valid_rows": int(len(valid_df)),
            "invalid_rows": int((audit_df["row_status"] != "有效").sum()),
            "date_filtered_rows": int((audit_df["trade_date_match_flag"] == 0).sum()),
            "buy_rows": int((valid_df["side"] == "buy").sum()),
            "sell_rows": int((valid_df["side"] == "sell").sum()),
            "mapped_columns": colmap,
        }

        return valid_df, audit_df, summary

    def _write_summary(self, summary: Dict[str, Any]) -> None:
        lines = [
            "=" * 60,
            "券商原始流水标准化汇总",
            "=" * 60,
            f"目标交易日: {self.trade_date.strftime('%Y-%m-%d')}",
            f"原始文件: {self.input_path}",
            f"标准输出文件: {self.output_path}",
            f"审计文件: {self.audit_output_path}",
            "-" * 60,
            f"原始行数: {summary['input_rows']}",
            f"有效行数: {summary['valid_rows']}",
            f"无效行数: {summary['invalid_rows']}",
            f"日期过滤行数: {summary['date_filtered_rows']}",
            f"买入成交行数: {summary['buy_rows']}",
            f"卖出成交行数: {summary['sell_rows']}",
            "-" * 60,
            "字段映射：",
        ]

        for k, v in summary["mapped_columns"].items():
            lines.append(f"{k}: {v}")

        lines.append("=" * 60)

        with open(self.summary_output_path, "w", encoding="utf-8-sig") as f:
            f.write("\n".join(lines))

    def run(self) -> Dict[str, Any]:
        raw_df = self._read_table_auto(self.input_path)
        valid_df, audit_df, summary = self._standardize(raw_df)

        valid_df.to_csv(self.output_path, index=False, encoding="utf-8-sig")
        audit_df.to_csv(self.audit_output_path, index=False, encoding="utf-8-sig")
        self._write_summary(summary)

        return {
            "valid_df": valid_df,
            "audit_df": audit_df,
            "summary": summary,
        }