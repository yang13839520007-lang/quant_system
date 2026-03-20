# -*- coding: utf-8 -*-
"""
Created on Tue Mar 17 23:53:41 2026

@author: DELL
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd


def read_csv_auto(path: Path) -> pd.DataFrame:
    encodings = ["utf-8-sig", "utf-8", "gbk", "gb2312", "utf-16", "latin1"]
    last_error = None
    for encoding in encodings:
        try:
            return pd.read_csv(path, encoding=encoding)
        except Exception as exc:
            last_error = exc
    raise ValueError(f"CSV 读取失败: {path} | {last_error}")


def first_existing_column(df: pd.DataFrame, candidates: list[str]) -> Optional[str]:
    for col in candidates:
        if col in df.columns:
            return col
    return None


def normalize_code(code: str) -> str:
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
        market = "sh" if digits.startswith(("5", "6", "9")) else "sz"
        return f"{market}.{digits}"
    return code


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="基于 daily_close_review 生成第13段联调基准计划文件")
    parser.add_argument(
        "--trade_date",
        type=str,
        required=True,
        help="交易日期，例如 2026-03-17",
    )
    parser.add_argument(
        "--project_root",
        type=str,
        default=str(Path(__file__).resolve().parents[1]),
        help="项目根目录",
    )
    parser.add_argument(
        "--close_review_path",
        type=str,
        default="",
        help="收盘复盘文件路径，默认 reports/daily_close_review.csv",
    )
    parser.add_argument(
        "--output_path",
        type=str,
        default="",
        help="输出基准计划文件路径，默认 reports/daily_trade_reconciliation_plan_baseline.csv",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    project_root = Path(args.project_root)
    reports_dir = project_root / "reports"

    close_review_path = Path(args.close_review_path) if args.close_review_path else reports_dir / "daily_close_review.csv"
    output_path = (
        Path(args.output_path)
        if args.output_path
        else reports_dir / "daily_trade_reconciliation_plan_baseline.csv"
    )

    if not close_review_path.exists():
        raise FileNotFoundError(f"未找到收盘复盘文件: {close_review_path}")

    df = read_csv_auto(close_review_path)
    df.columns = [str(c).strip() for c in df.columns]

    code_col = first_existing_column(df, ["code", "ts_code", "symbol", "证券代码", "股票代码", "代码"])
    shares_col = first_existing_column(df, ["filled_shares", "成交股数", "成交数量", "持仓股数", "持仓数量", "股数"])
    price_col = first_existing_column(df, ["avg_fill_price", "成交均价", "filled_price", "entry_price", "买入价格", "价格"])
    rank_col = first_existing_column(df, ["execution_rank", "rank", "priority_rank", "排序", "执行优先级"])
    status_col = first_existing_column(df, ["position_status", "status", "持仓状态"])

    if code_col is None or shares_col is None or price_col is None:
        raise ValueError("daily_close_review.csv 至少需要 code / filled_shares / avg_fill_price 三类字段")

    out = pd.DataFrame()
    out["trade_date"] = args.trade_date
    out["code"] = df[code_col].astype(str).map(normalize_code)
    out["action"] = "正常买入"
    out["planned_shares"] = pd.to_numeric(df[shares_col], errors="coerce").fillna(0).astype(int)
    out["planned_price"] = pd.to_numeric(df[price_col], errors="coerce")
    out["execution_rank"] = pd.to_numeric(df[rank_col], errors="coerce") if rank_col else np.nan
    out["position_status"] = df[status_col].astype(str) if status_col else ""
    out["plan_source"] = "daily_close_review_baseline"

    out = out[(out["code"].notna()) & (out["planned_shares"] > 0) & (out["planned_price"] > 0)].copy()
    out = out.sort_values(["execution_rank", "code"], na_position="last").reset_index(drop=True)

    out.to_csv(output_path, index=False, encoding="utf-8-sig")

    print("=" * 60)
    print("第13段联调基准计划文件生成完成")
    print(f"交易日期: {args.trade_date}")
    print(f"基准计划数: {len(out)}")
    print(f"输出文件: {output_path}")
    print("=" * 60)

    if not out.empty:
        print(out.head(20).to_string(index=False))


if __name__ == "__main__":
    main()