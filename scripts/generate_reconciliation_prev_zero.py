# -*- coding: utf-8 -*-
"""
Created on Wed Mar 18 01:01:26 2026

@author: DELL
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional

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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="为第13段联调生成零昨日持仓文件")
    parser.add_argument(
        "--project_root",
        type=str,
        default=str(Path(__file__).resolve().parents[1]),
    )
    parser.add_argument(
        "--plan_path",
        type=str,
        default="",
        help="默认 reports/daily_trade_reconciliation_plan_baseline.csv",
    )
    parser.add_argument(
        "--output_path",
        type=str,
        default="",
        help="默认 reports/daily_trade_reconciliation_prev_zero.csv",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    project_root = Path(args.project_root)
    reports_dir = project_root / "reports"

    plan_path = Path(args.plan_path) if args.plan_path else reports_dir / "daily_trade_reconciliation_plan_baseline.csv"
    output_path = Path(args.output_path) if args.output_path else reports_dir / "daily_trade_reconciliation_prev_zero.csv"

    if not plan_path.exists():
        raise FileNotFoundError(f"未找到基准计划文件: {plan_path}")

    df = read_csv_auto(plan_path)
    df.columns = [str(c).strip() for c in df.columns]

    code_col = first_existing_column(df, ["code", "证券代码", "股票代码", "代码"])
    price_col = first_existing_column(df, ["planned_price", "avg_fill_price", "成交均价", "价格", "price"])

    if code_col is None:
        raise ValueError("基准计划文件缺少 code 字段")

    out = pd.DataFrame()
    out["code"] = df[code_col].astype(str)
    out["position_shares"] = 0
    out["close_price"] = pd.to_numeric(df[price_col], errors="coerce") if price_col else 0.0

    out = out.drop_duplicates(subset=["code"], keep="last").reset_index(drop=True)
    out.to_csv(output_path, index=False, encoding="utf-8-sig")

    print("=" * 60)
    print("第13段联调零昨日持仓文件生成完成")
    print(f"输出文件: {output_path}")
    print(f"标的数: {len(out)}")
    print("=" * 60)
    print(out.to_string(index=False))


if __name__ == "__main__":
    main()