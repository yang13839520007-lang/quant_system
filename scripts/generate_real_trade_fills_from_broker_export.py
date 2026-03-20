# -*- coding: utf-8 -*-
"""
Created on Wed Mar 18 01:07:58 2026

@author: DELL
"""

from __future__ import annotations

import argparse
from pathlib import Path

from core.broker_trade_fill_standardizer import BrokerTradeFillStandardizer


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="将券商原始成交导出标准化为 real_trade_fills.csv")
    parser.add_argument("--trade_date", type=str, required=True, help="交易日期，例如 2026-03-17")
    parser.add_argument("--input_path", type=str, required=True, help="券商原始导出文件路径，支持 csv/xlsx/xls")
    parser.add_argument(
        "--project_root",
        type=str,
        default=str(Path(__file__).resolve().parents[1]),
        help="项目根目录",
    )
    parser.add_argument(
        "--output_path",
        type=str,
        default="",
        help="默认 reports/real_trade_fills.csv",
    )
    parser.add_argument(
        "--audit_output_path",
        type=str,
        default="",
        help="默认 reports/real_trade_fills_standardization_audit.csv",
    )
    parser.add_argument(
        "--summary_output_path",
        type=str,
        default="",
        help="默认 reports/real_trade_fills_standardization_summary.txt",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    project_root = Path(args.project_root)
    reports_dir = project_root / "reports"

    output_path = Path(args.output_path) if args.output_path else reports_dir / "real_trade_fills.csv"
    audit_output_path = (
        Path(args.audit_output_path)
        if args.audit_output_path
        else reports_dir / "real_trade_fills_standardization_audit.csv"
    )
    summary_output_path = (
        Path(args.summary_output_path)
        if args.summary_output_path
        else reports_dir / "real_trade_fills_standardization_summary.txt"
    )

    standardizer = BrokerTradeFillStandardizer(
        trade_date=args.trade_date,
        input_path=args.input_path,
        output_path=output_path,
        audit_output_path=audit_output_path,
        summary_output_path=summary_output_path,
    )
    result = standardizer.run()
    summary = result["summary"]
    valid_df = result["valid_df"]

    print("=" * 60)
    print("券商原始成交标准化完成")
    print(f"目标交易日: {args.trade_date}")
    print(f"原始文件: {args.input_path}")
    print(f"有效成交行数: {summary['valid_rows']}")
    print(f"无效行数: {summary['invalid_rows']}")
    print(f"日期过滤行数: {summary['date_filtered_rows']}")
    print(f"买入成交行数: {summary['buy_rows']}")
    print(f"卖出成交行数: {summary['sell_rows']}")
    print(f"标准输出文件: {output_path}")
    print(f"审计文件: {audit_output_path}")
    print(f"汇总文件: {summary_output_path}")
    print("=" * 60)

    if not valid_df.empty:
        print(valid_df.head(20).to_string(index=False))


if __name__ == "__main__":
    main()