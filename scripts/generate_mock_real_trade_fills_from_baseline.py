# -*- coding: utf-8 -*-
"""
Created on Wed Mar 18 01:00:45 2026

@author: DELL
"""

from __future__ import annotations

import argparse
import re
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


def infer_market(code_digits: str) -> str:
    if not code_digits or len(code_digits) != 6:
        return ""
    return "sh" if code_digits.startswith(("5", "6", "9")) else "sz"


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
        market = infer_market(digits)
        return f"{market}.{digits}"
    return code


def parse_trade_date_value(x) -> pd.Timestamp:
    if pd.isna(x):
        return pd.NaT

    s = str(x).strip()
    if s == "" or s.lower() in {"nan", "nat", "none"}:
        return pd.NaT

    s = s.replace("/", "-").replace(".", "-")

    if re.fullmatch(r"\d{8}", s):
        return pd.to_datetime(s, format="%Y%m%d", errors="coerce")

    return pd.to_datetime(s, errors="coerce")


def parse_trade_date_series(series: pd.Series) -> pd.Series:
    return series.map(parse_trade_date_value).dt.normalize()


def find_stock_file(stock_data_dir: Path, code: str) -> Optional[Path]:
    exact_path = stock_data_dir / f"{code}.csv"
    if exact_path.exists():
        return exact_path

    digits = "".join(ch for ch in code if ch.isdigit())
    market = code[:2]

    candidates = [
        stock_data_dir / f"{digits}.csv",
        stock_data_dir / f"{market}{digits}.csv",
        stock_data_dir / f"{digits}.{market}.csv",
        stock_data_dir / f"{market}.{digits}.csv",
        stock_data_dir / f"{digits}.{market.upper()}.csv",
        stock_data_dir / f"{market.upper()}.{digits}.csv",
    ]

    for path in candidates:
        if path.exists():
            return path

    for path in stock_data_dir.glob("*.csv"):
        stem = path.stem.lower()
        if digits in stem and market in stem:
            return path

    return None


def read_stock_bar(stock_file: Path, trade_date: pd.Timestamp) -> Optional[pd.Series]:
    df = read_csv_auto(stock_file)
    original_cols = [str(c).strip() for c in df.columns]
    df.columns = [c.lower() for c in original_cols]

    date_col = first_existing_column(df, ["trade_date", "date", "datetime", "cal_date", "日期", "交易日期"])
    open_col = first_existing_column(df, ["open", "开盘", "开盘价"])
    high_col = first_existing_column(df, ["high", "最高", "最高价"])
    low_col = first_existing_column(df, ["low", "最低", "最低价"])
    close_col = first_existing_column(df, ["close", "收盘", "收盘价"])

    if date_col is None or open_col is None or high_col is None or low_col is None or close_col is None:
        return None

    temp = df.copy()
    temp["__date__"] = parse_trade_date_series(temp[date_col])
    temp = temp.dropna(subset=["__date__"]).copy()

    if temp.empty:
        return None

    exact = temp[temp["__date__"] == trade_date].copy()
    if not exact.empty:
        row = exact.sort_values("__date__").iloc[-1]
        return pd.Series(
            {
                "bar_date": row["__date__"],
                "date_fallback_flag": 0,
                "open": pd.to_numeric(row[open_col], errors="coerce"),
                "high": pd.to_numeric(row[high_col], errors="coerce"),
                "low": pd.to_numeric(row[low_col], errors="coerce"),
                "close": pd.to_numeric(row[close_col], errors="coerce"),
            }
        )

    hist = temp[temp["__date__"] <= trade_date].copy()
    if hist.empty:
        return None

    row = hist.sort_values("__date__").iloc[-1]
    return pd.Series(
        {
            "bar_date": row["__date__"],
            "date_fallback_flag": 1,
            "open": pd.to_numeric(row[open_col], errors="coerce"),
            "high": pd.to_numeric(row[high_col], errors="coerce"),
            "low": pd.to_numeric(row[low_col], errors="coerce"),
            "close": pd.to_numeric(row[close_col], errors="coerce"),
        }
    )


def simulate_fill(side: str, planned_price: float, planned_shares: int, bar: pd.Series,
                  buy_slippage_bps: float, sell_slippage_bps: float) -> dict:
    o = float(bar["open"])
    h = float(bar["high"])
    l = float(bar["low"])

    if planned_shares <= 0 or side not in {"buy", "sell"}:
        return {
            "filled": False,
            "fill_price": np.nan,
            "fill_time": "",
            "fill_logic": "非交易计划",
        }

    if pd.isna(planned_price) or planned_price <= 0:
        if side == "buy":
            return {
                "filled": True,
                "fill_price": round(o * (1 + buy_slippage_bps / 10000), 4),
                "fill_time": "09:35:00",
                "fill_logic": "无计划价-按开盘价模拟买入",
            }
        return {
            "filled": True,
            "fill_price": round(o * (1 - sell_slippage_bps / 10000), 4),
            "fill_time": "09:35:00",
            "fill_logic": "无计划价-按开盘价模拟卖出",
        }

    if side == "buy":
        if o <= planned_price:
            return {
                "filled": True,
                "fill_price": round(o * (1 + buy_slippage_bps / 10000), 4),
                "fill_time": "09:35:00",
                "fill_logic": "买入-开盘可成交",
            }
        if l <= planned_price <= h:
            return {
                "filled": True,
                "fill_price": round(planned_price * (1 + buy_slippage_bps / 10000), 4),
                "fill_time": "10:30:00",
                "fill_logic": "买入-盘中触发计划价",
            }
        return {
            "filled": False,
            "fill_price": np.nan,
            "fill_time": "",
            "fill_logic": "买入-全天未触发计划价",
        }

    if o >= planned_price:
        return {
            "filled": True,
            "fill_price": round(o * (1 - sell_slippage_bps / 10000), 4),
            "fill_time": "09:35:00",
            "fill_logic": "卖出-开盘可成交",
        }
    if l <= planned_price <= h:
        return {
            "filled": True,
            "fill_price": round(planned_price * (1 - sell_slippage_bps / 10000), 4),
            "fill_time": "10:30:00",
            "fill_logic": "卖出-盘中触发计划价",
        }
    return {
        "filled": False,
        "fill_price": np.nan,
        "fill_time": "",
        "fill_logic": "卖出-全天未触发计划价",
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="基于联调基准计划生成模拟真实成交流水")
    parser.add_argument("--trade_date", type=str, required=True, help="交易日期，例如 2026-03-17")
    parser.add_argument(
        "--project_root",
        type=str,
        default=str(Path(__file__).resolve().parents[1]),
        help="项目根目录",
    )
    parser.add_argument(
        "--plan_path",
        type=str,
        default="",
        help="基准计划文件路径，默认 reports/daily_trade_reconciliation_plan_baseline.csv",
    )
    parser.add_argument(
        "--stock_data_dir",
        type=str,
        default="",
        help="日线目录，默认 project_root/stock_data_5years",
    )
    parser.add_argument(
        "--output_path",
        type=str,
        default="",
        help="输出模拟成交文件路径，默认 reports/real_trade_fills.csv",
    )
    parser.add_argument(
        "--detail_output_path",
        type=str,
        default="",
        help="输出明细文件路径，默认 reports/mock_fill_generation_detail.csv",
    )
    parser.add_argument("--buy_slippage_bps", type=float, default=8.0)
    parser.add_argument("--sell_slippage_bps", type=float, default=8.0)
    parser.add_argument("--commission_rate", type=float, default=0.0003)
    parser.add_argument("--sell_tax_rate", type=float, default=0.0005)
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    project_root = Path(args.project_root)
    reports_dir = project_root / "reports"
    trade_date = pd.Timestamp(args.trade_date).normalize()

    plan_path = Path(args.plan_path) if args.plan_path else reports_dir / "daily_trade_reconciliation_plan_baseline.csv"
    stock_data_dir = Path(args.stock_data_dir) if args.stock_data_dir else project_root / "stock_data_5years"
    output_path = Path(args.output_path) if args.output_path else reports_dir / "real_trade_fills.csv"
    detail_output_path = Path(args.detail_output_path) if args.detail_output_path else reports_dir / "mock_fill_generation_detail.csv"

    if not plan_path.exists():
        raise FileNotFoundError(f"未找到基准计划文件: {plan_path}")
    if not stock_data_dir.exists():
        raise FileNotFoundError(f"未找到日线目录: {stock_data_dir}")

    plan_df = read_csv_auto(plan_path)
    plan_df.columns = [str(c).strip() for c in plan_df.columns]

    code_col = first_existing_column(plan_df, ["code", "证券代码", "股票代码", "代码"])
    action_col = first_existing_column(plan_df, ["action", "planned_action", "操作", "交易动作"])
    shares_col = first_existing_column(plan_df, ["planned_shares", "成交股数", "成交数量", "股数", "数量"])
    price_col = first_existing_column(plan_df, ["planned_price", "avg_fill_price", "成交均价", "价格", "price"])
    rank_col = first_existing_column(plan_df, ["execution_rank", "rank", "排序"])

    if code_col is None or shares_col is None or price_col is None:
        raise ValueError("基准计划文件至少需要 code / planned_shares / planned_price")

    plan = pd.DataFrame()
    plan["code"] = plan_df[code_col].astype(str).map(normalize_code)
    plan["planned_action"] = plan_df[action_col].astype(str) if action_col else "正常买入"
    plan["planned_side"] = "buy"
    plan["planned_shares"] = pd.to_numeric(plan_df[shares_col], errors="coerce").fillna(0).astype(int)
    plan["planned_price"] = pd.to_numeric(plan_df[price_col], errors="coerce")
    plan["execution_rank"] = pd.to_numeric(plan_df[rank_col], errors="coerce") if rank_col else np.nan

    plan = plan[(plan["planned_shares"] > 0) & (plan["planned_price"] > 0)].copy()
    plan = plan.sort_values(["execution_rank", "code"], na_position="last").reset_index(drop=True)

    detail_rows = []
    fill_rows = []

    for _, row in plan.iterrows():
        code = row["code"]
        stock_file = find_stock_file(stock_data_dir, code)

        base_info = {
            "trade_date": trade_date.strftime("%Y-%m-%d"),
            "code": code,
            "planned_action": row["planned_action"],
            "planned_shares": int(row["planned_shares"]),
            "planned_price": float(row["planned_price"]),
            "execution_rank": row["execution_rank"],
            "stock_file": str(stock_file) if stock_file else "",
        }

        if stock_file is None:
            detail_rows.append(
                {
                    **base_info,
                    "status": "失败",
                    "reason": "未匹配到日线文件",
                    "bar_date": "",
                    "date_fallback_flag": 0,
                    "fill_price": np.nan,
                    "fill_time": "",
                    "fill_logic": "",
                }
            )
            continue

        bar = read_stock_bar(stock_file, trade_date)
        if bar is None:
            detail_rows.append(
                {
                    **base_info,
                    "status": "失败",
                    "reason": "日线日期列或OHLC列未识别",
                    "bar_date": "",
                    "date_fallback_flag": 0,
                    "fill_price": np.nan,
                    "fill_time": "",
                    "fill_logic": "",
                }
            )
            continue

        sim = simulate_fill(
            side="buy",
            planned_price=float(row["planned_price"]),
            planned_shares=int(row["planned_shares"]),
            bar=bar,
            buy_slippage_bps=float(args.buy_slippage_bps),
            sell_slippage_bps=float(args.sell_slippage_bps),
        )

        if not sim["filled"]:
            detail_rows.append(
                {
                    **base_info,
                    "status": "未成交",
                    "reason": sim["fill_logic"],
                    "bar_date": pd.Timestamp(bar["bar_date"]).strftime("%Y-%m-%d"),
                    "date_fallback_flag": int(bar["date_fallback_flag"]),
                    "fill_price": np.nan,
                    "fill_time": "",
                    "fill_logic": sim["fill_logic"],
                }
            )
            continue

        filled_shares = int(row["planned_shares"])
        filled_price = float(sim["fill_price"])
        filled_amount = round(filled_shares * filled_price, 2)
        commission = round(filled_amount * float(args.commission_rate), 2)

        fill_rows.append(
            {
                "trade_date": trade_date.strftime("%Y-%m-%d"),
                "trade_time": sim["fill_time"],
                "code": code,
                "side": "buy",
                "filled_shares": filled_shares,
                "filled_price": filled_price,
                "filled_amount": filled_amount,
                "commission": commission,
                "order_id": f"SIMORD_{trade_date.strftime('%Y%m%d')}_{code.replace('.', '').upper()}",
                "deal_id": f"SIMDL_{trade_date.strftime('%Y%m%d')}_{code.replace('.', '').upper()}",
            }
        )

        detail_rows.append(
            {
                **base_info,
                "status": "已生成模拟成交",
                "reason": "",
                "bar_date": pd.Timestamp(bar["bar_date"]).strftime("%Y-%m-%d"),
                "date_fallback_flag": int(bar["date_fallback_flag"]),
                "fill_price": filled_price,
                "fill_time": sim["fill_time"],
                "fill_logic": sim["fill_logic"],
            }
        )

    detail_df = pd.DataFrame(detail_rows)
    fill_columns = [
        "trade_date", "trade_time", "code", "side", "filled_shares", "filled_price",
        "filled_amount", "commission", "order_id", "deal_id"
    ]
    fills_df = pd.DataFrame(fill_rows, columns=fill_columns)

    detail_df.to_csv(detail_output_path, index=False, encoding="utf-8-sig")
    fills_df.to_csv(output_path, index=False, encoding="utf-8-sig")

    print("=" * 60)
    print("基于基准计划的模拟成交流水生成完成")
    print(f"交易日期: {trade_date.strftime('%Y-%m-%d')}")
    print(f"计划交易数: {len(plan)}")
    print(f"模拟成交数: {len(fills_df)}")
    print(f"未成交数: {(detail_df['status'] == '未成交').sum() if not detail_df.empty else 0}")
    print(f"失败数: {(detail_df['status'] == '失败').sum() if not detail_df.empty else 0}")
    print(f"模拟成交文件: {output_path}")
    print(f"生成明细文件: {detail_output_path}")
    print("=" * 60)

    if not detail_df.empty:
        show_cols = [
            "code", "planned_shares", "planned_price", "status",
            "reason", "bar_date", "date_fallback_flag", "stock_file", "fill_price"
        ]
        print(detail_df[show_cols].to_string(index=False))


if __name__ == "__main__":
    main()