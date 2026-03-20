from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Dict, Optional

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


def classify_side(action_text: str) -> str:
    text = str(action_text).strip().lower()
    if text in {"nan", "none", ""}:
        return "hold"

    buy_keywords = ["买", "买入", "建仓", "加仓", "补仓", "开仓", "buy", "long", "b", "正常买入"]
    sell_keywords = ["卖", "卖出", "减仓", "清仓", "止盈", "止损", "平仓", "sell", "short", "s"]
    hold_keywords = ["持有", "观察", "跟踪", "续持", "hold", "watch", "不动"]

    if any(k == text or k in text for k in sell_keywords):
        return "sell"
    if any(k == text or k in text for k in buy_keywords):
        return "buy"
    if any(k in text for k in hold_keywords):
        return "hold"
    return "hold"


def last_valid(series: pd.Series):
    s = series.dropna()
    if s.empty:
        return np.nan
    return s.iloc[-1]


def last_valid_text(series: pd.Series, invalid_values: Optional[list[str]] = None) -> str:
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


def standardize_plan_df(raw_df: pd.DataFrame, source_name: str, source_priority: int) -> pd.DataFrame:
    df = raw_df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    code_col = first_existing_column(
        df, ["code", "ts_code", "symbol", "stock_code", "证券代码", "股票代码", "代码"]
    )
    action_col = first_existing_column(
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
    shares_col = first_existing_column(
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
    price_col = first_existing_column(
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
    rank_col = first_existing_column(
        df, ["execution_rank", "rank", "priority_rank", "priority", "排序", "执行优先级"]
    )
    date_col = first_existing_column(
        df, ["trade_date", "date", "交易日期", "目标日期", "signal_date", "target_date"]
    )

    if code_col is None:
        raise ValueError(f"计划文件缺少代码字段: {source_name}")

    result = pd.DataFrame()
    result["code"] = df[code_col].astype(str).map(normalize_code)
    result["planned_action"] = df[action_col].astype(str) if action_col else np.nan
    result["planned_side"] = result["planned_action"].map(classify_side)
    result["planned_shares"] = pd.to_numeric(df[shares_col], errors="coerce") if shares_col else np.nan
    result["planned_price"] = pd.to_numeric(df[price_col], errors="coerce") if price_col else np.nan
    result["execution_rank"] = pd.to_numeric(df[rank_col], errors="coerce") if rank_col else np.nan
    result["trade_date"] = pd.to_datetime(df[date_col], errors="coerce").dt.normalize() if date_col else pd.NaT
    result["plan_source"] = source_name
    result["plan_priority"] = int(source_priority)

    result = result.dropna(subset=["code"]).drop_duplicates(subset=["code"], keep="last")
    return result.reset_index(drop=True)


def load_final_plan(plan_paths: list[Path]) -> pd.DataFrame:
    plan_frames = []
    for idx, path in enumerate(plan_paths, start=1):
        if not path.exists():
            continue
        raw_df = read_csv_auto(path)
        std_df = standardize_plan_df(raw_df, path.name, idx)
        plan_frames.append(std_df)

    if not plan_frames:
        raise FileNotFoundError("未找到可用计划文件")

    combined = pd.concat(plan_frames, ignore_index=True)
    combined = combined.sort_values(["code", "plan_priority"]).reset_index(drop=True)

    rows = []
    for code, g in combined.groupby("code", as_index=False):
        g = g.sort_values("plan_priority").reset_index(drop=True)

        planned_action = last_valid_text(g["planned_action"])
        planned_side = classify_side(planned_action)

        planned_shares = last_valid(pd.to_numeric(g["planned_shares"], errors="coerce"))
        positive_shares = pd.to_numeric(g["planned_shares"], errors="coerce")
        positive_shares = positive_shares[positive_shares > 0]
        if pd.isna(planned_shares) and not positive_shares.empty:
            planned_shares = positive_shares.iloc[-1]

        planned_price = last_valid(pd.to_numeric(g["planned_price"], errors="coerce"))
        positive_prices = pd.to_numeric(g["planned_price"], errors="coerce")
        positive_prices = positive_prices[positive_prices > 0]
        if pd.isna(planned_price) and not positive_prices.empty:
            planned_price = positive_prices.iloc[-1]

        execution_rank = last_valid(pd.to_numeric(g["execution_rank"], errors="coerce"))
        trade_date = last_valid(pd.to_datetime(g["trade_date"], errors="coerce"))

        rows.append(
            {
                "trade_date": trade_date,
                "code": code,
                "planned_action": planned_action if planned_action else "未计划",
                "planned_side": planned_side,
                "planned_shares": 0 if pd.isna(planned_shares) else int(round(float(planned_shares))),
                "planned_price": np.nan if pd.isna(planned_price) else float(planned_price),
                "execution_rank": np.nan if pd.isna(execution_rank) else float(execution_rank),
                "plan_source": g["plan_source"].iloc[-1],
                "plan_source_trace": " -> ".join(g["plan_source"].astype(str).tolist()),
            }
        )

    final_plan = pd.DataFrame(rows)
    final_plan = final_plan.sort_values(["execution_rank", "code"], na_position="last").reset_index(drop=True)
    return final_plan


def extract_code_from_filename(path: Path) -> Optional[str]:
    stem = path.stem.lower().replace("_", ".").replace("-", ".")
    digits_match = re.findall(r"\d{6}", stem)
    if not digits_match:
        return None

    digits = digits_match[0]

    if "sh" in stem:
        return f"sh.{digits}"
    if "sz" in stem:
        return f"sz.{digits}"

    return normalize_code(digits)


def extract_code_from_file_content(path: Path) -> Optional[str]:
    try:
        df = read_csv_auto(path)
        df.columns = [str(c).strip() for c in df.columns]
        code_col = first_existing_column(
            df, ["code", "ts_code", "symbol", "stock_code", "证券代码", "股票代码", "代码"]
        )
        if code_col is None or df.empty:
            return None

        series = df[code_col].dropna()
        if series.empty:
            return None

        code = normalize_code(series.iloc[0])
        return code if re.search(r"(sh|sz)\.\d{6}", code) else None
    except Exception:
        return None


def build_daily_file_index(stock_data_dir: Path) -> Dict[str, Path]:
    file_index: Dict[str, Path] = {}

    all_files = list(stock_data_dir.glob("*.csv"))
    for path in all_files:
        code = extract_code_from_filename(path)
        if code and code not in file_index:
            file_index[code] = path

    missing_like_files = [p for p in all_files if extract_code_from_filename(p) is None]
    for path in missing_like_files:
        code = extract_code_from_file_content(path)
        if code and code not in file_index:
            file_index[code] = path

    return file_index


def read_stock_bar(stock_file: Path, trade_date: pd.Timestamp) -> Optional[pd.Series]:
    df = read_csv_auto(stock_file)
    original_cols = [str(c).strip() for c in df.columns]
    df.columns = [c.lower() for c in original_cols]

    date_col = first_existing_column(df, ["trade_date", "date", "日期", "交易日期"])
    open_col = first_existing_column(df, ["open", "开盘", "开盘价"])
    high_col = first_existing_column(df, ["high", "最高", "最高价"])
    low_col = first_existing_column(df, ["low", "最低", "最低价"])
    close_col = first_existing_column(df, ["close", "收盘", "收盘价"])

    if date_col is None or open_col is None or high_col is None or low_col is None or close_col is None:
        return None

    temp = df.copy()
    temp["__date__"] = pd.to_datetime(temp[date_col], errors="coerce").dt.normalize()
    temp = temp[temp["__date__"] == trade_date].copy()
    if temp.empty:
        return None

    row = temp.iloc[-1]
    return pd.Series(
        {
            "open": pd.to_numeric(row[open_col], errors="coerce"),
            "high": pd.to_numeric(row[high_col], errors="coerce"),
            "low": pd.to_numeric(row[low_col], errors="coerce"),
            "close": pd.to_numeric(row[close_col], errors="coerce"),
        }
    )


def simulate_fill_for_plan_row(
    row: pd.Series,
    bar: pd.Series,
    buy_slippage_bps: float,
    sell_slippage_bps: float,
) -> dict:
    side = row["planned_side"]
    planned_price = row["planned_price"]
    planned_shares = int(row["planned_shares"])

    o = float(bar["open"])
    h = float(bar["high"])
    l = float(bar["low"])

    filled = False
    fill_price = np.nan
    fill_time = ""
    fill_logic = ""

    if planned_shares <= 0 or side not in {"buy", "sell"}:
        return {
            "filled": False,
            "fill_price": np.nan,
            "fill_time": "",
            "fill_logic": "非交易计划",
            "bar_open": o,
            "bar_high": h,
            "bar_low": l,
            "bar_close": float(bar["close"]),
        }

    if pd.isna(planned_price) or planned_price <= 0:
        if side == "buy":
            fill_price = o * (1 + buy_slippage_bps / 10000)
            fill_time = "09:35:00"
            fill_logic = "无计划价-按开盘价模拟买入"
            filled = True
        else:
            fill_price = o * (1 - sell_slippage_bps / 10000)
            fill_time = "09:35:00"
            fill_logic = "无计划价-按开盘价模拟卖出"
            filled = True
    else:
        if side == "buy":
            if o <= planned_price:
                fill_price = o * (1 + buy_slippage_bps / 10000)
                fill_time = "09:35:00"
                fill_logic = "买入-开盘可成交"
                filled = True
            elif l <= planned_price <= h:
                fill_price = planned_price * (1 + buy_slippage_bps / 10000)
                fill_time = "10:30:00"
                fill_logic = "买入-盘中触发计划价"
                filled = True
            else:
                fill_logic = "买入-全天未触发计划价"
        elif side == "sell":
            if o >= planned_price:
                fill_price = o * (1 - sell_slippage_bps / 10000)
                fill_time = "09:35:00"
                fill_logic = "卖出-开盘可成交"
                filled = True
            elif l <= planned_price <= h:
                fill_price = planned_price * (1 - sell_slippage_bps / 10000)
                fill_time = "10:30:00"
                fill_logic = "卖出-盘中触发计划价"
                filled = True
            else:
                fill_logic = "卖出-全天未触发计划价"

    if filled:
        fill_price = round(float(fill_price), 4)

    return {
        "filled": filled,
        "fill_price": fill_price,
        "fill_time": fill_time,
        "fill_logic": fill_logic,
        "bar_open": o,
        "bar_high": h,
        "bar_low": l,
        "bar_close": float(bar["close"]),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="基于日线数据生成模拟真实成交流水，仅用于第13段联调")
    parser.add_argument("--trade_date", type=str, required=True, help="交易日期，例如 2026-03-17")
    parser.add_argument(
        "--project_root",
        type=str,
        default=str(Path(__file__).resolve().parents[1]),
        help="项目根目录",
    )
    parser.add_argument(
        "--stock_data_dir",
        type=str,
        default="",
        help="日线数据目录，默认 project_root/stock_data_5years",
    )
    parser.add_argument("--plan_path_1", type=str, default="", help="计划文件1")
    parser.add_argument("--plan_path_2", type=str, default="", help="计划文件2")
    parser.add_argument("--plan_path_3", type=str, default="", help="计划文件3")
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
        help="输出生成明细路径，默认 reports/mock_fill_generation_detail.csv",
    )
    parser.add_argument("--buy_slippage_bps", type=float, default=8.0, help="买入模拟滑点 bps")
    parser.add_argument("--sell_slippage_bps", type=float, default=8.0, help="卖出模拟滑点 bps")
    parser.add_argument("--commission_rate", type=float, default=0.0003, help="模拟佣金率")
    parser.add_argument("--sell_tax_rate", type=float, default=0.0005, help="模拟卖出附加税率")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    project_root = Path(args.project_root)
    reports_dir = project_root / "reports"
    stock_data_dir = Path(args.stock_data_dir) if args.stock_data_dir else project_root / "stock_data_5years"
    output_path = Path(args.output_path) if args.output_path else reports_dir / "real_trade_fills.csv"
    detail_output_path = Path(args.detail_output_path) if args.detail_output_path else reports_dir / "mock_fill_generation_detail.csv"
    trade_date = pd.Timestamp(args.trade_date).normalize()

    if not stock_data_dir.exists():
        raise FileNotFoundError(f"未找到日线数据目录: {stock_data_dir}")

    plan_paths = []
    if args.plan_path_1:
        plan_paths.append(Path(args.plan_path_1))
    if args.plan_path_2:
        plan_paths.append(Path(args.plan_path_2))
    if args.plan_path_3:
        plan_paths.append(Path(args.plan_path_3))
    if not plan_paths:
        plan_paths = [
            reports_dir / "daily_execution_plan.csv",
            reports_dir / "daily_open_execution_decision.csv",
            reports_dir / "daily_intraday_recheck_decision.csv",
        ]

    final_plan = load_final_plan(plan_paths)
    final_plan = final_plan[
        (final_plan["planned_side"].isin(["buy", "sell"])) &
        (final_plan["planned_shares"] > 0)
    ].copy()

    if final_plan.empty:
        raise ValueError("最终计划文件中无有效交易计划，请先检查计划合并后的 planned_shares")

    file_index = build_daily_file_index(stock_data_dir)

    detail_rows = []
    fill_rows = []

    for _, row in final_plan.iterrows():
        code = row["code"]
        stock_file = file_index.get(code)

        base_info = {
            "trade_date": trade_date.strftime("%Y-%m-%d"),
            "code": code,
            "planned_action": row["planned_action"],
            "planned_side": row["planned_side"],
            "planned_shares": int(row["planned_shares"]),
            "planned_price": row["planned_price"],
            "execution_rank": row["execution_rank"],
            "plan_source": row["plan_source"],
            "plan_source_trace": row["plan_source_trace"],
            "stock_file": str(stock_file) if stock_file else "",
        }

        if stock_file is None:
            detail_rows.append(
                {
                    **base_info,
                    "status": "失败",
                    "reason": "未匹配到日线文件",
                    "bar_open": np.nan,
                    "bar_high": np.nan,
                    "bar_low": np.nan,
                    "bar_close": np.nan,
                    "fill_price": np.nan,
                    "fill_time": "",
                    "fill_logic": "",
                    "filled_shares": 0,
                    "filled_amount": 0.0,
                    "commission": 0.0,
                }
            )
            continue

        bar = read_stock_bar(stock_file, trade_date)
        if bar is None:
            detail_rows.append(
                {
                    **base_info,
                    "status": "失败",
                    "reason": "该交易日无日线数据或列名不匹配",
                    "bar_open": np.nan,
                    "bar_high": np.nan,
                    "bar_low": np.nan,
                    "bar_close": np.nan,
                    "fill_price": np.nan,
                    "fill_time": "",
                    "fill_logic": "",
                    "filled_shares": 0,
                    "filled_amount": 0.0,
                    "commission": 0.0,
                }
            )
            continue

        sim = simulate_fill_for_plan_row(
            row=row,
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
                    "bar_open": sim.get("bar_open", np.nan),
                    "bar_high": sim.get("bar_high", np.nan),
                    "bar_low": sim.get("bar_low", np.nan),
                    "bar_close": sim.get("bar_close", np.nan),
                    "fill_price": np.nan,
                    "fill_time": "",
                    "fill_logic": sim["fill_logic"],
                    "filled_shares": 0,
                    "filled_amount": 0.0,
                    "commission": 0.0,
                }
            )
            continue

        filled_shares = int(row["planned_shares"])
        filled_price = float(sim["fill_price"])
        filled_amount = round(filled_shares * filled_price, 2)

        commission = filled_amount * float(args.commission_rate)
        if row["planned_side"] == "sell":
            commission += filled_amount * float(args.sell_tax_rate)
        commission = round(commission, 2)

        order_id = f"SIMORD_{trade_date.strftime('%Y%m%d')}_{code.replace('.', '').upper()}"
        deal_id = f"SIMDL_{trade_date.strftime('%Y%m%d')}_{code.replace('.', '').upper()}"

        fill_rows.append(
            {
                "trade_date": trade_date.strftime("%Y-%m-%d"),
                "trade_time": sim["fill_time"],
                "code": code,
                "side": row["planned_side"],
                "filled_shares": filled_shares,
                "filled_price": filled_price,
                "filled_amount": filled_amount,
                "commission": commission,
                "order_id": order_id,
                "deal_id": deal_id,
                "simulated_flag": 1,
                "fill_logic": sim["fill_logic"],
            }
        )

        detail_rows.append(
            {
                **base_info,
                "status": "已生成模拟成交",
                "reason": "",
                "bar_open": sim.get("bar_open", np.nan),
                "bar_high": sim.get("bar_high", np.nan),
                "bar_low": sim.get("bar_low", np.nan),
                "bar_close": sim.get("bar_close", np.nan),
                "fill_price": filled_price,
                "fill_time": sim["fill_time"],
                "fill_logic": sim["fill_logic"],
                "filled_shares": filled_shares,
                "filled_amount": filled_amount,
                "commission": commission,
            }
        )

    detail_df = pd.DataFrame(detail_rows)

    fill_columns = [
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
        "simulated_flag",
        "fill_logic",
    ]
    fills_df = pd.DataFrame(fill_rows, columns=fill_columns)

    detail_df.to_csv(detail_output_path, index=False, encoding="utf-8-sig")
    fills_df.to_csv(output_path, index=False, encoding="utf-8-sig")

    print("=" * 60)
    print("模拟真实成交流水生成完成（仅用于第13段联调）")
    print(f"交易日期: {trade_date.strftime('%Y-%m-%d')}")
    print(f"计划交易数: {len(final_plan)}")
    print(f"模拟成交数: {len(fills_df)}")
    print(f"未成交数: {(detail_df['status'] == '未成交').sum() if not detail_df.empty else 0}")
    print(f"失败数: {(detail_df['status'] == '失败').sum() if not detail_df.empty else 0}")
    print(f"模拟成交文件: {output_path}")
    print(f"生成明细文件: {detail_output_path}")
    print("=" * 60)

    if not detail_df.empty:
        preview_cols = [
            "code",
            "planned_action",
            "planned_shares",
            "planned_price",
            "status",
            "reason",
            "stock_file",
            "fill_price",
            "fill_logic",
        ]
        print(detail_df[preview_cols].head(20).to_string(index=False))


if __name__ == "__main__":
    main()