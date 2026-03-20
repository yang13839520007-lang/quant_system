# -*- coding: utf-8 -*-
"""
Created on Thu Mar 19 10:13:51 2026

@author: DELL
"""

from __future__ import annotations

import argparse
import math
import shlex
import sys
from pathlib import Path
from typing import Any, Optional

import numpy as np
import pandas as pd


DEFAULT_MAX_EXECUTION_NAMES = 5
DEFAULT_TOTAL_CAPITAL = 1_000_000.0


def resolve_project_root(project_root: Optional[str] = None) -> Path:
    if project_root:
        root = Path(project_root).resolve()
    else:
        root = Path(__file__).resolve().parent.parent
    if root.name.lower() == "scripts":
        root = root.parent
    return root


def pick_first_column(df: pd.DataFrame, aliases: list[str]) -> Optional[str]:
    lowered = {str(col).strip().lower(): col for col in df.columns}
    for alias in aliases:
        col = lowered.get(alias.lower())
        if col is not None:
            return col
    return None


def _looks_like_date(text: str) -> bool:
    text = str(text).strip()
    if len(text) != 10:
        return False
    if text[4] != "-" or text[7] != "-":
        return False
    y, m, d = text.split("-")
    return y.isdigit() and m.isdigit() and d.isdigit()


def _infer_trading_date_from_files(root: Path) -> Optional[str]:
    candidate_files = [
        root / "reports" / "daily_portfolio_plan_risk_checked.csv",
        root / "reports" / "daily_portfolio_plan.csv",
        root / "reports" / "daily_trade_plan_all.csv",
    ]
    candidate_cols = ["trading_date", "trade_date", "date"]

    for path in candidate_files:
        if not path.exists():
            continue
        try:
            df = pd.read_csv(path, nrows=5)
        except Exception:
            continue

        col = pick_first_column(df, candidate_cols)
        if col is None or df.empty:
            continue

        values = df[col].dropna().astype(str).str.strip()
        if values.empty:
            continue

        value = values.iloc[0]
        if _looks_like_date(value):
            return value

    return None


def load_runtime_settings() -> dict[str, float]:
    settings = {
        "total_capital": DEFAULT_TOTAL_CAPITAL,
        "max_execution_names": float(DEFAULT_MAX_EXECUTION_NAMES),
    }
    try:
        from config import settings as cfg  # type: ignore

        mapping = [
            ("TOTAL_CAPITAL", "total_capital"),
            ("PORTFOLIO_TOTAL_CAPITAL", "total_capital"),
            ("ACCOUNT_TOTAL_CAPITAL", "total_capital"),
            ("MAX_EXECUTION_NAMES", "max_execution_names"),
            ("PORTFOLIO_MAX_NAMES", "max_execution_names"),
            ("MAX_PORTFOLIO_NAMES", "max_execution_names"),
        ]
        for attr_name, target_key in mapping:
            if hasattr(cfg, attr_name):
                value = getattr(cfg, attr_name)
                if value is not None:
                    settings[target_key] = float(value)
    except Exception:
        pass
    return settings


def load_risk_checked_plan(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"未找到风控复核文件: {path}")
    return pd.read_csv(path)


def normalize_risk_checked_input(raw: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame(index=raw.index)

    mapping = {
        "trading_date": ["trading_date", "trade_date", "date"],
        "portfolio_rank": ["portfolio_rank", "rank"],
        "code": ["code", "ts_code", "symbol"],
        "name": ["name", "stock_name", "sec_name", "股票名称"],
        "score": ["score", "final_score"],
        "entry_price": ["entry_price", "close_price", "close", "latest"],
        "stop_loss": ["stop_loss"],
        "target_price": ["target_price"],
        "suggested_shares": ["suggested_shares", "shares"],
        "suggested_position_pct": ["suggested_position_pct", "position_pct"],
        "expected_loss_amt": ["expected_loss_amt"],
        "expected_profit_amt": ["expected_profit_amt"],
        "risk_review_passed": ["risk_review_passed"],
        "risk_review_note": ["risk_review_note"],
        "turnover_amount": ["turnover_amount", "amount"],
        "turnover_rate": ["turnover_rate", "turn_rate"],
        "heat_level": ["heat_level"],
        "action": ["action"],
    }

    for target_col, aliases in mapping.items():
        src_col = pick_first_column(raw, aliases)
        if src_col is not None:
            out[target_col] = raw[src_col]
        else:
            out[target_col] = pd.NA

    for col in [
        "portfolio_rank",
        "score",
        "entry_price",
        "stop_loss",
        "target_price",
        "suggested_shares",
        "suggested_position_pct",
        "expected_loss_amt",
        "expected_profit_amt",
        "turnover_amount",
        "turnover_rate",
    ]:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    out["code"] = out["code"].fillna("").astype(str)
    out["name"] = out["name"].fillna("").astype(str)
    out["heat_level"] = out["heat_level"].fillna("").astype(str)
    out["action"] = out["action"].fillna("").astype(str)
    out["risk_review_note"] = out["risk_review_note"].fillna("").astype(str)

    # 风控通过列兼容
    rr = out["risk_review_passed"]
    if rr.dtype == bool:
        out["risk_review_passed"] = rr
    else:
        out["risk_review_passed"] = (
            rr.astype(str)
            .str.strip()
            .str.lower()
            .isin(["1", "true", "yes", "y", "通过"])
        )

    return out


def normalize_position_pct(series: pd.Series) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce").fillna(0.0)
    s = np.where(s > 1.0, s / 100.0, s)
    s = pd.Series(s, index=series.index, dtype="float64")
    return s.clip(lower=0.0, upper=1.0)


def safe_div(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    num = pd.to_numeric(numerator, errors="coerce").fillna(0.0)
    den = pd.to_numeric(denominator, errors="coerce").replace(0, np.nan)
    out = num / den
    return out.replace([np.inf, -np.inf], np.nan).fillna(0.0)


def log_scale_score(series: pd.Series, floor_log: float = 7.0, ceil_log: float = 10.0) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce").fillna(0.0).clip(lower=1.0)
    x = np.log10(s)
    out = (x - floor_log) / max(1e-9, (ceil_log - floor_log)) * 100.0
    return pd.Series(out, index=series.index).clip(lower=0.0, upper=100.0)


def build_execution_priority(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()

    work["suggested_position_pct"] = normalize_position_pct(work["suggested_position_pct"])
    work["reward_risk_ratio"] = safe_div(work["expected_profit_amt"], work["expected_loss_amt"]).clip(upper=5.0)
    work["liquidity_score"] = (
        log_scale_score(work["turnover_amount"]).fillna(40.0) * 0.7
        + (pd.to_numeric(work["turnover_rate"], errors="coerce").fillna(2.0).clip(0, 20) / 20.0 * 100.0) * 0.3
    ).clip(0, 100)

    work["rank_boost"] = (
        (1.0 - (pd.to_numeric(work["portfolio_rank"], errors="coerce").fillna(99) - 1.0) / 10.0) * 100.0
    ).clip(0, 100)

    action_boost = np.select(
        [
            work["action"].astype(str).str.contains("优先", na=False),
            work["action"].astype(str).str.contains("正常", na=False),
        ],
        [100.0, 75.0],
        default=50.0,
    )

    heat_boost = np.select(
        [
            work["heat_level"].astype(str).str.contains("高热", na=False),
            work["heat_level"].astype(str).str.contains("正常", na=False),
        ],
        [100.0, 75.0],
        default=50.0,
    )

    rr_score = (work["reward_risk_ratio"] / 3.0 * 100.0).clip(0, 100)
    position_score = (work["suggested_position_pct"] / 0.10 * 100.0).clip(0, 100)

    work["execution_priority_score"] = (
        work["score"].fillna(0) * 0.40
        + work["liquidity_score"] * 0.20
        + rr_score * 0.15
        + work["rank_boost"] * 0.10
        + action_boost * 0.10
        + heat_boost * 0.05
    ).round(2)

    work["priority_tier"] = np.select(
        [
            work["execution_priority_score"] >= 75,
            work["execution_priority_score"] >= 60,
        ],
        ["P1", "P2"],
        default="P3",
    )

    work["execution_action"] = "BUY"
    work["order_side"] = "BUY"

    work["price_offset_pct"] = np.select(
        [
            work["priority_tier"] == "P1",
            work["priority_tier"] == "P2",
        ],
        [0.003, 0.005],
        default=0.008,
    )
    work["max_chase_pct"] = np.select(
        [
            work["priority_tier"] == "P1",
            work["priority_tier"] == "P2",
        ],
        [0.008, 0.012],
        default=0.018,
    )
    work["cancel_after_minutes"] = np.select(
        [
            work["priority_tier"] == "P1",
            work["priority_tier"] == "P2",
        ],
        [12, 20],
        default=30,
    )

    work["order_type"] = "LIMIT"
    work["limit_price"] = (work["entry_price"] * (1.0 + work["price_offset_pct"])).round(2)
    work["planned_order_value"] = (work["suggested_shares"] * work["entry_price"]).round(2)
    work["execution_window"] = np.select(
        [
            work["priority_tier"] == "P1",
            work["priority_tier"] == "P2",
        ],
        ["09:30-09:45", "09:30-10:00"],
        default="09:30-10:30",
    )

    work["execution_note"] = (
        "priority="
        + work["priority_tier"].astype(str)
        + "; rr="
        + work["reward_risk_ratio"].round(2).astype(str)
        + "; liquidity="
        + work["liquidity_score"].round(1).astype(str)
    )

    return work


def split_execution_and_keep(
    df: pd.DataFrame,
    max_execution_names: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    work = df.copy()

    invalid_mask = (
        (work["code"].str.len() == 0)
        | (work["entry_price"].fillna(0) <= 0)
        | (work["suggested_shares"].fillna(0) <= 0)
        | (~work["risk_review_passed"].fillna(False))
    )

    invalid_df = work.loc[invalid_mask].copy()
    valid_df = work.loc[~invalid_mask].copy()

    if not invalid_df.empty:
        invalid_df["keep_reason"] = np.select(
            [
                ~invalid_df["risk_review_passed"].fillna(False),
                invalid_df["code"].str.len() == 0,
                invalid_df["entry_price"].fillna(0) <= 0,
                invalid_df["suggested_shares"].fillna(0) <= 0,
            ],
            [
                "风控未通过",
                "缺少代码",
                "缺少有效价格",
                "建议股数<=0",
            ],
            default="基础字段异常",
        )

    valid_df = valid_df.sort_values(
        ["execution_priority_score", "score", "planned_order_value"],
        ascending=[False, False, False],
    ).reset_index(drop=True)

    exec_df = valid_df.head(int(max_execution_names)).copy()
    keep_overflow_df = valid_df.iloc[int(max_execution_names):].copy()
    if not keep_overflow_df.empty:
        keep_overflow_df["keep_reason"] = "执行名额外保留"

    keep_df = pd.concat([invalid_df, keep_overflow_df], ignore_index=True, sort=False)

    if not exec_df.empty:
        exec_df.insert(0, "execution_rank", range(1, len(exec_df) + 1))

    return exec_df, keep_df


def write_outputs(root: Path, exec_df: pd.DataFrame, keep_df: pd.DataFrame, summary: dict[str, Any]) -> None:
    reports_dir = root / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    exec_cols = [
        "execution_rank",
        "trading_date",
        "portfolio_rank",
        "code",
        "name",
        "execution_action",
        "order_side",
        "priority_tier",
        "execution_priority_score",
        "score",
        "entry_price",
        "limit_price",
        "stop_loss",
        "target_price",
        "suggested_shares",
        "suggested_position_pct",
        "planned_order_value",
        "expected_loss_amt",
        "expected_profit_amt",
        "reward_risk_ratio",
        "order_type",
        "price_offset_pct",
        "max_chase_pct",
        "cancel_after_minutes",
        "execution_window",
        "risk_review_note",
        "execution_note",
    ]
    keep_cols = [
        "trading_date",
        "portfolio_rank",
        "code",
        "name",
        "score",
        "entry_price",
        "suggested_shares",
        "suggested_position_pct",
        "risk_review_passed",
        "risk_review_note",
        "keep_reason",
    ]

    for col in exec_cols:
        if col not in exec_df.columns:
            exec_df[col] = pd.NA
    for col in keep_cols:
        if col not in keep_df.columns:
            keep_df[col] = pd.NA

    plan_path = reports_dir / "daily_execution_plan.csv"
    keep_path = reports_dir / "daily_execution_plan_keep.csv"
    summary_path = reports_dir / "daily_execution_plan_summary.txt"

    exec_df[exec_cols].to_csv(plan_path, index=False, encoding="utf-8-sig")
    keep_df[keep_cols].to_csv(keep_path, index=False, encoding="utf-8-sig")

    lines = [
        "组合执行优先级生成完成",
        f"执行标的数: {summary['execution_count']}",
        f"保留标的数: {summary['keep_count']}",
        f"总资金: {summary['capital']:.2f}",
        f"执行计划总金额: {summary['execution_total_value']:.2f}",
        f"执行计划总仓位: {summary['execution_total_position_pct']:.6f}",
        f"执行计划总预期亏损: {summary['expected_total_loss_amt']:.2f}",
        f"执行计划总预期盈利: {summary['expected_total_profit_amt']:.2f}",
        f"平均优先级分数: {summary['avg_execution_priority_score']:.4f}",
        f"P1数量: {summary['p1_count']}",
        f"P2数量: {summary['p2_count']}",
        f"P3数量: {summary['p3_count']}",
    ]
    summary_path.write_text("\n".join(lines), encoding="utf-8")


def build_summary(exec_df: pd.DataFrame, keep_df: pd.DataFrame, total_capital: float) -> dict[str, Any]:
    return {
        "execution_count": int(len(exec_df)),
        "keep_count": int(len(keep_df)),
        "capital": float(total_capital),
        "execution_total_value": round(float(exec_df["planned_order_value"].sum()) if not exec_df.empty else 0.0, 2),
        "execution_total_position_pct": round(
            float(exec_df["planned_order_value"].sum()) / float(total_capital) if not exec_df.empty else 0.0,
            6,
        ),
        "expected_total_loss_amt": round(float(exec_df["expected_loss_amt"].sum()) if not exec_df.empty else 0.0, 2),
        "expected_total_profit_amt": round(float(exec_df["expected_profit_amt"].sum()) if not exec_df.empty else 0.0, 2),
        "avg_execution_priority_score": round(
            float(exec_df["execution_priority_score"].mean()) if not exec_df.empty else 0.0,
            4,
        ),
        "p1_count": int((exec_df["priority_tier"] == "P1").sum()) if not exec_df.empty else 0,
        "p2_count": int((exec_df["priority_tier"] == "P2").sum()) if not exec_df.empty else 0,
        "p3_count": int((exec_df["priority_tier"] == "P3").sum()) if not exec_df.empty else 0,
    }


def _extract_from_mapping(mapping: dict[str, Any]) -> dict[str, Any]:
    return {
        "trading_date": mapping.get("trading_date")
        or mapping.get("trade_date")
        or mapping.get("target_trading_date"),
        "project_root": mapping.get("project_root")
        or mapping.get("base_dir")
        or mapping.get("root_dir")
        or mapping.get("root_path")
        or mapping.get("base_path"),
        "max_execution_names": mapping.get("max_execution_names")
        or mapping.get("max_names")
        or mapping.get("portfolio_max_names"),
        "total_capital": mapping.get("total_capital") or mapping.get("capital"),
    }


def _resolve_runtime_call(*args: Any, **kwargs: Any) -> tuple[Optional[list[str]], dict[str, Any]]:
    runtime = {
        "trading_date": kwargs.get("trading_date"),
        "project_root": kwargs.get("project_root") or kwargs.get("base_dir"),
        "max_execution_names": kwargs.get("max_execution_names") or kwargs.get("max_names"),
        "total_capital": kwargs.get("total_capital"),
    }

    argv: Optional[list[str]] = kwargs.get("argv")

    for item in args:
        if isinstance(item, dict):
            runtime_from_dict = _extract_from_mapping(item)
            for k, v in runtime_from_dict.items():
                if runtime.get(k) in (None, "") and v not in (None, ""):
                    runtime[k] = v

        elif isinstance(item, (list, tuple)):
            if argv in (None, [], ()):
                argv = list(item)

        elif isinstance(item, str):
            if _looks_like_date(item) and runtime["trading_date"] in (None, ""):
                runtime["trading_date"] = item
            elif Path(item).drive and runtime["project_root"] in (None, ""):
                runtime["project_root"] = item
            elif ("--" in item or " -" in item) and argv in (None, [], ()):
                argv = shlex.split(item)

        else:
            obj_dict = getattr(item, "__dict__", None)
            if isinstance(obj_dict, dict):
                runtime_from_obj = _extract_from_mapping(obj_dict)
                for k, v in runtime_from_obj.items():
                    if runtime.get(k) in (None, "") and v not in (None, ""):
                        runtime[k] = v

    return argv, runtime


def _run_build(
    trading_date: str,
    project_root: Optional[str] = None,
    max_execution_names: Optional[int] = None,
    total_capital: Optional[float] = None,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    root = resolve_project_root(project_root)
    reports_dir = root / "reports"

    print("=" * 60)
    print("组合执行优先级开始生成")
    print(f"目标交易日  : {trading_date}")
    print(f"风控文件    : {reports_dir / 'daily_portfolio_plan_risk_checked.csv'}")
    print(f"输出目录    : {reports_dir}")
    print("入口类型    : function")
    print("调用入口    : core.portfolio_executor.build_execution_plan")
    print("=" * 60)

    raw_df = load_risk_checked_plan(reports_dir / "daily_portfolio_plan_risk_checked.csv")
    df = normalize_risk_checked_input(raw_df)

    if "trading_date" not in df.columns or df["trading_date"].isna().all():
        df["trading_date"] = trading_date
    else:
        df["trading_date"] = df["trading_date"].fillna(trading_date)

    settings = load_runtime_settings()
    total_capital = float(settings["total_capital"] if total_capital in (None, "") else total_capital)
    max_execution_names = int(
        settings["max_execution_names"] if max_execution_names in (None, "") else max_execution_names
    )

    df["planned_order_value"] = (pd.to_numeric(df["suggested_shares"], errors="coerce").fillna(0.0)
                                 * pd.to_numeric(df["entry_price"], errors="coerce").fillna(0.0)).round(2)

    priority_df = build_execution_priority(df)
    exec_df, keep_df = split_execution_and_keep(priority_df, max_execution_names=max_execution_names)

    if exec_df.empty:
        raise RuntimeError("组合执行优先级生成失败：无可执行标的，请检查风控复核结果。")

    summary = build_summary(exec_df=exec_df, keep_df=keep_df, total_capital=total_capital)
    write_outputs(root=root, exec_df=exec_df, keep_df=keep_df, summary=summary)

    display_cols = [
        "execution_rank",
        "code",
        "name",
        "priority_tier",
        "execution_priority_score",
        "entry_price",
        "limit_price",
        "suggested_shares",
        "suggested_position_pct",
        "expected_loss_amt",
        "expected_profit_amt",
    ]
    printable_df = exec_df[[c for c in display_cols if c in exec_df.columns]].copy()

    print("=" * 60)
    print("组合执行优先级生成完成")
    print(f"执行标的数: {len(exec_df)}")
    print(f"保留标的数: {len(keep_df)}")
    print(f"执行计划文件: {reports_dir / 'daily_execution_plan.csv'}")
    print(f"保留文件    : {reports_dir / 'daily_execution_plan_keep.csv'}")
    print(f"摘要文件    : {reports_dir / 'daily_execution_plan_summary.txt'}")
    print("=" * 60)
    print((printable_df, summary))

    return exec_df, keep_df, summary


def build_execution_plan(*args: Any, **kwargs: Any) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    argv, runtime = _resolve_runtime_call(*args, **kwargs)

    root = resolve_project_root(None if runtime["project_root"] in (None, "") else str(runtime["project_root"]))
    if runtime["trading_date"] in (None, ""):
        inferred = _infer_trading_date_from_files(root)
        if inferred:
            runtime["trading_date"] = inferred

    if runtime["trading_date"] not in (None, ""):
        return _run_build(
            trading_date=str(runtime["trading_date"]),
            project_root=str(root),
            max_execution_names=None
            if runtime["max_execution_names"] in (None, "")
            else int(runtime["max_execution_names"]),
            total_capital=None if runtime["total_capital"] in (None, "") else float(runtime["total_capital"]),
        )

    parser = argparse.ArgumentParser(description="生成组合执行优先级")
    parser.add_argument("--trading-date", required=False, help="目标交易日")
    parser.add_argument("--project-root", default=None, help="项目根目录")
    parser.add_argument("--max-execution-names", type=int, default=None, help="最大执行标的数")
    parser.add_argument("--total-capital", type=float, default=None, help="总资金")

    if argv in (None, [], ()):
        argv = sys.argv[1:]

    parsed_args, _unknown = parser.parse_known_args(argv)
    trading_date = parsed_args.trading_date or _infer_trading_date_from_files(resolve_project_root(parsed_args.project_root))
    if not trading_date:
        parser.error("the following arguments are required: --trading-date")

    return _run_build(
        trading_date=trading_date,
        project_root=parsed_args.project_root,
        max_execution_names=parsed_args.max_execution_names,
        total_capital=parsed_args.total_capital,
    )


def main(*args: Any, **kwargs: Any) -> int:
    build_execution_plan(*args, **kwargs)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())