# -*- coding: utf-8 -*-
"""
Created on Thu Mar 19 10:05:14 2026

@author: DELL
"""

from __future__ import annotations

import argparse
import shlex
import sys
from pathlib import Path
from typing import Any, Optional

import numpy as np
import pandas as pd


DEFAULT_TOTAL_CAPITAL = 1_000_000.0
DEFAULT_MAX_NAMES = 5
DEFAULT_TARGET_TOTAL_POSITION_PCT = 0.40


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


def load_runtime_settings() -> dict[str, float]:
    settings = {
        "total_capital": DEFAULT_TOTAL_CAPITAL,
        "max_names": float(DEFAULT_MAX_NAMES),
        "target_total_position_pct": DEFAULT_TARGET_TOTAL_POSITION_PCT,
    }
    try:
        from config import settings as cfg  # type: ignore

        mapping = [
            ("TOTAL_CAPITAL", "total_capital"),
            ("PORTFOLIO_TOTAL_CAPITAL", "total_capital"),
            ("ACCOUNT_TOTAL_CAPITAL", "total_capital"),
            ("PORTFOLIO_MAX_NAMES", "max_names"),
            ("MAX_PORTFOLIO_NAMES", "max_names"),
            ("TARGET_TOTAL_POSITION_PCT", "target_total_position_pct"),
            ("PORTFOLIO_TARGET_TOTAL_POSITION_PCT", "target_total_position_pct"),
        ]
        for attr_name, target_key in mapping:
            if hasattr(cfg, attr_name):
                value = getattr(cfg, attr_name)
                if value is not None:
                    settings[target_key] = float(value)
    except Exception:
        pass
    return settings


def load_trade_plan(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"未找到交易计划文件: {path}")
    return pd.read_csv(path)


def normalize_trade_plan_input(raw: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame(index=raw.index)

    required_map = {
        "trade_date": ["trade_date", "trading_date", "date"],
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
        "action": ["action"],
        "heat_level": ["heat_level"],
        "turnover_amount": ["turnover_amount", "amount"],
        "turnover_rate": ["turnover_rate", "turn_rate"],
        "backtest_score": ["backtest_score"],
        "win_rate": ["win_rate", "winrate"],
    }

    for target_col, aliases in required_map.items():
        src_col = pick_first_column(raw, aliases)
        if src_col is not None:
            out[target_col] = raw[src_col]
        else:
            out[target_col] = pd.NA

    for col in [
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
        "backtest_score",
        "win_rate",
    ]:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    out["name"] = out["name"].fillna("").astype(str)
    out["code"] = out["code"].fillna("").astype(str)
    out["action"] = out["action"].fillna("").astype(str)
    out["heat_level"] = out["heat_level"].fillna("").astype(str)
    return out


def normalize_position_pct(series: pd.Series) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce").fillna(0.0)
    s = np.where(s > 1.0, s / 100.0, s)
    s = pd.Series(s, index=series.index, dtype="float64")
    return s.clip(lower=0.0, upper=1.0)


def board_lot_shares(price: float, capital: float) -> int:
    if price <= 0 or capital <= 0:
        return 0
    raw = int(capital // price)
    return max(0, (raw // 100) * 100)


def prepare_plan_frame(df: pd.DataFrame, total_capital: float) -> pd.DataFrame:
    work = df.copy()

    work = work.loc[
        (work["code"].str.len() > 0)
        & (work["entry_price"].fillna(0) > 0)
        & (work["score"].fillna(0) > 0)
    ].copy()

    if work.empty:
        raise RuntimeError("组合计划生成失败：交易计划为空或缺少有效 price/score/code 字段。")

    work["suggested_position_pct"] = normalize_position_pct(work["suggested_position_pct"])

    need_fill_position = work["suggested_position_pct"].fillna(0).le(0)
    if need_fill_position.any():
        score_strength = (work["score"].clip(lower=60, upper=100) - 60) / 40.0
        fallback_pct = (0.04 + score_strength * 0.06).clip(lower=0.04, upper=0.10)
        work.loc[need_fill_position, "suggested_position_pct"] = fallback_pct.loc[need_fill_position]

    need_fill_shares = work["suggested_shares"].fillna(0).le(0)
    if need_fill_shares.any():
        capital_per_name = total_capital * work["suggested_position_pct"]
        work.loc[need_fill_shares, "suggested_shares"] = [
            board_lot_shares(float(price), float(capital))
            for price, capital in zip(work.loc[need_fill_shares, "entry_price"], capital_per_name.loc[need_fill_shares])
        ]

    work["suggested_shares"] = pd.to_numeric(work["suggested_shares"], errors="coerce").fillna(0).astype(int)
    work = work.loc[work["suggested_shares"] > 0].copy()

    if work.empty:
        raise RuntimeError("组合计划生成失败：交易计划经手数归整后为空。")

    need_fill_stop = work["stop_loss"].isna() | (work["stop_loss"] <= 0) | (work["stop_loss"] >= work["entry_price"])
    if need_fill_stop.any():
        work.loc[need_fill_stop, "stop_loss"] = (work.loc[need_fill_stop, "entry_price"] * 0.94).round(2)

    need_fill_target = work["target_price"].isna() | (work["target_price"] <= work["entry_price"])
    if need_fill_target.any():
        risk_per_share = (work.loc[need_fill_target, "entry_price"] - work.loc[need_fill_target, "stop_loss"]).clip(
            lower=work.loc[need_fill_target, "entry_price"] * 0.02
        )
        work.loc[need_fill_target, "target_price"] = (
            work.loc[need_fill_target, "entry_price"] + risk_per_share * 2.0
        ).round(2)

    need_fill_loss = work["expected_loss_amt"].isna() | (work["expected_loss_amt"] < 0)
    if need_fill_loss.any():
        work.loc[need_fill_loss, "expected_loss_amt"] = (
            work.loc[need_fill_loss, "suggested_shares"]
            * (work.loc[need_fill_loss, "entry_price"] - work.loc[need_fill_loss, "stop_loss"])
        ).round(2)

    need_fill_profit = work["expected_profit_amt"].isna() | (work["expected_profit_amt"] < 0)
    if need_fill_profit.any():
        work.loc[need_fill_profit, "expected_profit_amt"] = (
            work.loc[need_fill_profit, "suggested_shares"]
            * (work.loc[need_fill_profit, "target_price"] - work.loc[need_fill_profit, "entry_price"])
        ).round(2)

    work["planned_position_amt"] = (work["suggested_shares"] * work["entry_price"]).round(2)
    work["suggested_position_pct"] = (work["planned_position_amt"] / float(total_capital)).round(4)

    work = work.sort_values(
        ["score", "expected_profit_amt", "turnover_amount", "win_rate"],
        ascending=[False, False, False, False],
    ).reset_index(drop=True)

    return work


def select_portfolio(
    prepared_df: pd.DataFrame,
    total_capital: float,
    max_names: int,
    target_total_position_pct: float,
) -> pd.DataFrame:
    target_total_position_pct = float(target_total_position_pct)
    max_names = int(max_names)

    selected_rows = []
    current_amt = 0.0

    for _, row in prepared_df.iterrows():
        if len(selected_rows) >= max_names:
            break

        row_amt = float(row["planned_position_amt"])
        tentative_total_pct = (current_amt + row_amt) / total_capital

        if tentative_total_pct > target_total_position_pct:
            remain_amt = max(0.0, target_total_position_pct * total_capital - current_amt)
            scaled_shares = board_lot_shares(float(row["entry_price"]), remain_amt)

            if scaled_shares <= 0:
                continue

            row = row.copy()
            row["suggested_shares"] = scaled_shares
            row["planned_position_amt"] = round(scaled_shares * float(row["entry_price"]), 2)
            row["suggested_position_pct"] = round(row["planned_position_amt"] / total_capital, 4)
            row["expected_loss_amt"] = round(
                scaled_shares * (float(row["entry_price"]) - float(row["stop_loss"])),
                2,
            )
            row["expected_profit_amt"] = round(
                scaled_shares * (float(row["target_price"]) - float(row["entry_price"])),
                2,
            )

        if float(row["suggested_shares"]) <= 0:
            continue

        selected_rows.append(row)
        current_amt += float(row["planned_position_amt"])

        if current_amt / total_capital >= target_total_position_pct:
            break

    if not selected_rows:
        raise RuntimeError("组合计划生成失败：在总仓位约束下未选出有效组合。")

    out = pd.DataFrame(selected_rows).reset_index(drop=True)
    out.insert(0, "portfolio_rank", range(1, len(out) + 1))
    return out


def build_summary(selected_df: pd.DataFrame, total_capital: float, target_total_position_pct: float) -> dict[str, Any]:
    actual_total_position_pct = round(float(selected_df["planned_position_amt"].sum()) / float(total_capital), 6)

    return {
        "selected_count": int(len(selected_df)),
        "capital": float(total_capital),
        "target_total_position_pct": round(float(target_total_position_pct), 4),
        "actual_total_position_pct": actual_total_position_pct,
        "cash_pct": round(max(0.0, 1.0 - actual_total_position_pct), 6),
        "expected_total_loss_amt": round(float(selected_df["expected_loss_amt"].sum()), 2),
        "expected_total_profit_amt": round(float(selected_df["expected_profit_amt"].sum()), 2),
        "avg_portfolio_score": round(float(selected_df["score"].mean()), 6),
    }


def write_outputs(root: Path, selected_df: pd.DataFrame, summary: dict[str, Any]) -> None:
    reports_dir = root / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    keep_cols = [
        "portfolio_rank",
        "trade_date",
        "code",
        "name",
        "action",
        "heat_level",
        "score",
        "entry_price",
        "stop_loss",
        "target_price",
        "suggested_shares",
        "suggested_position_pct",
        "expected_loss_amt",
        "expected_profit_amt",
        "planned_position_amt",
        "turnover_amount",
        "turnover_rate",
        "backtest_score",
        "win_rate",
    ]

    for col in keep_cols:
        if col not in selected_df.columns:
            selected_df[col] = pd.NA

    plan_path = reports_dir / "daily_portfolio_plan.csv"
    top5_path = reports_dir / "daily_portfolio_plan_top5.csv"
    summary_path = reports_dir / "daily_portfolio_summary.txt"

    selected_df[keep_cols].to_csv(plan_path, index=False, encoding="utf-8-sig")
    selected_df[keep_cols].head(5).to_csv(top5_path, index=False, encoding="utf-8-sig")

    lines = [
        "组合计划生成完成",
        f"组合标的数: {summary['selected_count']}",
        f"总资金: {summary['capital']:.2f}",
        f"目标总仓位: {summary['target_total_position_pct']:.4f}",
        f"实际总仓位: {summary['actual_total_position_pct']:.6f}",
        f"现金占比: {summary['cash_pct']:.6f}",
        f"预期总亏损: {summary['expected_total_loss_amt']:.2f}",
        f"预期总盈利: {summary['expected_total_profit_amt']:.2f}",
        f"平均组合分数: {summary['avg_portfolio_score']:.6f}",
    ]
    summary_path.write_text("\n".join(lines), encoding="utf-8")


def _looks_like_date(text: str) -> bool:
    text = str(text).strip()
    if len(text) != 10:
        return False
    if text[4] != "-" or text[7] != "-":
        return False
    y, m, d = text.split("-")
    return y.isdigit() and m.isdigit() and d.isdigit()


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
        "max_names": mapping.get("max_names") or mapping.get("portfolio_max_names"),
        "target_total_position_pct": mapping.get("target_total_position_pct"),
        "total_capital": mapping.get("total_capital") or mapping.get("capital"),
    }


def _infer_trading_date_from_files(root: Path) -> Optional[str]:
    candidate_files = [
        root / "reports" / "daily_trade_plan_all.csv",
        root / "reports" / "daily_candidates_all.csv",
        root / "reports" / "market_signal_snapshot.csv",
    ]
    candidate_cols = ["trade_date", "trading_date", "date"]

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

        series = df[col].dropna().astype(str).str.strip()
        if series.empty:
            continue

        value = series.iloc[0]
        if _looks_like_date(value):
            return value

    return None


def _resolve_runtime_call(*args: Any, **kwargs: Any) -> tuple[Optional[list[str]], dict[str, Any]]:
    runtime = {
        "trading_date": kwargs.get("trading_date"),
        "project_root": kwargs.get("project_root") or kwargs.get("base_dir"),
        "max_names": kwargs.get("max_names"),
        "target_total_position_pct": kwargs.get("target_total_position_pct"),
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
    max_names: Optional[int] = None,
    target_total_position_pct: Optional[float] = None,
    total_capital: Optional[float] = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    root = resolve_project_root(project_root)
    reports_dir = root / "reports"

    print("=" * 60)
    print("组合计划开始生成")
    print(f"目标交易日  : {trading_date}")
    print(f"交易计划文件: {reports_dir / 'daily_trade_plan_all.csv'}")
    print(f"输出目录    : {reports_dir}")
    print("入口类型    : function")
    print("调用入口    : core.portfolio_builder.build_portfolio_plan")
    print("=" * 60)

    raw_plan = load_trade_plan(reports_dir / "daily_trade_plan_all.csv")
    plan_df = normalize_trade_plan_input(raw_plan)

    if "trade_date" not in plan_df.columns or plan_df["trade_date"].isna().all():
        plan_df["trade_date"] = trading_date
    else:
        plan_df["trade_date"] = plan_df["trade_date"].fillna(trading_date)

    runtime_settings = load_runtime_settings()
    total_capital = float(runtime_settings["total_capital"] if total_capital in (None, "") else total_capital)
    max_names = int(runtime_settings["max_names"] if max_names in (None, "") else max_names)
    target_total_position_pct = float(
        runtime_settings["target_total_position_pct"]
        if target_total_position_pct in (None, "")
        else target_total_position_pct
    )

    prepared_df = prepare_plan_frame(plan_df, total_capital=total_capital)
    selected_df = select_portfolio(
        prepared_df=prepared_df,
        total_capital=total_capital,
        max_names=max_names,
        target_total_position_pct=target_total_position_pct,
    )
    summary = build_summary(
        selected_df=selected_df,
        total_capital=total_capital,
        target_total_position_pct=target_total_position_pct,
    )

    write_outputs(root=root, selected_df=selected_df, summary=summary)

    display_cols = [
        "portfolio_rank",
        "code",
        "name",
        "score",
        "entry_price",
        "stop_loss",
        "target_price",
        "suggested_shares",
        "suggested_position_pct",
        "expected_loss_amt",
        "expected_profit_amt",
    ]
    printable_df = selected_df[[c for c in display_cols if c in selected_df.columns]].copy()

    print((printable_df, summary))
    return selected_df, summary


def build_portfolio_plan(*args: Any, **kwargs: Any) -> tuple[pd.DataFrame, dict[str, Any]]:
    argv, runtime = _resolve_runtime_call(*args, **kwargs)

    root = resolve_project_root(None if runtime["project_root"] in (None, "") else str(runtime["project_root"]))

    if runtime["trading_date"] in (None, ""):
        inferred_date = _infer_trading_date_from_files(root)
        if inferred_date:
            runtime["trading_date"] = inferred_date

    if runtime["trading_date"] not in (None, ""):
        return _run_build(
            trading_date=str(runtime["trading_date"]),
            project_root=str(root),
            max_names=None if runtime["max_names"] in (None, "") else int(runtime["max_names"]),
            target_total_position_pct=None
            if runtime["target_total_position_pct"] in (None, "")
            else float(runtime["target_total_position_pct"]),
            total_capital=None if runtime["total_capital"] in (None, "") else float(runtime["total_capital"]),
        )

    parser = argparse.ArgumentParser(description="生成组合计划")
    parser.add_argument("--trading-date", required=False, help="目标交易日")
    parser.add_argument("--project-root", default=None, help="项目根目录")
    parser.add_argument("--max-names", type=int, default=None, help="最大入选数量")
    parser.add_argument("--target-total-position-pct", type=float, default=None, help="目标总仓位")
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
        max_names=parsed_args.max_names,
        target_total_position_pct=parsed_args.target_total_position_pct,
        total_capital=parsed_args.total_capital,
    )


def main(*args: Any, **kwargs: Any) -> int:
    build_portfolio_plan(*args, **kwargs)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())