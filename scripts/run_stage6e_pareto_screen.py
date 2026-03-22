# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import pandas as pd


SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR = SCRIPT_DIR.parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import run_stage6d_capacity_analysis as stage6d  # noqa: E402
import run_stage6c_account_replay as stage6c  # noqa: E402


DEFAULT_BASE_DIR = ROOT_DIR
CONCURRENT_OPTIONS = [8, 10, 12]
NEW_PER_DAY_OPTIONS = [2, 3, 5]
BUY_PRIORITY_OPTIONS = [
    "rps50_desc_ma20_bias_asc_score_desc",
    "score_desc_rps50_desc_ma20_bias_asc",
    "rps50_desc_score_desc_ma20_bias_asc",
]


def _safe_float(value: Any, default: float = 0.0) -> float:
    return stage6d._safe_float(value, default=default)


def _sort_events(events: list[dict[str, Any]], ordering: str) -> list[dict[str, Any]]:
    if ordering == "score_desc_rps50_desc_ma20_bias_asc":
        key_fn = lambda item: (-float(item["score"]), -float(item["rps50"]), float(item["ma20_bias"]), item["code"])
    elif ordering == "rps50_desc_score_desc_ma20_bias_asc":
        key_fn = lambda item: (-float(item["rps50"]), -float(item["score"]), float(item["ma20_bias"]), item["code"])
    else:
        key_fn = lambda item: (-float(item["rps50"]), float(item["ma20_bias"]), -float(item["score"]), item["code"])
    return sorted(events, key=key_fn)


def _build_experiments() -> list[stage6d.CapacityExperiment]:
    experiments: list[stage6d.CapacityExperiment] = []
    for max_concurrent in CONCURRENT_OPTIONS:
        for max_new in NEW_PER_DAY_OPTIONS:
            for ordering in BUY_PRIORITY_OPTIONS:
                experiment_id = f"PARETO_C{max_concurrent}_N{max_new}_{ordering}"
                description = f"concurrent={max_concurrent}, new_per_day={max_new}, ordering={ordering}"
                experiments.append(
                    stage6d.CapacityExperiment(
                        experiment_id=experiment_id,
                        experiment_group="Pareto",
                        description=description,
                        max_new_positions_per_day=max_new,
                        max_concurrent_positions=max_concurrent,
                        buy_priority=ordering,
                        settlement_mode="NEXT_DAY_AVAILABLE",
                    )
                )
    return experiments


def _dominates(a: pd.Series, b: pd.Series) -> bool:
    better_or_equal = (
        _safe_float(a["account_total_return_pct"]) >= _safe_float(b["account_total_return_pct"])
        and _safe_float(a["account_max_drawdown_pct"]) >= _safe_float(b["account_max_drawdown_pct"])
        and _safe_float(a["account_profit_factor"]) >= _safe_float(b["account_profit_factor"])
        and _safe_float(a["account_expectancy"]) >= _safe_float(b["account_expectancy"])
        and _safe_float(a["account_level_miss_trade_rate"], 9999.0) <= _safe_float(b["account_level_miss_trade_rate"], 9999.0)
    )
    strictly_better = (
        _safe_float(a["account_total_return_pct"]) > _safe_float(b["account_total_return_pct"])
        or _safe_float(a["account_max_drawdown_pct"]) > _safe_float(b["account_max_drawdown_pct"])
        or _safe_float(a["account_profit_factor"]) > _safe_float(b["account_profit_factor"])
        or _safe_float(a["account_expectancy"]) > _safe_float(b["account_expectancy"])
        or _safe_float(a["account_level_miss_trade_rate"], 9999.0) < _safe_float(b["account_level_miss_trade_rate"], 9999.0)
    )
    return better_or_equal and strictly_better


def _compute_pareto_front(summary_df: pd.DataFrame) -> pd.DataFrame:
    if summary_df.empty:
        return summary_df.copy()
    mask = []
    for idx_a, row_a in summary_df.iterrows():
        dominated = False
        for idx_b, row_b in summary_df.iterrows():
            if idx_a == idx_b:
                continue
            if _dominates(row_b, row_a):
                dominated = True
                break
        mask.append(not dominated)
    frontier = summary_df.loc[mask].copy().reset_index(drop=True)
    frontier["pareto_front"] = True
    return frontier


def _rank_pct(series: pd.Series, ascending: bool) -> pd.Series:
    return series.rank(pct=True, ascending=ascending, method="average").fillna(0.0)


def _attach_scores(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    work["return_score"] = _rank_pct(pd.to_numeric(work["account_total_return_pct"], errors="coerce"), ascending=True)
    work["drawdown_score"] = _rank_pct(pd.to_numeric(work["account_max_drawdown_pct"], errors="coerce"), ascending=True)
    work["pf_score"] = _rank_pct(pd.to_numeric(work["account_profit_factor"], errors="coerce"), ascending=True)
    work["expectancy_score"] = _rank_pct(pd.to_numeric(work["account_expectancy"], errors="coerce"), ascending=True)
    miss_series = pd.to_numeric(work["account_level_miss_trade_rate"], errors="coerce").fillna(9999.0)
    work["miss_score"] = _rank_pct(-miss_series, ascending=True)
    work["balanced_score"] = (
        work["return_score"] * 0.28
        + work["drawdown_score"] * 0.22
        + work["pf_score"] * 0.18
        + work["expectancy_score"] * 0.18
        + work["miss_score"] * 0.14
    ).round(6)
    return work


def _select_recommendations(frontier_df: pd.DataFrame) -> dict[str, dict[str, Any]]:
    aggressive = frontier_df.sort_values(
        ["account_total_return_pct", "account_profit_factor", "account_max_drawdown_pct"],
        ascending=[False, False, False],
    ).iloc[0]
    defensive = frontier_df.sort_values(
        ["account_max_drawdown_pct", "account_level_miss_trade_rate", "account_profit_factor"],
        ascending=[False, True, False],
    ).iloc[0]
    balanced = frontier_df.sort_values(
        ["balanced_score", "account_total_return_pct", "account_max_drawdown_pct"],
        ascending=[False, False, False],
    ).iloc[0]
    return {
        "AGGRESSIVE": aggressive.to_dict(),
        "BALANCED": balanced.to_dict(),
        "DEFENSIVE": defensive.to_dict(),
    }


def _find_baseline(summary_df: pd.DataFrame) -> pd.Series:
    baseline = summary_df[
        (summary_df["max_concurrent_positions"] == 10)
        & (summary_df["max_new_positions_per_day"] == 3)
        & (summary_df["buy_priority"] == "rps50_desc_ma20_bias_asc_score_desc")
    ]
    if baseline.empty:
        return pd.Series(dtype=object)
    return baseline.iloc[0]


def _find_near_return_lower_miss(summary_df: pd.DataFrame, baseline: pd.Series) -> pd.DataFrame:
    if baseline.empty:
        return pd.DataFrame()
    baseline_return = _safe_float(baseline["account_total_return_pct"])
    baseline_miss = _safe_float(baseline["account_level_miss_trade_rate"], 9999.0)
    candidates = summary_df[
        (pd.to_numeric(summary_df["account_total_return_pct"], errors="coerce") >= baseline_return * 0.9)
        & (pd.to_numeric(summary_df["account_level_miss_trade_rate"], errors="coerce") < baseline_miss)
    ].copy()
    return candidates.sort_values(
        ["account_level_miss_trade_rate", "account_total_return_pct"],
        ascending=[True, False],
    )


def _markdown_table(df: pd.DataFrame) -> str:
    if df.empty:
        return "_无数据_"
    safe_df = df.copy().where(pd.notna(df), "N/A")
    columns = list(safe_df.columns)
    lines = ["|" + "|".join(columns) + "|", "|" + "|".join(["---"] * len(columns)) + "|"]
    for _, row in safe_df.iterrows():
        rendered = [str(row[col]).replace("|", "\\|") for col in columns]
        lines.append("|" + "|".join(rendered) + "|")
    return "\n".join(lines)


def _write_report(
    path: Path,
    summary_df: pd.DataFrame,
    frontier_df: pd.DataFrame,
    recommendations: dict[str, dict[str, Any]],
    baseline_on_frontier: bool,
    near_return_lower_miss_df: pd.DataFrame,
) -> None:
    display_cols = [
        "experiment_id",
        "max_concurrent_positions",
        "max_new_positions_per_day",
        "buy_priority",
        "account_total_return_pct",
        "account_max_drawdown_pct",
        "account_profit_factor",
        "account_expectancy",
        "account_level_miss_trade_rate",
        "trade_count",
        "pareto_front",
        "balanced_score",
    ]
    lines = [
        "# Stage 6E Pareto Recommendation",
        "",
        "## Pareto 前沿",
        "",
        _markdown_table(frontier_df[display_cols]),
        "",
        "## 推荐配置",
        "",
        "### AGGRESSIVE",
        "",
        _markdown_table(pd.DataFrame([recommendations["AGGRESSIVE"]])),
        "",
        "### BALANCED",
        "",
        _markdown_table(pd.DataFrame([recommendations["BALANCED"]])),
        "",
        "### DEFENSIVE",
        "",
        _markdown_table(pd.DataFrame([recommendations["DEFENSIVE"]])),
        "",
        "## 回答",
        "",
        f"- 当前 baseline 是否仍在 Pareto 前沿上: `{baseline_on_frontier}`",
        f"- 是否存在更优均衡点: `{recommendations['BALANCED']['experiment_id'] != 'PARETO_C10_N3_rps50_desc_ma20_bias_asc_score_desc'}`",
        f"- 是否存在“收益近似但 miss 更低”的配置: `{not near_return_lower_miss_df.empty}`",
        "",
        "## 收益近似但 miss 更低的配置",
        "",
        _markdown_table(
            near_return_lower_miss_df[
                [
                    "experiment_id",
                    "max_concurrent_positions",
                    "max_new_positions_per_day",
                    "buy_priority",
                    "account_total_return_pct",
                    "account_level_miss_trade_rate",
                    "account_max_drawdown_pct",
                ]
            ]
            if not near_return_lower_miss_df.empty
            else pd.DataFrame()
        ),
        "",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8-sig")


def run(
    base_dir: str = str(DEFAULT_BASE_DIR),
    start_date: str = "2021-01-01",
    end_date: str | None = None,
    max_files: int | None = 160,
    initial_cash: float = 1_000_000.0,
    max_single_position_pct: float = 0.10,
    max_total_position_pct: float = 0.95,
) -> dict[str, Any]:
    base_path = Path(base_dir)
    reports_dir = base_path / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    config = stage6d._build_period_config(
        base_dir=base_path,
        start_date=start_date,
        end_date=end_date,
        max_files=max_files,
        initial_cash=initial_cash,
        max_single_position_pct=max_single_position_pct,
        max_total_position_pct=max_total_position_pct,
    )
    histories, row_maps, sorted_dates = stage6c._load_histories(config)
    if not histories:
        raise FileNotFoundError("未加载到有效历史行情文件，无法执行 Stage 6E Pareto 筛选")

    experiments = _build_experiments()
    rows: list[dict[str, Any]] = []

    print("============================================================")
    print("Stage 6E 账户层 Pareto 筛选开始")
    print(f"样本文件数: {len(histories)}")
    print(f"样本区间  : {start_date} ~ {end_date or '最新'}")
    print("============================================================")

    for experiment in experiments:
        exp_config = stage6d._build_period_config(
            base_dir=base_path,
            start_date=start_date,
            end_date=end_date,
            max_files=max_files,
            initial_cash=initial_cash,
            max_single_position_pct=max_single_position_pct,
            max_total_position_pct=max_total_position_pct,
        )
        exp_config.max_new_positions_per_day = experiment.max_new_positions_per_day
        exp_config.max_concurrent_positions = experiment.max_concurrent_positions
        summary, _ = stage6d._run_experiment(
            experiment=experiment,
            config=exp_config,
            histories=histories,
            row_maps=row_maps,
            sorted_dates=sorted_dates,
        )
        rows.append(summary)

    summary_df = pd.DataFrame(rows)
    summary_df = _attach_scores(summary_df)
    frontier_df = _compute_pareto_front(summary_df)
    summary_df["pareto_front"] = summary_df["experiment_id"].isin(frontier_df["experiment_id"])
    frontier_df = summary_df[summary_df["pareto_front"]].copy().reset_index(drop=True)
    recommendations = _select_recommendations(frontier_df)
    baseline = _find_baseline(summary_df)
    baseline_on_frontier = bool(not baseline.empty and baseline["pareto_front"])
    near_return_lower_miss_df = _find_near_return_lower_miss(summary_df, baseline)

    summary_path = reports_dir / "stage6e_pareto_summary.csv"
    analysis_path = reports_dir / "stage6e_pareto_analysis.json"
    report_path = reports_dir / "stage6e_recommendation.md"

    summary_df.to_csv(summary_path, index=False, encoding="utf-8-sig")
    analysis_payload = {
        "baseline_on_pareto_front": baseline_on_frontier,
        "baseline_experiment_id": baseline.get("experiment_id", "") if not baseline.empty else "",
        "pareto_frontier": frontier_df.to_dict(orient="records"),
        "recommendations": recommendations,
        "near_return_lower_miss": near_return_lower_miss_df.to_dict(orient="records"),
        "has_better_balanced_point": recommendations["BALANCED"]["experiment_id"] != "PARETO_C10_N3_rps50_desc_ma20_bias_asc_score_desc",
        "has_near_return_lower_miss": not near_return_lower_miss_df.empty,
    }
    analysis_path.write_text(json.dumps(analysis_payload, ensure_ascii=False, indent=2), encoding="utf-8-sig")
    _write_report(
        report_path,
        summary_df=summary_df,
        frontier_df=frontier_df,
        recommendations=recommendations,
        baseline_on_frontier=baseline_on_frontier,
        near_return_lower_miss_df=near_return_lower_miss_df,
    )

    print("============================================================")
    print("Stage 6E 账户层 Pareto 筛选完成")
    print(f"汇总 CSV : {summary_path}")
    print(f"分析 JSON: {analysis_path}")
    print(f"建议报告 : {report_path}")
    print("============================================================")

    return {
        "stage_status": "SUCCESS_EXECUTED",
        "summary_path": str(summary_path),
        "analysis_path": str(analysis_path),
        "report_path": str(report_path),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stage 6E 账户层 Pareto 前沿筛选")
    parser.add_argument("--base-dir", default=str(DEFAULT_BASE_DIR))
    parser.add_argument("--start-date", default="2021-01-01")
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--max-files", type=int, default=160)
    parser.add_argument("--initial-cash", type=float, default=1_000_000.0)
    parser.add_argument("--max-single-position-pct", type=float, default=0.10)
    parser.add_argument("--max-total-position-pct", type=float, default=0.95)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run(
        base_dir=args.base_dir,
        start_date=args.start_date,
        end_date=args.end_date,
        max_files=args.max_files,
        initial_cash=args.initial_cash,
        max_single_position_pct=args.max_single_position_pct,
        max_total_position_pct=args.max_total_position_pct,
    )


if __name__ == "__main__":
    main()
