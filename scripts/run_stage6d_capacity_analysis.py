# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd


SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR = SCRIPT_DIR.parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import run_stage6a_parameter_search as stage6a  # noqa: E402
import run_stage6c_account_replay as stage6c  # noqa: E402


DEFAULT_BASE_DIR = ROOT_DIR
MISS_REASONS = [
    "NO_CASH_AVAILABLE",
    "MAX_NEW_POSITIONS_PER_DAY_REACHED",
    "MAX_CONCURRENT_POSITIONS_REACHED",
    "MAX_SINGLE_POSITION_LIMIT",
    "MAX_TOTAL_POSITION_LIMIT",
    "ENTRY_NOT_FILLED",
    "T1_CASH_NOT_RELEASED",
    "AUDIT_BLOCKED",
]


@dataclass(frozen=True)
class CapacityExperiment:
    experiment_id: str
    experiment_group: str
    description: str
    max_new_positions_per_day: int = 3
    max_concurrent_positions: int = 10
    buy_priority: str = "rps50_desc_ma20_bias_asc_score_desc"
    settlement_mode: str = "NEXT_DAY_AVAILABLE"


def _safe_float(value: Any, default: float = 0.0) -> float:
    return stage6a._safe_float(value, default=default)


def _sort_events(events: list[dict[str, Any]], ordering: str) -> list[dict[str, Any]]:
    if ordering == "ma20_bias_asc_rps50_desc_score_desc":
        key_fn = lambda item: (float(item["ma20_bias"]), -float(item["rps50"]), -float(item["score"]), item["code"])
    elif ordering == "score_desc_rps50_desc_ma20_bias_asc":
        key_fn = lambda item: (-float(item["score"]), -float(item["rps50"]), float(item["ma20_bias"]), item["code"])
    elif ordering == "rps50_desc_score_desc_ma20_bias_asc":
        key_fn = lambda item: (-float(item["rps50"]), -float(item["score"]), float(item["ma20_bias"]), item["code"])
    else:
        key_fn = lambda item: (-float(item["rps50"]), float(item["ma20_bias"]), -float(item["score"]), item["code"])
    return sorted(events, key=key_fn)


def _build_experiments() -> list[CapacityExperiment]:
    experiments: list[CapacityExperiment] = [
        CapacityExperiment("BASELINE_CAPACITY", "Baseline", "Stage 6C 基线容量口径"),
    ]
    for value in [3, 5, 8]:
        experiments.append(
            CapacityExperiment(
                experiment_id=f"EXP1_NEW_PER_DAY_{value}",
                experiment_group="Exp1",
                description=f"max_new_positions_per_day={value}",
                max_new_positions_per_day=value,
            )
        )
    for value in [8, 10, 12, 15]:
        experiments.append(
            CapacityExperiment(
                experiment_id=f"EXP2_CONCURRENT_{value}",
                experiment_group="Exp2",
                description=f"max_concurrent_positions={value}",
                max_concurrent_positions=value,
            )
        )
    for ordering in [
        "rps50_desc_ma20_bias_asc_score_desc",
        "ma20_bias_asc_rps50_desc_score_desc",
        "score_desc_rps50_desc_ma20_bias_asc",
    ]:
        experiments.append(
            CapacityExperiment(
                experiment_id=f"EXP3_PRIORITY_{ordering}",
                experiment_group="Exp3",
                description=f"buy_priority={ordering}",
                buy_priority=ordering,
            )
        )
    for mode in ["NEXT_DAY_AVAILABLE", "SAME_DAY_AVAILABLE"]:
        experiments.append(
            CapacityExperiment(
                experiment_id=f"EXP4_SETTLEMENT_{mode}",
                experiment_group="Exp4",
                description=f"settlement_mode={mode}",
                settlement_mode=mode,
            )
        )
    return experiments


def _build_period_config(base_dir: Path, start_date: str | None, end_date: str | None, max_files: int | None, initial_cash: float, max_single_position_pct: float, max_total_position_pct: float) -> stage6c.AccountReplayConfig:
    return stage6c.AccountReplayConfig(
        base_dir=base_dir,
        reports_dir=base_dir / "reports",
        data_dir=base_dir / "stock_data_5years",
        universe_file=base_dir / "reports" / "batch_backtest_summary.csv",
        start_date=start_date,
        end_date=end_date,
        max_files=max_files,
        amount_min=2e8,
        target_profit_pct=0.08,
        initial_cash=initial_cash,
        commission_rate=0.0003,
        stamp_tax_rate=0.001,
        buy_slippage_bps=8.0,
        sell_slippage_bps=8.0,
        lot_size=100,
        max_concurrent_positions=10,
        max_new_positions_per_day=3,
        max_single_position_pct=max_single_position_pct,
        max_total_position_pct=max_total_position_pct,
    )


def _calc_budget_components(
    available_cash: float,
    receivable_cash: float,
    positions: dict[str, stage6c.Position],
    trade_date: str,
    row_maps: dict[str, dict[str, pd.Series]],
    config: stage6c.AccountReplayConfig,
) -> dict[str, float]:
    market_value = stage6c._market_value_of_positions(positions, trade_date, row_maps)
    total_equity = available_cash + receivable_cash + market_value
    single_cap = max(total_equity * config.max_single_position_pct, 0.0) if total_equity > 0 else 0.0
    total_cap_remaining = max(total_equity * config.max_total_position_pct - market_value, 0.0) if total_equity > 0 else 0.0
    return {
        "market_value": market_value,
        "total_equity": total_equity,
        "single_cap": single_cap,
        "total_cap_remaining": total_cap_remaining,
        "available_cash": available_cash,
        "receivable_cash": receivable_cash,
    }


def _categorize_capacity_reason(
    estimated_total_cost: float,
    budget_components: dict[str, float],
) -> str:
    if budget_components["single_cap"] < estimated_total_cost:
        return "MAX_SINGLE_POSITION_LIMIT"
    if budget_components["total_cap_remaining"] < estimated_total_cost:
        return "MAX_TOTAL_POSITION_LIMIT"
    if budget_components["available_cash"] < estimated_total_cost <= budget_components["available_cash"] + budget_components["receivable_cash"]:
        return "T1_CASH_NOT_RELEASED"
    if budget_components["available_cash"] < estimated_total_cost:
        return "NO_CASH_AVAILABLE"
    return "NO_CASH_AVAILABLE"


def _estimate_entry_total_cost(signal_row: pd.Series, exec_row: pd.Series, config: stage6c.AccountReplayConfig) -> tuple[bool, float, str]:
    open_price = _safe_float(exec_row.get("open"))
    low_price = _safe_float(exec_row.get("low"))
    if open_price <= 0 or low_price <= 0:
        return False, 0.0, "ENTRY_NOT_FILLED"
    if stage6a.exp5._is_one_word_limit_down(exec_row):
        return False, 0.0, "ENTRY_NOT_FILLED"
    target_buy_price_base = _safe_float(signal_row.get("low")) * stage6c.BASELINE_CFG.left_catch_multiplier
    if target_buy_price_base <= 0:
        return False, 0.0, "ENTRY_NOT_FILLED"
    planned_buy_price = min(open_price, target_buy_price_base)
    if low_price <= planned_buy_price:
        raw_price = planned_buy_price
    elif stage6c.BASELINE_CFG.fallback_rule == "NO_FILL_BUY_AT_OPEN_IF_GAP_SMALL" and open_price <= target_buy_price_base * (1 + stage6c.BASELINE_CFG.max_gap_chase_pct):
        raw_price = open_price
    else:
        return False, 0.0, "ENTRY_NOT_FILLED"
    executed_price = round(raw_price * (1 + config.buy_slippage_bps / 10000.0), 4)
    estimated_total_cost = executed_price * config.lot_size + max(executed_price * config.lot_size * config.commission_rate, 5.0)
    return True, estimated_total_cost, ""


def _run_experiment(
    experiment: CapacityExperiment,
    config: stage6c.AccountReplayConfig,
    histories: dict[str, pd.DataFrame],
    row_maps: dict[str, dict[str, pd.Series]],
    sorted_dates: list[str],
) -> tuple[dict[str, Any], dict[str, int]]:
    by_entry_date, _, _ = stage6c._build_candidate_events(histories=histories, config=config)
    positions: dict[str, stage6c.Position] = {}
    pending_cash: list[dict[str, Any]] = []
    available_cash = float(config.initial_cash)
    trades: list[dict[str, Any]] = []
    equity_records: list[dict[str, Any]] = []
    miss_counts = {reason: 0 for reason in MISS_REASONS}
    attempted_buy_count = 0
    max_reached = 0

    runner_cfg = stage6a.RunnerConfig(
        base_dir=config.base_dir,
        reports_dir=config.reports_dir,
        data_dir=config.data_dir,
        universe_file=config.universe_file,
        start_date=config.start_date,
        end_date=config.end_date,
        max_files=config.max_files,
        amount_min=config.amount_min,
        target_profit_pct=config.target_profit_pct,
        initial_trade_cash=config.initial_cash,
        commission_rate=config.commission_rate,
        stamp_tax_rate=config.stamp_tax_rate,
        buy_slippage_bps=config.buy_slippage_bps,
        sell_slippage_bps=config.sell_slippage_bps,
        lot_size=config.lot_size,
    )

    for trade_date in sorted_dates:
        released = [item for item in pending_cash if item["available_date"] <= trade_date]
        pending_cash = [item for item in pending_cash if item["available_date"] > trade_date]
        for item in released:
            available_cash += float(item["amount"])

        sell_codes = sorted([code for code, pos in positions.items() if pos.scheduled_sell_date == trade_date])
        for code in sell_codes:
            pos = positions[code]
            filled, trade, net_amount = stage6c._process_sell_order(pos=pos, trade_date=trade_date, row_maps=row_maps, config=config)
            if not filled:
                next_sell_date = stage6c._next_date(trade_date, sorted_dates)
                positions[code].scheduled_sell_date = next_sell_date or ""
                continue
            next_cash_date = trade_date if experiment.settlement_mode == "SAME_DAY_AVAILABLE" else stage6c._next_date(trade_date, sorted_dates)
            if next_cash_date is None or next_cash_date <= trade_date:
                available_cash += net_amount
            else:
                pending_cash.append({"available_date": next_cash_date, "amount": net_amount})
            entry_dt = pd.to_datetime(pos.entry_date)
            exit_dt = pd.to_datetime(trade_date)
            trade["holding_days"] = int(max((exit_dt - entry_dt).days, 1))
            trades.append(trade)
            del positions[code]

        day_events = _sort_events(by_entry_date.get(trade_date, []), experiment.buy_priority)
        current_count = len(positions)
        open_slots = max(experiment.max_concurrent_positions - current_count, 0)
        used_new_positions = 0
        receivable_cash = sum(float(item["amount"]) for item in pending_cash)

        for event in day_events:
            attempted_buy_count += 1
            code = str(event["code"])
            if code in positions:
                miss_counts["MAX_CONCURRENT_POSITIONS_REACHED"] += 1
                continue
            if len(positions) >= experiment.max_concurrent_positions:
                miss_counts["MAX_CONCURRENT_POSITIONS_REACHED"] += 1
                continue
            if used_new_positions >= min(experiment.max_new_positions_per_day, open_slots):
                miss_counts["MAX_NEW_POSITIONS_PER_DAY_REACHED"] += 1
                continue

            signal_row = histories[code].iloc[int(event["signal_idx"])]
            exec_row = row_maps.get(code, {}).get(trade_date)
            if exec_row is None:
                miss_counts["ENTRY_NOT_FILLED"] += 1
                continue

            fillable, estimated_total_cost, pre_reason = _estimate_entry_total_cost(signal_row=signal_row, exec_row=exec_row, config=config)
            if not fillable:
                miss_counts[pre_reason] += 1
                continue

            budget_components = _calc_budget_components(
                available_cash=available_cash,
                receivable_cash=receivable_cash,
                positions=positions,
                trade_date=trade_date,
                row_maps=row_maps,
                config=config,
            )
            dominant_reason = _categorize_capacity_reason(estimated_total_cost, budget_components)
            buy_budget = stage6c._calc_buy_budget(
                available_cash=available_cash,
                receivable_cash=receivable_cash,
                positions=positions,
                trade_date=trade_date,
                row_maps=row_maps,
                config=config,
            )
            if buy_budget < estimated_total_cost:
                miss_counts[dominant_reason] += 1
                continue

            filled, entry_price, shares, total_cost, reason = stage6c._calc_buy_fill(
                signal_row=signal_row,
                exec_row=exec_row,
                budget=buy_budget,
                config=config,
            )
            if not filled or shares <= 0:
                miss_counts["ENTRY_NOT_FILLED" if reason != "INSUFFICIENT_CASH_OR_LOT" else dominant_reason] += 1
                continue

            available_cash -= total_cost
            used_new_positions += 1
            positions[code] = stage6c.Position(
                code=code,
                entry_date=trade_date,
                shares=shares,
                entry_price=entry_price,
                entry_total_cost=total_cost,
                signal_date=str(event["signal_date"]),
                rps50=float(event["rps50"]),
                ma20_bias=float(event["ma20_bias"]),
                score=float(event["score"]),
            )

        for code, pos in list(positions.items()):
            row = row_maps.get(code, {}).get(trade_date)
            if row is None:
                continue
            exit_signal, exit_reason = stage6a._evaluate_exit_signal(review_row=row, entry_price=pos.entry_price, cfg=stage6c.BASELINE_CFG, config=runner_cfg)
            if exit_signal:
                next_sell_date = stage6c._next_date(trade_date, sorted_dates)
                if next_sell_date:
                    pos.pending_exit = True
                    pos.pending_exit_signal = exit_signal
                    pos.pending_exit_reason = exit_reason
                    pos.scheduled_sell_date = next_sell_date

        market_value = stage6c._market_value_of_positions(positions, trade_date, row_maps)
        receivable_cash = sum(float(item["amount"]) for item in pending_cash)
        total_equity = available_cash + receivable_cash + market_value
        concurrent_positions = len(positions)
        max_reached = max(max_reached, concurrent_positions)
        equity_records.append(
            {
                "trade_date": trade_date,
                "equity": round(total_equity, 2),
                "market_value": round(market_value, 2),
                "available_cash": round(available_cash, 2),
                "receivable_cash": round(receivable_cash, 2),
                "concurrent_positions": concurrent_positions,
                "position_utilization": round(market_value / total_equity, 6) if total_equity > 0 else 0.0,
                "cash_utilization": round(available_cash / total_equity, 6) if total_equity > 0 else 0.0,
            }
        )

    trade_df = pd.DataFrame(trades)
    equity_df = pd.DataFrame(equity_records)
    gross_profit = float(trade_df.loc[trade_df["pnl_amount"] > 0, "pnl_amount"].sum()) if not trade_df.empty else 0.0
    gross_loss = float(trade_df.loc[trade_df["pnl_amount"] < 0, "pnl_amount"].sum()) if not trade_df.empty else 0.0
    profit_factor = gross_profit / abs(gross_loss) if gross_loss < 0 else None
    final_equity = float(equity_df["equity"].iloc[-1]) if not equity_df.empty else config.initial_cash
    total_return_pct = (final_equity / config.initial_cash - 1.0) * 100.0 if config.initial_cash > 0 else 0.0
    miss_trade_rate = sum(miss_counts.values()) / attempted_buy_count * 100.0 if attempted_buy_count else None
    monthly_stats = stage6c._calc_monthly_stats(equity_df)

    summary = {
        "experiment_id": experiment.experiment_id,
        "experiment_group": experiment.experiment_group,
        "description": experiment.description,
        "max_new_positions_per_day": experiment.max_new_positions_per_day,
        "max_concurrent_positions": experiment.max_concurrent_positions,
        "buy_priority": experiment.buy_priority,
        "settlement_mode": experiment.settlement_mode,
        "account_total_return_pct": round(total_return_pct, 4),
        "account_max_drawdown_pct": stage6c._calc_max_drawdown(equity_df["equity"]) if not equity_df.empty else 0.0,
        "account_profit_factor": round(profit_factor, 4) if profit_factor is not None else None,
        "account_expectancy": round(float(trade_df["return_pct"].mean()), 4) if not trade_df.empty else 0.0,
        "account_level_miss_trade_rate": round(miss_trade_rate, 4) if miss_trade_rate is not None else None,
        "average_position_utilization": round(float(equity_df["position_utilization"].mean()), 4) if not equity_df.empty else 0.0,
        "average_cash_utilization": round(float(equity_df["cash_utilization"].mean()), 4) if not equity_df.empty else 0.0,
        "average_concurrent_positions": round(float(equity_df["concurrent_positions"].mean()), 4) if not equity_df.empty else 0.0,
        "trade_count": int(len(trade_df)),
        "max_concurrent_positions_reached": int(max_reached),
        "miss_trade_reason_breakdown": json.dumps(miss_counts, ensure_ascii=False, sort_keys=True),
        "monthly_return_stats": json.dumps(
            {
                "monthly_return_mean": monthly_stats["monthly_return_mean"],
                "monthly_return_std": monthly_stats["monthly_return_std"],
                "best_month_return_pct": monthly_stats["best_month_return_pct"],
                "worst_month_return_pct": monthly_stats["worst_month_return_pct"],
                "positive_month_ratio_pct": monthly_stats["positive_month_ratio_pct"],
                "extreme_month_flag": monthly_stats["extreme_month_flag"],
            },
            ensure_ascii=False,
        ),
    }
    return summary, miss_counts


def _build_breakdown_rows(summary_rows: list[dict[str, Any]], miss_maps: dict[str, dict[str, int]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    summary_lookup = {row["experiment_id"]: row for row in summary_rows}
    for experiment_id, miss_counts in miss_maps.items():
        total_attempts = sum(miss_counts.values())
        meta = summary_lookup[experiment_id]
        for reason in MISS_REASONS:
            count = int(miss_counts.get(reason, 0))
            rows.append(
                {
                    "experiment_id": experiment_id,
                    "experiment_group": meta["experiment_group"],
                    "description": meta["description"],
                    "reason": reason,
                    "miss_count": count,
                    "miss_pct_of_misses": round(count / total_attempts * 100.0, 4) if total_attempts else 0.0,
                }
            )
    return pd.DataFrame(rows)


def _write_report(path: Path, summary_df: pd.DataFrame, breakdown_df: pd.DataFrame, analysis: dict[str, Any]) -> None:
    display_cols = [
        "experiment_id",
        "experiment_group",
        "description",
        "account_total_return_pct",
        "account_max_drawdown_pct",
        "account_profit_factor",
        "account_expectancy",
        "account_level_miss_trade_rate",
        "average_position_utilization",
        "average_cash_utilization",
        "average_concurrent_positions",
        "trade_count",
    ]
    top_bottlenecks = pd.DataFrame(analysis.get("baseline_top_bottlenecks", []))
    best_miss = pd.DataFrame([analysis.get("best_miss_trade_experiment", {})])
    best_return = pd.DataFrame([analysis.get("best_return_experiment", {})])
    lines = [
        "# Stage 6D Capacity Report",
        "",
        "## 实验总表",
        "",
        stage6a._markdown_table(summary_df[display_cols]),
        "",
        "## Baseline Miss Trade 主瓶颈",
        "",
        stage6a._markdown_table(top_bottlenecks),
        "",
        "## 最能降低 Miss Trade 的实验",
        "",
        stage6a._markdown_table(best_miss),
        "",
        "## 收益最优实验",
        "",
        stage6a._markdown_table(best_return),
        "",
        "## 结论",
        "",
        f"- 账户层主瓶颈: `{analysis.get('primary_bottleneck_reason', 'N/A')}`",
        f"- baseline miss trade rate: `{analysis.get('baseline_miss_trade_rate', 'N/A')}`",
        f"- 最优缓解实验: `{analysis.get('best_miss_trade_experiment', {}).get('experiment_id', 'N/A')}`",
        f"- 是否由容量约束主导: `{analysis.get('capacity_is_primary_bottleneck', False)}`",
        "",
    ]
    for note in analysis.get("notes", []):
        lines.append(f"- {note}")
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
    config = _build_period_config(
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
        raise FileNotFoundError("未加载到有效历史行情文件，无法执行 Stage 6D 容量分析")

    summary_rows: list[dict[str, Any]] = []
    miss_maps: dict[str, dict[str, int]] = {}
    experiments = _build_experiments()

    print("============================================================")
    print("Stage 6D 容量与排队规则复核开始")
    print(f"样本文件数: {len(histories)}")
    print(f"样本区间  : {start_date} ~ {end_date or '最新'}")
    print("============================================================")

    for experiment in experiments:
        exp_config = _build_period_config(
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
        summary, miss_counts = _run_experiment(
            experiment=experiment,
            config=exp_config,
            histories=histories,
            row_maps=row_maps,
            sorted_dates=sorted_dates,
        )
        summary_rows.append(summary)
        miss_maps[experiment.experiment_id] = miss_counts

    summary_df = pd.DataFrame(summary_rows)
    breakdown_df = _build_breakdown_rows(summary_rows, miss_maps)

    baseline_breakdown = breakdown_df[breakdown_df["experiment_id"] == "BASELINE_CAPACITY"].sort_values("miss_count", ascending=False).reset_index(drop=True)
    best_miss = summary_df.sort_values(["account_level_miss_trade_rate", "account_total_return_pct"], ascending=[True, False]).iloc[0].to_dict()
    best_return = summary_df.sort_values(["account_total_return_pct", "account_max_drawdown_pct"], ascending=[False, False]).iloc[0].to_dict()
    primary_reason = baseline_breakdown.iloc[0]["reason"] if not baseline_breakdown.empty else "N/A"
    analysis = {
        "primary_bottleneck_reason": primary_reason,
        "baseline_miss_trade_rate": float(summary_df.loc[summary_df["experiment_id"] == "BASELINE_CAPACITY", "account_level_miss_trade_rate"].iloc[0]),
        "baseline_top_bottlenecks": baseline_breakdown.head(5).to_dict(orient="records"),
        "best_miss_trade_experiment": best_miss,
        "best_return_experiment": best_return,
        "capacity_is_primary_bottleneck": primary_reason in {
            "MAX_NEW_POSITIONS_PER_DAY_REACHED",
            "MAX_CONCURRENT_POSITIONS_REACHED",
            "NO_CASH_AVAILABLE",
            "T1_CASH_NOT_RELEASED",
            "MAX_TOTAL_POSITION_LIMIT",
            "MAX_SINGLE_POSITION_LIMIT",
        },
        "notes": [
            f"Baseline 最大 miss reason 为 {primary_reason}。",
            "AUDIT_BLOCKED 在当前账户层回放中未触发，原因是本阶段只复核容量与排队规则，不额外重跑 Stage 1 审计。",
        ],
    }

    summary_path = reports_dir / "stage6d_capacity_summary.csv"
    analysis_path = reports_dir / "stage6d_capacity_analysis.json"
    report_path = reports_dir / "stage6d_capacity_report.md"
    breakdown_path = reports_dir / "stage6d_miss_trade_breakdown.csv"

    summary_df.to_csv(summary_path, index=False, encoding="utf-8-sig")
    breakdown_df.to_csv(breakdown_path, index=False, encoding="utf-8-sig")
    analysis_path.write_text(json.dumps(analysis, ensure_ascii=False, indent=2), encoding="utf-8-sig")
    _write_report(report_path, summary_df, breakdown_df, analysis)

    print("============================================================")
    print("Stage 6D 容量与排队规则复核完成")
    print(f"汇总 CSV : {summary_path}")
    print(f"分析 JSON: {analysis_path}")
    print(f"报告      : {report_path}")
    print(f"拆解 CSV  : {breakdown_path}")
    print("============================================================")

    return {
        "stage_status": "SUCCESS_EXECUTED",
        "summary_path": str(summary_path),
        "analysis_path": str(analysis_path),
        "report_path": str(report_path),
        "breakdown_path": str(breakdown_path),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stage 6D 账户层容量与排队规则复核")
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
