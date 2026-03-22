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

import run_stage6a_parameter_search as stage6a  # noqa: E402


DEFAULT_BASE_DIR = ROOT_DIR
RECOMMENDED_BASELINE = stage6a.SearchConfig(
    regime_mode="HARD_BLOCK",
    regime_ma_window=25,
    regime_risk_scale=1.0,
    left_catch_multiplier=1.015,
    max_gap_chase_pct=0.02,
    fallback_rule="NO_FILL_BUY_AT_OPEN_IF_GAP_SMALL",
    shadow_threshold=0.60,
    min_profit_to_trigger_shadow_exit=0.0,
    shadow_exit_mode="NEXT_OPEN_HALF_EXIT",
    use_volume_sanity_check=False,
    volume_sanity_ratio=1.20,
)


def _safe_float(value: Any, default: float = 0.0) -> float:
    return stage6a._safe_float(value, default=default)


def _calc_trade_concentration(trade_df: pd.DataFrame) -> dict[str, Any]:
    if trade_df.empty:
        return {
            "top_5_pnl_contribution_pct": None,
            "top_10_pnl_contribution_pct": None,
            "positive_trade_count": 0,
            "extreme_concentration_flag": False,
        }
    positive_df = trade_df[pd.to_numeric(trade_df["pnl_amount"], errors="coerce") > 0].copy()
    if positive_df.empty:
        return {
            "top_5_pnl_contribution_pct": 0.0,
            "top_10_pnl_contribution_pct": 0.0,
            "positive_trade_count": 0,
            "extreme_concentration_flag": False,
        }
    total_positive = float(positive_df["pnl_amount"].sum())
    ranked = positive_df.sort_values("pnl_amount", ascending=False)
    top5 = float(ranked.head(5)["pnl_amount"].sum()) / total_positive * 100.0 if total_positive > 0 else 0.0
    top10 = float(ranked.head(10)["pnl_amount"].sum()) / total_positive * 100.0 if total_positive > 0 else 0.0
    return {
        "top_5_pnl_contribution_pct": round(top5, 4),
        "top_10_pnl_contribution_pct": round(top10, 4),
        "positive_trade_count": int(len(positive_df)),
        "extreme_concentration_flag": bool(top10 >= 60.0),
    }


def _calc_monthly_stability(trade_df: pd.DataFrame) -> dict[str, Any]:
    if trade_df.empty or "exit_date" not in trade_df.columns:
        return {
            "monthly_return_std": None,
            "best_month_return_pct": None,
            "worst_month_return_pct": None,
            "extreme_month_flag": False,
            "months": 0,
        }
    work = trade_df.copy()
    work["exit_date"] = pd.to_datetime(work["exit_date"], errors="coerce")
    work = work[work["exit_date"].notna()].copy()
    if work.empty:
        return {
            "monthly_return_std": None,
            "best_month_return_pct": None,
            "worst_month_return_pct": None,
            "extreme_month_flag": False,
            "months": 0,
        }
    work["month"] = work["exit_date"].dt.to_period("M").astype(str)
    monthly = work.groupby("month", as_index=False)["return_pct"].sum()
    std_val = float(monthly["return_pct"].std(ddof=0)) if len(monthly) > 1 else 0.0
    best_val = float(monthly["return_pct"].max()) if not monthly.empty else 0.0
    worst_val = float(monthly["return_pct"].min()) if not monthly.empty else 0.0
    return {
        "monthly_return_std": round(std_val, 4),
        "best_month_return_pct": round(best_val, 4),
        "worst_month_return_pct": round(worst_val, 4),
        "extreme_month_flag": bool(abs(best_val) >= 35.0 or abs(worst_val) >= 25.0),
        "months": int(len(monthly)),
    }


def _build_period_metrics(
    label: str,
    tag: str,
    cfg: stage6a.SearchConfig,
    trades: list[dict[str, Any]],
    counters: dict[str, int],
) -> tuple[dict[str, Any], dict[str, Any]]:
    metrics = stage6a._summarize_combo(search_stage="STAGE6B", tag=tag, cfg=cfg, trades=trades, counters=counters)
    trade_df = pd.DataFrame(trades)
    concentration = _calc_trade_concentration(trade_df)
    monthly = _calc_monthly_stability(trade_df)
    cost_total = round(float(trade_df.get("pnl_amount", pd.Series(dtype=float)).sum()), 4) if not trade_df.empty else 0.0
    authenticity = {
        "period": label,
        "cost_deducted": True,
        "cost_model_note": "买入计入佣金，卖出计入佣金与印花税，并保留买卖双边滑点。",
        "miss_trade_rate_abnormal": bool(_safe_float(metrics.get("miss_trade_rate"), 0.0) >= 20.0),
        "net_pnl_amount": cost_total,
        **concentration,
        **monthly,
    }
    metrics["period"] = label
    return metrics, authenticity


def _simulate_period(
    label: str,
    cfg: stage6a.SearchConfig,
    base_config: stage6a.RunnerConfig,
    histories: dict[str, pd.DataFrame],
    regime_maps: dict[int, dict[str, bool]],
    start_date: str | None,
    end_date: str | None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    scoped = stage6a.RunnerConfig(
        base_dir=base_config.base_dir,
        reports_dir=base_config.reports_dir,
        data_dir=base_config.data_dir,
        universe_file=base_config.universe_file,
        start_date=start_date,
        end_date=end_date,
        max_files=base_config.max_files,
        amount_min=base_config.amount_min,
        target_profit_pct=base_config.target_profit_pct,
        initial_trade_cash=base_config.initial_trade_cash,
        commission_rate=base_config.commission_rate,
        stamp_tax_rate=base_config.stamp_tax_rate,
        buy_slippage_bps=base_config.buy_slippage_bps,
        sell_slippage_bps=base_config.sell_slippage_bps,
        lot_size=base_config.lot_size,
    )
    rps_map = stage6a._build_rps_map(histories=histories, start_date=start_date, end_date=end_date)
    events_by_stock, events_by_date = stage6a._build_candidate_events(
        histories=histories,
        rps_map=rps_map,
        config=scoped,
        search_cfg=cfg,
    )
    allow_map = stage6a._build_daily_regime_allow_map(events_by_date=events_by_date, regime_maps=regime_maps, cfg=cfg)

    all_trades: list[dict[str, Any]] = []
    counters = {"signal_count": 0, "attempted_entry_count": 0, "missed_entry_count": 0}
    for code, hist in histories.items():
        stock_events = events_by_stock.get(code, [])
        trades, stock_counters = stage6a._simulate_stock(
            hist=hist,
            stock_events=stock_events,
            allow_map=allow_map,
            cfg=cfg,
            config=scoped,
        )
        all_trades.extend(trades)
        for key in counters:
            counters[key] += stock_counters.get(key, 0)
    return _build_period_metrics(label=label, tag=f"{label}_baseline_validation", cfg=cfg, trades=all_trades, counters=counters)


def _write_report(
    path: Path,
    summary_df: pd.DataFrame,
    authenticity: list[dict[str, Any]],
    in_sample_range: tuple[str | None, str | None],
    out_of_sample_range: tuple[str | None, str | None],
    full_range: tuple[str | None, str | None],
) -> None:
    auth_df = pd.DataFrame(authenticity)
    summary_view = summary_df[
        [
            "period",
            "total_return_pct",
            "max_drawdown_pct",
            "expectancy",
            "profit_factor",
            "trade_count",
            "miss_trade_rate",
            "avg_holding_days",
        ]
    ]
    lines = [
        "# Stage 6B Out-of-Sample Validation",
        "",
        "## 验证区间",
        "",
        f"- in_sample: `{in_sample_range[0]}` ~ `{in_sample_range[1]}`",
        f"- out_of_sample: `{out_of_sample_range[0]}` ~ `{out_of_sample_range[1] or '最新'}`",
        f"- full_period: `{full_range[0]}` ~ `{full_range[1] or '最新'}`",
        "",
        "## 核心指标",
        "",
        stage6a._markdown_table(summary_view),
        "",
        "## 真实性复核",
        "",
        stage6a._markdown_table(auth_df),
        "",
        "## 风险提示",
        "",
    ]
    full_row = summary_df[summary_df["period"] == "full_period"].iloc[0]
    if _safe_float(full_row["total_return_pct"]) >= 1000.0:
        lines.append("- `total_return_pct` 较高，当前口径是等额单笔资金累计收益，不代表单账户逐笔复利实盘收益，需谨慎解读。")
    if bool(auth_df["extreme_concentration_flag"].any()):
        lines.append("- 存在正收益被少数交易集中贡献的风险，需警惕样本偶然性。")
    if bool(auth_df["extreme_month_flag"].any()):
        lines.append("- 存在月度收益波动较大的阶段，样本外稳定性需继续观察。")
    if bool(auth_df["miss_trade_rate_abnormal"].any()):
        lines.append("- 某些验证区间 miss_trade_rate 偏高，说明 LEFT_CATCH 在部分阶段存在较强漏单风险。")
    lines.append("- 成本已扣除：买入佣金、卖出佣金、印花税、双边滑点均已纳入。")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8-sig")


def run(
    base_dir: str = str(DEFAULT_BASE_DIR),
    in_sample_start: str = "2021-01-01",
    in_sample_end: str = "2024-12-31",
    out_sample_start: str = "2025-01-01",
    out_sample_end: str | None = None,
    full_start: str = "2021-01-01",
    full_end: str | None = None,
    max_files: int | None = 160,
    amount_min: float = 2e8,
    target_profit_pct: float = 0.08,
    initial_trade_cash: float = 100000.0,
    commission_rate: float = 0.0003,
    stamp_tax_rate: float = 0.001,
    buy_slippage_bps: float = 8.0,
    sell_slippage_bps: float = 8.0,
    lot_size: int = 100,
) -> dict[str, Any]:
    base_path = Path(base_dir)
    reports_dir = base_path / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    runner_config = stage6a.RunnerConfig(
        base_dir=base_path,
        reports_dir=reports_dir,
        data_dir=base_path / "stock_data_5years",
        universe_file=reports_dir / "batch_backtest_summary.csv",
        start_date=full_start,
        end_date=full_end,
        max_files=max_files,
        amount_min=float(amount_min),
        target_profit_pct=float(target_profit_pct),
        initial_trade_cash=float(initial_trade_cash),
        commission_rate=float(commission_rate),
        stamp_tax_rate=float(stamp_tax_rate),
        buy_slippage_bps=float(buy_slippage_bps),
        sell_slippage_bps=float(sell_slippage_bps),
        lot_size=int(lot_size),
    )

    histories, loaded_files = stage6a._load_histories(runner_config)
    if not histories:
        raise FileNotFoundError("未加载到有效历史行情文件，无法执行 Stage 6B 验证")
    regime_maps = stage6a._build_regime_maps(runner_config)

    print("============================================================")
    print("Stage 6B 基线长样本验证开始")
    print(f"样本文件数: {len(loaded_files)}")
    print(f"in_sample   : {in_sample_start} ~ {in_sample_end}")
    print(f"out_of_sample: {out_sample_start} ~ {out_sample_end or '最新'}")
    print(f"full_period : {full_start} ~ {full_end or '最新'}")
    print("============================================================")

    results: list[dict[str, Any]] = []
    authenticity_rows: list[dict[str, Any]] = []
    for label, start_date, end_date in [
        ("in_sample", in_sample_start, in_sample_end),
        ("out_of_sample", out_sample_start, out_sample_end),
        ("full_period", full_start, full_end),
    ]:
        metrics, authenticity = _simulate_period(
            label=label,
            cfg=RECOMMENDED_BASELINE,
            base_config=runner_config,
            histories=histories,
            regime_maps=regime_maps,
            start_date=start_date,
            end_date=end_date,
        )
        results.append(metrics)
        authenticity_rows.append(authenticity)

    summary_df = pd.DataFrame(results)
    authenticity_df = pd.DataFrame(authenticity_rows)

    csv_path = reports_dir / "stage6b_baseline_validation_summary.csv"
    json_path = reports_dir / "stage6b_baseline_validation.json"
    report_path = reports_dir / "stage6b_out_of_sample_report.md"

    summary_df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    payload = {
        "recommended_baseline": {
            "regime_mode": RECOMMENDED_BASELINE.regime_mode,
            "regime_ma_window": RECOMMENDED_BASELINE.regime_ma_window,
            "left_catch_multiplier": RECOMMENDED_BASELINE.left_catch_multiplier,
            "max_gap_chase_pct": RECOMMENDED_BASELINE.max_gap_chase_pct,
            "fallback_rule": RECOMMENDED_BASELINE.fallback_rule,
            "shadow_threshold": RECOMMENDED_BASELINE.shadow_threshold,
            "min_profit_to_trigger_shadow_exit": RECOMMENDED_BASELINE.min_profit_to_trigger_shadow_exit,
            "shadow_exit_mode": RECOMMENDED_BASELINE.shadow_exit_mode,
        },
        "ranges": {
            "in_sample": {"start": in_sample_start, "end": in_sample_end},
            "out_of_sample": {"start": out_sample_start, "end": out_sample_end},
            "full_period": {"start": full_start, "end": full_end},
        },
        "summary": summary_df.to_dict(orient="records"),
        "authenticity_checks": authenticity_df.to_dict(orient="records"),
        "notes": [
            "成本已扣除：买入佣金、卖出佣金、印花税、双边滑点。",
            "若 total_return_pct 显著偏高，应按等额单笔资金累计收益口径理解，不等同于单账户逐笔复利实盘。"
        ],
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8-sig")
    _write_report(
        path=report_path,
        summary_df=summary_df,
        authenticity=authenticity_rows,
        in_sample_range=(in_sample_start, in_sample_end),
        out_of_sample_range=(out_sample_start, out_sample_end),
        full_range=(full_start, full_end),
    )

    print("============================================================")
    print("Stage 6B 基线长样本验证完成")
    print(f"汇总 CSV : {csv_path}")
    print(f"汇总 JSON: {json_path}")
    print(f"样本外报告 : {report_path}")
    print("============================================================")

    return {
        "stage_status": "SUCCESS_EXECUTED",
        "summary_path": str(csv_path),
        "summary_json_path": str(json_path),
        "report_path": str(report_path),
        "sample_file_count": int(len(loaded_files)),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stage 6B 推荐 baseline 长样本与样本外验证")
    parser.add_argument("--base-dir", default=str(DEFAULT_BASE_DIR))
    parser.add_argument("--in-sample-start", default="2021-01-01")
    parser.add_argument("--in-sample-end", default="2024-12-31")
    parser.add_argument("--out-sample-start", default="2025-01-01")
    parser.add_argument("--out-sample-end", default=None)
    parser.add_argument("--full-start", default="2021-01-01")
    parser.add_argument("--full-end", default=None)
    parser.add_argument("--max-files", type=int, default=160)
    parser.add_argument("--amount-min", type=float, default=2e8)
    parser.add_argument("--target-profit-pct", type=float, default=0.08)
    parser.add_argument("--initial-trade-cash", type=float, default=100000.0)
    parser.add_argument("--commission-rate", type=float, default=0.0003)
    parser.add_argument("--stamp-tax-rate", type=float, default=0.001)
    parser.add_argument("--buy-slippage-bps", type=float, default=8.0)
    parser.add_argument("--sell-slippage-bps", type=float, default=8.0)
    parser.add_argument("--lot-size", type=int, default=100)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run(
        base_dir=args.base_dir,
        in_sample_start=args.in_sample_start,
        in_sample_end=args.in_sample_end,
        out_sample_start=args.out_sample_start,
        out_sample_end=args.out_sample_end,
        full_start=args.full_start,
        full_end=args.full_end,
        max_files=args.max_files,
        amount_min=args.amount_min,
        target_profit_pct=args.target_profit_pct,
        initial_trade_cash=args.initial_trade_cash,
        commission_rate=args.commission_rate,
        stamp_tax_rate=args.stamp_tax_rate,
        buy_slippage_bps=args.buy_slippage_bps,
        sell_slippage_bps=args.sell_slippage_bps,
        lot_size=args.lot_size,
    )


if __name__ == "__main__":
    main()
