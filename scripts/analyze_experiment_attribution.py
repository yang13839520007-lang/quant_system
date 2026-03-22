# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd


ROOT_DIR = Path(__file__).resolve().parent.parent
DEFAULT_BASE_DIR = ROOT_DIR

MODULE_MAP = {
    "Exp1": "Regime_Safe",
    "Exp2": "Volume Shrink",
    "Exp3": "LEFT_CATCH entry",
    "Exp4": "Upper Shadow exit",
}


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
        if pd.isna(out):
            return default
        return out
    except (TypeError, ValueError):
        return default


def _read_summary(csv_path: Path, json_path: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    payload: dict[str, Any] = {}
    if csv_path.exists():
        summary_df = pd.read_csv(csv_path, encoding="utf-8-sig")
        if json_path.exists():
            with json_path.open("r", encoding="utf-8-sig") as fh:
                payload = json.load(fh)
        return summary_df, payload
    if json_path.exists():
        with json_path.open("r", encoding="utf-8-sig") as fh:
            payload = json.load(fh)
        summary_df = pd.DataFrame(payload.get("experiments", []))
        return summary_df, payload
    raise FileNotFoundError("未找到 experiment_matrix_summary.csv/json")


def _build_pairwise(summary_df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for group_name, module_name in MODULE_MAP.items():
        control = summary_df[(summary_df["group_name"] == group_name) & (summary_df["arm"] == "control")]
        test = summary_df[(summary_df["group_name"] == group_name) & (summary_df["arm"] == "test")]
        if control.empty or test.empty:
            continue
        control_row = control.iloc[0]
        test_row = test.iloc[0]
        rows.append(
            {
                "group_name": group_name,
                "module_name": module_name,
                "control_id": control_row["experiment_id"],
                "test_id": test_row["experiment_id"],
                "delta_total_return_pct": round(_safe_float(test_row["total_return_pct"]) - _safe_float(control_row["total_return_pct"]), 4),
                "delta_win_rate": round(_safe_float(test_row["win_rate"]) - _safe_float(control_row["win_rate"]), 4),
                "delta_profit_factor": round(_safe_float(test_row["profit_factor"]) - _safe_float(control_row["profit_factor"]), 4),
                "delta_max_drawdown_pct": round(_safe_float(test_row["max_drawdown_pct"]) - _safe_float(control_row["max_drawdown_pct"]), 4),
                "delta_expectancy": round(_safe_float(test_row["expectancy"]) - _safe_float(control_row["expectancy"]), 4),
                "delta_avg_holding_days": round(_safe_float(test_row["avg_holding_days"]) - _safe_float(control_row["avg_holding_days"]), 4),
                "delta_trade_count": int(_safe_float(test_row["trade_count"]) - _safe_float(control_row["trade_count"])),
                "delta_miss_trade_rate": round(_safe_float(test_row["miss_trade_rate"]) - _safe_float(control_row["miss_trade_rate"]), 4),
            }
        )
    return pd.DataFrame(rows)


def _recommendation(row: pd.Series) -> str:
    dd_gain = _safe_float(row.get("delta_max_drawdown_pct"))
    ret_drag = _safe_float(row.get("delta_total_return_pct"))
    exp_gain = _safe_float(row.get("delta_expectancy"))
    pf_gain = _safe_float(row.get("delta_profit_factor"))
    miss_drag = _safe_float(row.get("delta_miss_trade_rate"))

    if dd_gain >= 8 and ret_drag >= -80 and (exp_gain >= 0 or pf_gain >= 0):
        return "保留"
    if dd_gain >= 8 and ret_drag < -80:
        return "需要弱化/放宽参数"
    if miss_drag >= 8 or (ret_drag < -150 and exp_gain >= -0.10 and pf_gain >= -0.05):
        return "需要进入下一轮参数搜索"
    if ret_drag < -150 and exp_gain < -0.10 and pf_gain < -0.05 and dd_gain < 5:
        return "删除"
    return "需要进入下一轮参数搜索"


def _module_comment(row: pd.Series) -> str:
    module_name = str(row["module_name"])
    dd_gain = _safe_float(row["delta_max_drawdown_pct"])
    ret_drag = _safe_float(row["delta_total_return_pct"])
    exp_gain = _safe_float(row["delta_expectancy"])
    pf_gain = _safe_float(row["delta_profit_factor"])
    miss_drag = _safe_float(row["delta_miss_trade_rate"])

    parts = []
    if dd_gain > 0:
        parts.append(f"回撤改善 {dd_gain:.4f}pct")
    elif dd_gain < 0:
        parts.append(f"回撤恶化 {abs(dd_gain):.4f}pct")
    if ret_drag > 0:
        parts.append(f"收益提升 {ret_drag:.4f}pct")
    elif ret_drag < 0:
        parts.append(f"收益拖累 {abs(ret_drag):.4f}pct")
    if exp_gain > 0:
        parts.append(f"expectancy 提升 {exp_gain:.4f}")
    elif exp_gain < 0:
        parts.append(f"expectancy 下降 {abs(exp_gain):.4f}")
    if pf_gain > 0:
        parts.append(f"profit factor 提升 {pf_gain:.4f}")
    elif pf_gain < 0:
        parts.append(f"profit factor 下降 {abs(pf_gain):.4f}")
    if module_name == "LEFT_CATCH entry":
        if miss_drag > 0:
            parts.append(f"miss_trade_rate 增加 {miss_drag:.4f}pct")
        elif miss_drag < 0:
            parts.append(f"miss_trade_rate 改善 {abs(miss_drag):.4f}pct")
    return "；".join(parts)


def _markdown_table(df: pd.DataFrame) -> str:
    if df.empty:
        return "_无数据_"
    safe_df = df.copy().where(pd.notna(df), "N/A")
    columns = list(safe_df.columns)
    lines = [
        "|" + "|".join(columns) + "|",
        "|" + "|".join(["---"] * len(columns)) + "|",
    ]
    for _, row in safe_df.iterrows():
        lines.append("|" + "|".join(str(row[col]) for col in columns) + "|")
    return "\n".join(lines)


def _build_overall_diagnosis(summary_df: pd.DataFrame, pairwise_df: pd.DataFrame) -> dict[str, Any]:
    baseline = summary_df[summary_df["experiment_id"] == "BASELINE_ORIGINAL"]
    upgraded = summary_df[summary_df["experiment_id"] == "ROUTE_A_FULL_UPGRADED"]
    baseline_row = baseline.iloc[0] if not baseline.empty else pd.Series(dtype=object)
    upgraded_row = upgraded.iloc[0] if not upgraded.empty else pd.Series(dtype=object)

    by_dd = pairwise_df.sort_values("delta_max_drawdown_pct", ascending=False).reset_index(drop=True)
    by_ret = pairwise_df.sort_values("delta_total_return_pct").reset_index(drop=True)
    by_pf = pairwise_df.sort_values(["delta_profit_factor", "delta_expectancy"], ascending=False).reset_index(drop=True)

    best_drawdown = by_dd.iloc[0].to_dict() if not by_dd.empty else {}
    worst_return = by_ret.iloc[0].to_dict() if not by_ret.empty else {}
    best_pf = by_pf.iloc[0].to_dict() if not by_pf.empty else {}

    reasons = []
    trade_count_gap = int(_safe_float(upgraded_row.get("trade_count")) - _safe_float(baseline_row.get("trade_count")))
    miss_trade_gap = round(_safe_float(upgraded_row.get("miss_trade_rate")) - _safe_float(baseline_row.get("miss_trade_rate")), 4)
    expectancy_gap = round(_safe_float(upgraded_row.get("expectancy")) - _safe_float(baseline_row.get("expectancy")), 4)
    profit_factor_gap = round(_safe_float(upgraded_row.get("profit_factor")) - _safe_float(baseline_row.get("profit_factor")), 4)

    if trade_count_gap < 0:
        reasons.append(f"全升级版交易数较 baseline 减少 {abs(trade_count_gap)} 笔，说明 Regime_Safe 与 Volume Shrink 明显压缩了入场频率。")
    if miss_trade_gap > 0:
        reasons.append(f"全升级版 miss_trade_rate 高出 {miss_trade_gap:.4f}pct，说明 LEFT_CATCH 承接买入带来了显著未成交损耗。")
    if expectancy_gap < 0:
        reasons.append(f"全升级版 expectancy 下降 {abs(expectancy_gap):.4f}，单笔收益质量未能抵消交易机会收缩。")
    if profit_factor_gap < 0:
        reasons.append(f"全升级版 profit factor 下降 {abs(profit_factor_gap):.4f}，胜负收益比没有优于 baseline。")
    if not reasons:
        reasons.append("全升级版与 baseline 差异不显著，需要更长样本或进一步参数搜索。")

    return {
        "best_drawdown_module": best_drawdown,
        "worst_return_drag_module": worst_return,
        "best_expectancy_profit_factor_module": best_pf,
        "baseline_total_return_pct": round(_safe_float(baseline_row.get("total_return_pct")), 4),
        "upgraded_total_return_pct": round(_safe_float(upgraded_row.get("total_return_pct")), 4),
        "baseline_max_drawdown_pct": round(_safe_float(baseline_row.get("max_drawdown_pct")), 4),
        "upgraded_max_drawdown_pct": round(_safe_float(upgraded_row.get("max_drawdown_pct")), 4),
        "baseline_expectancy": round(_safe_float(baseline_row.get("expectancy")), 4),
        "upgraded_expectancy": round(_safe_float(upgraded_row.get("expectancy")), 4),
        "baseline_profit_factor": round(_safe_float(baseline_row.get("profit_factor")), 4),
        "upgraded_profit_factor": round(_safe_float(upgraded_row.get("profit_factor")), 4),
        "main_underperformance_reasons": reasons,
    }


def _write_report(
    path: Path,
    summary_df: pd.DataFrame,
    pairwise_df: pd.DataFrame,
    diagnosis: dict[str, Any],
    module_df: pd.DataFrame,
) -> None:
    baseline_df = summary_df[summary_df["experiment_id"].isin(["BASELINE_ORIGINAL", "ROUTE_A_FULL_UPGRADED"])][
        ["experiment_id", "description", "total_return_pct", "max_drawdown_pct", "expectancy", "profit_factor", "trade_count", "miss_trade_rate"]
    ]
    display_module_df = module_df[
        ["module_name", "delta_total_return_pct", "delta_max_drawdown_pct", "delta_expectancy", "delta_profit_factor", "delta_miss_trade_rate", "recommendation", "commentary"]
    ]
    lines = [
        "# Stage 5.5 Strategy Attribution Report",
        "",
        "## 基线与全升级版",
        "",
        _markdown_table(baseline_df),
        "",
        "## 模块贡献分析",
        "",
        _markdown_table(display_module_df),
        "",
        "## 关键归因结论",
        "",
        f"- 显著降低回撤的模块: `{diagnosis['best_drawdown_module'].get('module_name', 'N/A')}`",
        f"- 显著拖累绝对收益的模块: `{diagnosis['worst_return_drag_module'].get('module_name', 'N/A')}`",
        f"- expectancy / profit factor 改善最明显的模块: `{diagnosis['best_expectancy_profit_factor_module'].get('module_name', 'N/A')}`",
        "",
        "## Route A 全升级版未跑赢 baseline 的主要原因",
        "",
    ]
    for reason in diagnosis["main_underperformance_reasons"]:
        lines.append(f"- {reason}")
    lines.extend(
        [
            "",
            "## 建议",
            "",
        ]
    )
    for _, row in module_df.iterrows():
        lines.append(f"- {row['module_name']}: {row['recommendation']}。{row['commentary']}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8-sig")


def run(base_dir: str = str(DEFAULT_BASE_DIR)) -> dict[str, Any]:
    reports_dir = Path(base_dir) / "reports"
    csv_path = reports_dir / "experiment_matrix_summary.csv"
    json_path = reports_dir / "experiment_matrix_summary.json"
    report_path = reports_dir / "strategy_attribution_report.md"
    recommendation_path = reports_dir / "strategy_stage55_recommendation.json"

    summary_df, payload = _read_summary(csv_path=csv_path, json_path=json_path)
    if summary_df.empty:
        raise ValueError("experiment_matrix_summary 为空，无法做 Stage 5.5 归因")

    pairwise_df = _build_pairwise(summary_df)
    if pairwise_df.empty:
        raise ValueError("未找到 Exp1~Exp4 的 control/test 成对结果")

    module_df = pairwise_df.copy()
    module_df["recommendation"] = module_df.apply(_recommendation, axis=1)
    module_df["commentary"] = module_df.apply(_module_comment, axis=1)

    diagnosis = _build_overall_diagnosis(summary_df=summary_df, pairwise_df=pairwise_df)
    recommendation_payload = {
        "source_files": {
            "summary_csv": str(csv_path),
            "summary_json": str(json_path) if json_path.exists() else "",
        },
        "overall_diagnosis": diagnosis,
        "module_recommendations": module_df.to_dict(orient="records"),
        "stage5_config": payload.get("config", {}),
        "caveats": payload.get("caveats", []),
    }

    _write_report(
        path=report_path,
        summary_df=summary_df,
        pairwise_df=pairwise_df,
        diagnosis=diagnosis,
        module_df=module_df,
    )
    recommendation_path.write_text(
        json.dumps(recommendation_payload, ensure_ascii=False, indent=2),
        encoding="utf-8-sig",
    )

    print("============================================================")
    print("Stage 5.5 归因分析完成")
    print(f"归因报告: {report_path}")
    print(f"建议 JSON: {recommendation_path}")
    print("============================================================")

    return {
        "stage_status": "SUCCESS_EXECUTED",
        "report_path": str(report_path),
        "recommendation_path": str(recommendation_path),
        "module_count": int(len(module_df)),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stage 5.5 实验结果归因与建议")
    parser.add_argument("--base-dir", default=str(DEFAULT_BASE_DIR))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run(base_dir=args.base_dir)


if __name__ == "__main__":
    main()
