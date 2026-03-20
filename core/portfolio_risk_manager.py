from __future__ import annotations

from pathlib import Path
import numpy as np
import pandas as pd

try:
    from config.settings import (
        PORTFOLIO_CAPITAL,
        PORTFOLIO_RISK_METADATA_PATH,
        PORTFOLIO_MAX_INDUSTRY_POSITION,
        PORTFOLIO_MAX_STYLE_POSITION,
        PORTFOLIO_MAX_HOT_POSITION,
        PORTFOLIO_MAX_GROWTH_POSITION,
        PORTFOLIO_MAX_EXPECTED_LOSS_PCT,
        PORTFOLIO_UNKNOWN_INDUSTRY,
        PORTFOLIO_UNKNOWN_STYLE,
    )
except ImportError:
    PORTFOLIO_CAPITAL = 1_000_000
    PORTFOLIO_RISK_METADATA_PATH = r"C:\quant_system\reports\stock_risk_metadata.csv"
    PORTFOLIO_MAX_INDUSTRY_POSITION = 0.25
    PORTFOLIO_MAX_STYLE_POSITION = 0.30
    PORTFOLIO_MAX_HOT_POSITION = 0.12
    PORTFOLIO_MAX_GROWTH_POSITION = 0.20
    PORTFOLIO_MAX_EXPECTED_LOSS_PCT = 0.04
    PORTFOLIO_UNKNOWN_INDUSTRY = "未知行业"
    PORTFOLIO_UNKNOWN_STYLE = "未知风格"


REQUIRED_COLUMNS = [
    "code",
    "heat_level",
    "entry_price",
    "stop_loss",
    "target_price",
    "stop_loss_pct",
    "target_position_pct",
]


def _validate_columns(df: pd.DataFrame):
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"组合计划缺少字段: {missing}")


def _is_growth_board(code: str) -> bool:
    code = str(code)
    return code.startswith("sz.300") or code.startswith("sh.688")


def _append_reason(df: pd.DataFrame, mask: pd.Series, reason: str) -> pd.DataFrame:
    if mask.sum() == 0:
        return df
    old = df.loc[mask, "risk_reason"].fillna("")
    df.loc[mask, "risk_reason"] = old.apply(lambda x: f"{x}|{reason}" if x else reason)
    return df


def load_portfolio_plan(csv_path: str | Path) -> pd.DataFrame:
    csv_path = Path(csv_path)
    if not csv_path.exists():
        raise FileNotFoundError(f"未找到组合计划文件: {csv_path}")

    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    _validate_columns(df)
    return df


def _infer_style_tag(row: pd.Series) -> str:
    code = str(row.get("code", ""))
    heat = str(row.get("heat_level", ""))

    if code.startswith("sz.300") or code.startswith("sh.688"):
        return "成长弹性"
    if heat in ("偏热", "过热"):
        return "情绪弹性"
    return "主板趋势"


def merge_risk_metadata(
    df: pd.DataFrame,
    metadata_path: str | Path | None = None,
) -> pd.DataFrame:
    df = df.copy()

    df["industry_lv1"] = PORTFOLIO_UNKNOWN_INDUSTRY
    df["style_tag"] = None
    df["style_tag_source"] = "inferred"

    metadata_path = metadata_path or PORTFOLIO_RISK_METADATA_PATH
    metadata_path = Path(metadata_path)

    if metadata_path.exists():
        meta = pd.read_csv(metadata_path, encoding="utf-8-sig")

        keep_cols = ["code"]
        if "industry_lv1" in meta.columns:
            keep_cols.append("industry_lv1")
        if "style_tag" in meta.columns:
            keep_cols.append("style_tag")
            meta["style_tag_source"] = "metadata"
            keep_cols.append("style_tag_source")

        meta = meta[keep_cols].drop_duplicates(subset=["code"]).copy()

        df = df.merge(meta, on="code", how="left", suffixes=("", "_meta"))

        if "industry_lv1_meta" in df.columns:
            df["industry_lv1"] = df["industry_lv1_meta"].combine_first(df["industry_lv1"])
            df.drop(columns=["industry_lv1_meta"], inplace=True)

        if "style_tag_meta" in df.columns:
            df["style_tag"] = df["style_tag_meta"].combine_first(df["style_tag"])
            df.drop(columns=["style_tag_meta"], inplace=True)

        if "style_tag_source_meta" in df.columns:
            df["style_tag_source"] = df["style_tag_source_meta"].combine_first(df["style_tag_source"])
            df.drop(columns=["style_tag_source_meta"], inplace=True)

    df["industry_lv1"] = df["industry_lv1"].fillna(PORTFOLIO_UNKNOWN_INDUSTRY)
    df["industry_lv1"] = df["industry_lv1"].replace("", PORTFOLIO_UNKNOWN_INDUSTRY)

    style_missing_mask = df["style_tag"].isna() | (df["style_tag"] == "")
    if style_missing_mask.any():
        df.loc[style_missing_mask, "style_tag"] = df.loc[style_missing_mask].apply(_infer_style_tag, axis=1)

    df["style_tag"] = df["style_tag"].fillna(PORTFOLIO_UNKNOWN_STYLE)
    df["style_tag"] = df["style_tag"].replace("", PORTFOLIO_UNKNOWN_STYLE)
    df["style_tag_source"] = df["style_tag_source"].fillna("inferred")

    df["is_growth_board"] = df["code"].apply(_is_growth_board)

    return df


def init_review_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["pre_risk_target_position_pct"] = pd.to_numeric(df["target_position_pct"], errors="coerce").fillna(0.0)
    df["review_target_position_pct"] = df["pre_risk_target_position_pct"].copy()
    df["risk_reason"] = ""

    return df


def apply_mask_cap(
    df: pd.DataFrame,
    mask: pd.Series,
    cap: float,
    reason: str,
) -> tuple[pd.DataFrame, dict | None]:
    df = df.copy()

    current_sum = float(df.loc[mask, "review_target_position_pct"].sum())
    if current_sum <= cap + 1e-12 or current_sum <= 0:
        return df, None

    factor = cap / current_sum
    df.loc[mask, "review_target_position_pct"] *= factor
    df = _append_reason(df, mask, reason)

    log = {
        "rule": reason,
        "before": current_sum,
        "after": float(df.loc[mask, "review_target_position_pct"].sum()),
    }
    return df, log


def apply_group_cap(
    df: pd.DataFrame,
    group_col: str,
    cap: float,
    reason_prefix: str,
    skip_values: set[str] | None = None,
) -> tuple[pd.DataFrame, list[dict]]:
    df = df.copy()
    logs = []
    skip_values = skip_values or set()

    if group_col not in df.columns:
        return df, logs

    group_sum = (
        df.groupby(group_col, dropna=False)["review_target_position_pct"]
        .sum()
        .sort_values(ascending=False)
    )

    for group_value, total_pos in group_sum.items():
        if pd.isna(group_value):
            continue
        if str(group_value) in skip_values:
            continue
        if total_pos <= cap + 1e-12:
            continue

        mask = df[group_col] == group_value
        factor = cap / float(total_pos)
        df.loc[mask, "review_target_position_pct"] *= factor
        df = _append_reason(df, mask, f"{reason_prefix}:{group_value}")

        logs.append({
            "rule": f"{reason_prefix}:{group_value}",
            "before": float(total_pos),
            "after": float(df.loc[mask, 'review_target_position_pct'].sum()),
        })

    return df, logs


def apply_expected_loss_budget(
    df: pd.DataFrame,
    max_expected_loss_pct: float,
) -> tuple[pd.DataFrame, dict | None]:
    df = df.copy()

    total_loss_pct = float((df["review_target_position_pct"] * df["stop_loss_pct"]).sum())
    if total_loss_pct <= max_expected_loss_pct + 1e-12 or total_loss_pct <= 0:
        return df, None

    factor = max_expected_loss_pct / total_loss_pct
    df["review_target_position_pct"] *= factor
    df = _append_reason(df, pd.Series([True] * len(df), index=df.index), "组合风险预算压缩")

    log = {
        "rule": "组合风险预算压缩",
        "before": total_loss_pct,
        "after": float((df["review_target_position_pct"] * df["stop_loss_pct"]).sum()),
    }
    return df, log


def recalc_after_risk_review(
    df: pd.DataFrame,
    capital: float,
) -> pd.DataFrame:
    df = df.copy()

    df["review_planned_capital"] = df["review_target_position_pct"] * capital
    df["review_planned_shares"] = (
        np.floor(df["review_planned_capital"] / df["entry_price"] / 100) * 100
    ).astype(int)

    df["review_actual_capital"] = df["review_planned_shares"] * df["entry_price"]
    df["review_actual_position_pct"] = df["review_actual_capital"] / capital

    df["review_expected_loss_amt"] = (
        (df["entry_price"] - df["stop_loss"]) * df["review_planned_shares"]
    )
    df["review_expected_profit_amt"] = (
        (df["target_price"] - df["entry_price"]) * df["review_planned_shares"]
    )

    df["risk_adjustment_pct"] = (
        df["review_target_position_pct"] - df["pre_risk_target_position_pct"]
    )

    return df


def review_portfolio_plan(
    portfolio_plan_path: str | Path,
    capital: float = PORTFOLIO_CAPITAL,
    metadata_path: str | Path | None = None,
) -> tuple[pd.DataFrame, dict, list[dict]]:
    df = load_portfolio_plan(portfolio_plan_path)
    df = merge_risk_metadata(df, metadata_path=metadata_path)
    df = init_review_columns(df)

    logs = []

    # 1) 偏热仓位上限
    hot_mask = df["heat_level"].isin(["偏热", "过热"])
    df, log = apply_mask_cap(
        df,
        mask=hot_mask,
        cap=PORTFOLIO_MAX_HOT_POSITION,
        reason="偏热仓位超限压缩",
    )
    if log:
        logs.append(log)

    # 2) 创业板/科创板仓位上限
    growth_mask = df["is_growth_board"]
    df, log = apply_mask_cap(
        df,
        mask=growth_mask,
        cap=PORTFOLIO_MAX_GROWTH_POSITION,
        reason="成长板仓位超限压缩",
    )
    if log:
        logs.append(log)

    # 3) 行业集中度上限
    df, industry_logs = apply_group_cap(
        df,
        group_col="industry_lv1",
        cap=PORTFOLIO_MAX_INDUSTRY_POSITION,
        reason_prefix="行业集中度压缩",
        skip_values={PORTFOLIO_UNKNOWN_INDUSTRY},
    )
    logs.extend(industry_logs)

    # 4) 风格集中度上限
    # 只有 metadata 提供了真实 style_tag，才启用风格集中度约束
    style_df = df[df["style_tag_source"] == "metadata"].copy()
    if not style_df.empty:
        reviewed_style_df, style_logs = apply_group_cap(
            style_df,
            group_col="style_tag",
            cap=PORTFOLIO_MAX_STYLE_POSITION,
            reason_prefix="风格集中度压缩",
            skip_values={PORTFOLIO_UNKNOWN_STYLE},
        )

        df.loc[reviewed_style_df.index, "review_target_position_pct"] = reviewed_style_df["review_target_position_pct"]
        df.loc[reviewed_style_df.index, "risk_reason"] = reviewed_style_df["risk_reason"]
        logs.extend(style_logs)

    # 5) 组合级风险预算
    df, log = apply_expected_loss_budget(
        df,
        max_expected_loss_pct=PORTFOLIO_MAX_EXPECTED_LOSS_PCT,
    )
    if log:
        logs.append(log)

    df = recalc_after_risk_review(df, capital=capital)

    summary = {
        "capital": float(capital),
        "before_target_total_position_pct": float(df["pre_risk_target_position_pct"].sum()),
        "after_target_total_position_pct": float(df["review_target_position_pct"].sum()),
        "after_actual_total_position_pct": float(df["review_actual_position_pct"].sum()),
        "cash_pct": float(1 - df["review_actual_position_pct"].sum()),
        "before_expected_loss_pct": float((df["pre_risk_target_position_pct"] * df["stop_loss_pct"]).sum()),
        "after_expected_loss_pct": float((df["review_target_position_pct"] * df["stop_loss_pct"]).sum()),
        "after_expected_loss_amt": float(df["review_expected_loss_amt"].sum()),
        "after_expected_profit_amt": float(df["review_expected_profit_amt"].sum()),
        "industry_count": int(df["industry_lv1"].nunique()),
        "style_count": int(df["style_tag"].nunique()),
    }

    return df, summary, logs


def save_risk_review(
    reviewed_df: pd.DataFrame,
    summary: dict,
    logs: list[dict],
    reports_dir: str | Path,
) -> dict:
    reports_dir = Path(reports_dir)
    reports_dir.mkdir(parents=True, exist_ok=True)

    out_csv = reports_dir / "daily_portfolio_plan_risk_checked.csv"
    out_txt = reports_dir / "daily_portfolio_summary_risk_checked.txt"

    reviewed_df.to_csv(out_csv, index=False, encoding="utf-8-sig")

    industry_exp = (
        reviewed_df.groupby("industry_lv1")["review_actual_position_pct"]
        .sum()
        .sort_values(ascending=False)
    )
    style_exp = (
        reviewed_df.groupby("style_tag")["review_actual_position_pct"]
        .sum()
        .sort_values(ascending=False)
    )

    with open(out_txt, "w", encoding="utf-8") as f:
        f.write("======== 组合风控复核摘要 ========\n")
        f.write(f"总资金: {summary['capital']:.2f}\n")
        f.write(f"风控前目标总仓位: {summary['before_target_total_position_pct']:.2%}\n")
        f.write(f"风控后目标总仓位: {summary['after_target_total_position_pct']:.2%}\n")
        f.write(f"风控后实际总仓位: {summary['after_actual_total_position_pct']:.2%}\n")
        f.write(f"剩余现金比例: {summary['cash_pct']:.2%}\n")
        f.write(f"风控前预期亏损比例: {summary['before_expected_loss_pct']:.2%}\n")
        f.write(f"风控后预期亏损比例: {summary['after_expected_loss_pct']:.2%}\n")
        f.write(f"风控后预期止损金额: {summary['after_expected_loss_amt']:.2f}\n")
        f.write(f"风控后预期止盈金额: {summary['after_expected_profit_amt']:.2f}\n")
        f.write("\n")

        f.write("---- 风控规则触发记录 ----\n")
        if logs:
            for item in logs:
                f.write(f"{item['rule']}: {item['before']:.2%} -> {item['after']:.2%}\n")
        else:
            f.write("无规则触发\n")

        f.write("\n---- 行业暴露 ----\n")
        for k, v in industry_exp.items():
            f.write(f"{k}: {v:.2%}\n")

        f.write("\n---- 风格暴露 ----\n")
        for k, v in style_exp.items():
            f.write(f"{k}: {v:.2%}\n")

    return {
        "risk_checked_csv": str(out_csv),
        "risk_checked_summary": str(out_txt),
    }