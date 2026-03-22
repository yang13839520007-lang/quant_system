# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict

import pandas as pd


BASE_DIR = r"C:\quant_system"
ERROR_COLUMNS = ["code", "name", "signal_date", "route_a_signal", "reject_reason"]
STAGE01_STATUS_PATH = "daily_candidates_status.json"
STAGE02_STATUS_PATH = "daily_trade_plan_status.json"
PENDING_STAGE_STATUS = {"NON_TRADING_DAY", "WAITING_MARKET_DATA", "DATA_STALE"}


def _load_optional_settings():
    try:
        from config import settings  # type: ignore

        return settings
    except Exception:
        return None


def _get_setting(name: str, default: Any) -> Any:
    settings = _load_optional_settings()
    if settings is not None and hasattr(settings, name):
        return getattr(settings, name)
    return default


def _safe_float(value: Any) -> float | None:
    try:
        out = float(value)
        if pd.isna(out):
            return None
        return out
    except (TypeError, ValueError):
        return None


def _safe_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    return text in {"1", "true", "yes", "y"}


def _build_plan_reason(code: str, signal_date: str, target_buy_price_base: float, stop_loss: float, target_profit_pct: float) -> str:
    return (
        f"Route A T+1 左侧接针计划: {code} 基于 {signal_date} 信号日回踩结构，"
        f"次日参考承接价 {target_buy_price_base:.2f}，跌破 MA20 防守位 {stop_loss:.2f} 后按次日开盘退出，"
        f"止盈目标 {target_profit_pct:.2%}。"
    )


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _write_stage02_status(reports_dir: Path, payload: Dict[str, Any]) -> None:
    with open(reports_dir / STAGE02_STATUS_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def _write_pending_outputs(reports_dir: Path, payload: Dict[str, Any]) -> Dict[str, Any]:
    pd.DataFrame().to_csv(reports_dir / "daily_trade_plan_all.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame().to_csv(reports_dir / "daily_trade_plan_top10.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(columns=ERROR_COLUMNS).to_csv(reports_dir / "daily_trade_plan_errors.csv", index=False, encoding="utf-8-sig")
    _write_stage02_status(reports_dir, payload)
    return payload


def run(
    trading_date: str,
    base_dir: str = BASE_DIR,
    target_profit_pct: float | None = None,
    shadow_threshold: float | None = None,
) -> Dict[str, Any]:
    print(f"    --> [DEBUG] Stage 02 进入 Route A 交易计划层，日期: {trading_date}")
    reports_dir = Path(base_dir) / "reports"
    candidates_path = reports_dir / "daily_candidates_all.csv"
    errors_path = reports_dir / "daily_trade_plan_errors.csv"
    stage01_status_payload = _read_json(reports_dir / STAGE01_STATUS_PATH)
    upstream_status = str(stage01_status_payload.get("stage_status", "")).strip().upper()
    upstream_reason = str(stage01_status_payload.get("error", "")).strip()

    if not candidates_path.exists():
        if upstream_status in PENDING_STAGE_STATUS:
            payload = {
                "stage_status": upstream_status,
                "success": False,
                "trading_date": trading_date,
                "error": upstream_reason or "Stage 01 尚未形成当日候选，交易计划层暂停执行。",
                "blocked_by_stage": 1,
            }
            return _write_pending_outputs(reports_dir, payload)
        payload = {"stage_status": "FAILED", "error": "缺少 daily_candidates_all.csv"}
        _write_stage02_status(reports_dir, payload)
        return payload

    try:
        df = pd.read_csv(candidates_path, encoding="utf-8-sig")
    except pd.errors.EmptyDataError:
        df = pd.DataFrame()
    if df.empty:
        if upstream_status in PENDING_STAGE_STATUS:
            payload = {
                "stage_status": upstream_status,
                "success": False,
                "trading_date": trading_date,
                "error": upstream_reason or "Stage 01 尚未形成当日候选，交易计划层暂停执行。",
                "blocked_by_stage": 1,
            }
            return _write_pending_outputs(reports_dir, payload)
        payload = {"stage_status": "FAILED", "error": "候选文件为空，无法生成交易计划"}
        _write_stage02_status(reports_dir, payload)
        return payload

    target_profit_pct = float(
        target_profit_pct if target_profit_pct is not None else _get_setting("TRADE_PLAN_TARGET_PROFIT_PCT", 0.08)
    )
    shadow_threshold = float(
        shadow_threshold if shadow_threshold is not None else _get_setting("TRADE_PLAN_SHADOW_THRESHOLD", 0.60)
    )
    capital = float(_get_setting("PORTFOLIO_CAPITAL", 1_000_000.0))
    max_plan_count = int(_get_setting("TRADE_PLAN_MAX_STOCKS", 10))
    max_single_position_pct = float(_get_setting("TRADE_PLAN_MAX_SINGLE_POSITION_PCT", 0.10))
    lot_size = int(_get_setting("LOT_SIZE", 100))

    work = df.copy()
    work["route_a_signal"] = work.get("route_a_signal", False).apply(_safe_bool)
    work["signal_date"] = work.get("trade_date", work.get("trading_date", trading_date)).astype(str)
    work["pre_low"] = pd.to_numeric(work.get("low_price"), errors="coerce")
    work["pre_ma20"] = pd.to_numeric(work.get("ma20"), errors="coerce")
    work["score"] = pd.to_numeric(work.get("score"), errors="coerce")
    work["rank"] = pd.to_numeric(work.get("rank"), errors="coerce")
    work["target_buy_price_base"] = (work["pre_low"] * 1.01).round(4)
    work["entry_mode"] = "ROUTE_A_LEFT_CATCH"
    work["stop_mode"] = "CLOSE_BELOW_MA20_EXIT_NEXT_OPEN"
    work["stop_loss"] = work["pre_ma20"].round(4)
    work["target_profit_pct"] = float(target_profit_pct)
    work["shadow_threshold"] = float(shadow_threshold)
    work["entry_valid"] = (
        work["route_a_signal"]
        & work["pre_low"].notna()
        & work["pre_ma20"].notna()
        & (work["pre_low"] > 0)
        & (work["pre_ma20"] > 0)
    )

    error_records: list[dict[str, Any]] = []
    invalid_mask = ~work["route_a_signal"]
    if invalid_mask.any():
        invalid_rows = work[invalid_mask].copy()
        error_records.extend(
            invalid_rows.assign(reject_reason="ROUTE_A_SIGNAL_FALSE")[
                ["code", "name", "signal_date", "route_a_signal"]
            ].to_dict(orient="records")
        )

    invalid_entry_mask = work["route_a_signal"] & ~work["entry_valid"]
    if invalid_entry_mask.any():
        invalid_rows = work[invalid_entry_mask].copy()
        error_records.extend(
            invalid_rows.assign(reject_reason="INVALID_PRE_LOW_OR_PRE_MA20")[
                ["code", "name", "signal_date", "route_a_signal"]
            ].to_dict(orient="records")
        )

    selected = work[work["route_a_signal"] & work["entry_valid"]].copy()
    if selected.empty:
        pd.DataFrame(error_records, columns=ERROR_COLUMNS).to_csv(errors_path, index=False, encoding="utf-8-sig")
        payload = {"stage_status": "FAILED", "error": "没有 route_a_signal == True 且可生成 T+1 左侧接针计划的候选。"}
        _write_stage02_status(reports_dir, payload)
        return payload

    selected = selected.sort_values(["rank", "score"], ascending=[True, False]).reset_index(drop=True).head(max_plan_count).copy()
    target_position_pct = min(max_single_position_pct, 1.0 / max_plan_count if max_plan_count > 0 else max_single_position_pct)

    plans: list[dict[str, Any]] = []
    for idx, row in selected.iterrows():
        code = row.get("code")
        signal_date = str(row.get("signal_date", ""))
        target_buy_price_base = _safe_float(row.get("target_buy_price_base"))
        stop_loss = _safe_float(row.get("stop_loss"))
        score = _safe_float(row.get("score"))
        if not code or target_buy_price_base is None or target_buy_price_base <= 0 or stop_loss is None or stop_loss <= 0:
            error_records.append(
                {
                    "code": code,
                    "name": row.get("name", ""),
                    "signal_date": signal_date,
                    "route_a_signal": row.get("route_a_signal", False),
                    "reject_reason": "INVALID_TARGET_BUY_PRICE_OR_STOP",
                }
            )
            continue

        alloc_capital = capital * target_position_pct
        shares = int((alloc_capital // target_buy_price_base) // lot_size * lot_size)
        if shares < lot_size:
            error_records.append(
                {
                    "code": code,
                    "name": row.get("name", ""),
                    "signal_date": signal_date,
                    "route_a_signal": row.get("route_a_signal", False),
                    "reject_reason": "LESS_THAN_ONE_LOT_AFTER_POSITION_SIZING",
                }
            )
            continue

        expected_loss_amt = max(target_buy_price_base - stop_loss, 0.0) * shares
        target_price = round(target_buy_price_base * (1 + target_profit_pct), 4)
        expected_profit_amt = max(target_price - target_buy_price_base, 0.0) * shares

        plans.append(
            {
                "trade_date": trading_date,
                "signal_date": signal_date,
                "portfolio_rank": idx + 1,
                "code": code,
                "name": row.get("name", ""),
                "action": row.get("action", "正常跟踪"),
                "heat_level": row.get("heat_level", "正常"),
                "score": round(score if score is not None else 0.0, 2),
                "entry_mode": "ROUTE_A_LEFT_CATCH",
                "pre_low": round(float(row["pre_low"]), 4),
                "pre_ma20": round(float(row["pre_ma20"]), 4),
                "target_buy_price_base": round(target_buy_price_base, 4),
                "entry_valid": True,
                "entry_price": round(target_buy_price_base, 4),
                "stop_mode": "CLOSE_BELOW_MA20_EXIT_NEXT_OPEN",
                "stop_loss": round(stop_loss, 4),
                "target_profit_pct": round(float(target_profit_pct), 4),
                "shadow_threshold": round(float(shadow_threshold), 4),
                "target_price": round(target_price, 4),
                "suggested_shares": shares,
                "suggested_position_pct": round((shares * target_buy_price_base) / capital, 4),
                "expected_loss_amt": round(expected_loss_amt, 2),
                "expected_profit_amt": round(expected_profit_amt, 2),
                "plan_reason": _build_plan_reason(
                    code=code,
                    signal_date=signal_date,
                    target_buy_price_base=target_buy_price_base,
                    stop_loss=stop_loss,
                    target_profit_pct=float(target_profit_pct),
                ),
                "route_a_signal": True,
                "candidate_reason": row.get("candidate_reason", ""),
            }
        )

    df_plan = pd.DataFrame(plans)
    error_df = pd.DataFrame(error_records, columns=ERROR_COLUMNS)
    error_df.to_csv(errors_path, index=False, encoding="utf-8-sig")

    if df_plan.empty:
        payload = {"stage_status": "FAILED", "error": "资金分配后，没有标的满足最低买入 1 手的约束。"}
        _write_stage02_status(reports_dir, payload)
        return payload

    df_plan = df_plan.sort_values(["portfolio_rank", "score"], ascending=[True, False]).reset_index(drop=True)
    df_plan.to_csv(reports_dir / "daily_trade_plan_all.csv", index=False, encoding="utf-8-sig")
    df_plan.head(10).to_csv(reports_dir / "daily_trade_plan_top10.csv", index=False, encoding="utf-8-sig")

    payload = {
        "stage_status": "SUCCESS_EXECUTED",
        "success": True,
        "trading_date": trading_date,
        "plan_count": int(len(df_plan)),
        "error_count": int(len(error_df)),
        "target_profit_pct": float(target_profit_pct),
        "shadow_threshold": float(shadow_threshold),
    }
    _write_stage02_status(reports_dir, payload)
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description="Route A T+1 左侧接针交易计划层")
    parser.add_argument("--trading-date", required=True)
    parser.add_argument("--base-dir", default=BASE_DIR)
    parser.add_argument("--target-profit-pct", type=float, default=None)
    parser.add_argument("--shadow-threshold", type=float, default=None)
    args, _ = parser.parse_known_args()

    res = run(
        trading_date=args.trading_date,
        base_dir=args.base_dir,
        target_profit_pct=args.target_profit_pct,
        shadow_threshold=args.shadow_threshold,
    )
    if res.get("stage_status") == "FAILED":
        print(f"ERROR: {res.get('error')}")
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
