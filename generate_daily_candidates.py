# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
import sys
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict

import pandas as pd


BASE_DIR = r"C:\quant_system"
STAGE01_STATUS_PATH = "daily_candidates_status.json"
PENDING_STAGE_STATUS = {"NON_TRADING_DAY", "WAITING_MARKET_DATA", "DATA_STALE"}

try:
    import generate_market_signal_snapshot
except ImportError:  # pragma: no cover - defensive branch
    generate_market_signal_snapshot = None


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


def _pick_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    lower_map = {str(col).strip().lower(): col for col in df.columns}
    for candidate in candidates:
        hit = lower_map.get(candidate.strip().lower())
        if hit is not None:
            return hit
    return None


def _normalize_code(code: Any) -> str:
    text = str(code).strip().lower()
    if "." in text:
        left, right = text.split(".", 1)
        if left in {"sh", "sz", "bj"}:
            return f"{left}.{right}"
        if right in {"sh", "sz", "bj"}:
            return f"{right}.{left}"

    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) != 6:
        return text
    if digits.startswith(("600", "601", "603", "605", "688", "689")):
        return f"sh.{digits}"
    if digits.startswith(
        (
            "430",
            "831",
            "832",
            "833",
            "834",
            "835",
            "836",
            "837",
            "838",
            "839",
            "870",
            "871",
            "872",
            "873",
            "874",
            "875",
            "876",
            "877",
            "878",
            "879",
            "920",
        )
    ):
        return f"bj.{digits}"
    return f"sz.{digits}"


def _safe_float(value: Any) -> float | None:
    try:
        out = float(value)
        if pd.isna(out):
            return None
        return out
    except (TypeError, ValueError):
        return None


def _is_st_flag(row: pd.Series) -> bool:
    for column_name in ("is_st", "st_flag"):
        if column_name in row.index:
            value = row.get(column_name)
            if isinstance(value, bool):
                return value
            text = str(value).strip().lower()
            if text in {"1", "true", "yes", "y"}:
                return True
            if text in {"0", "false", "no", "n"}:
                return False

    name = str(row.get("name", "") or "").upper().replace(" ", "")
    return "ST" in name


def _is_paused_flag(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    return text in {"1", "true", "yes", "y"}


def _resolve_source_path(row: pd.Series, base_dir: Path) -> Path | None:
    source_file = str(row.get("source_file", "") or "").strip()
    if source_file:
        path = Path(source_file)
        if not path.is_absolute():
            path = (base_dir / path).resolve()
        if path.exists():
            return path

    file_name = str(row.get("file_name", "") or "").strip()
    if file_name:
        path = base_dir / "stock_data_5years" / file_name
        if path.exists():
            return path

    code = _normalize_code(row.get("code", ""))
    if code:
        path = base_dir / "stock_data_5years" / f"{code}.csv"
        if path.exists():
            return path
    return None


@lru_cache(maxsize=16384)
def _read_source_csv(path_str: str) -> pd.DataFrame:
    path = Path(path_str)
    try:
        return pd.read_csv(path, encoding="utf-8-sig")
    except UnicodeDecodeError:
        return pd.read_csv(path, encoding="gbk")


def _extract_max_date(df: pd.DataFrame, candidates: list[str]) -> str:
    date_col = _pick_col(df, candidates)
    if not date_col or date_col not in df.columns:
        return ""

    parsed = pd.to_datetime(df[date_col], errors="coerce")
    if parsed.isna().all():
        return ""
    return str(parsed.max().strftime("%Y-%m-%d"))


def _classify_data_pending_status(trading_date: str, latest_available_date: str) -> tuple[str, str]:
    trading_ts = pd.to_datetime(trading_date, errors="coerce")
    latest_ts = pd.to_datetime(latest_available_date, errors="coerce") if latest_available_date else pd.NaT

    if pd.notna(trading_ts) and int(trading_ts.weekday()) >= 5:
        latest_text = latest_available_date or "未知"
        return "NON_TRADING_DAY", f"目标日期 {trading_date} 为非交易日，当前最新可用行情日期为 {latest_text}。"

    if pd.isna(trading_ts):
        return "WAITING_MARKET_DATA", f"目标日期 {trading_date} 无法解析，当前无法确认当日行情是否到齐。"

    if pd.isna(latest_ts):
        return "WAITING_MARKET_DATA", f"目标日期 {trading_date} 的行情尚未到齐，当前无法识别最新有效行情日期。"

    gap_days = max(int((trading_ts - latest_ts).days), 0)
    if gap_days <= 1:
        return "WAITING_MARKET_DATA", f"目标日期 {trading_date} 的行情尚未到齐，当前最新可用行情日期为 {latest_available_date}。"
    return "DATA_STALE", f"目标日期 {trading_date} 的行情明显滞后，当前最新可用行情日期仅到 {latest_available_date}。"


def _write_stage01_status(reports_dir: Path, payload: Dict[str, Any]) -> None:
    with open(reports_dir / STAGE01_STATUS_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def _write_summary(
    path: Path,
    trading_date: str,
    source_path: Path,
    amount_min: float,
    vol_shrink_ratio: float,
    merged_count: int,
    selected_count: int,
    rejected_count: int,
    stage_status: str = "",
    note: str = "",
    latest_available_date: str = "",
) -> None:
    lines = [
        "============================================================",
        "每日候选股生成摘要",
        f"目标交易日: {trading_date}",
        f"上游输入文件: {source_path}",
        f"候选底池总数: {merged_count}",
        f"入选总数: {selected_count}",
        f"剔除总数: {rejected_count}",
        "候选模式: Route A 强势股缩量回踩",
        f"最低成交额门槛: {amount_min:.0f}",
        f"缩量阈值比例: {vol_shrink_ratio:.2f}",
        "过滤顺序: 非ST -> 非停牌 -> 流动性 -> RPS50可计算 -> RouteA 触发",
        "排序规则: rps50 降序, ma20_bias 升序",
    ]
    if stage_status:
        lines.append(f"阶段状态: {stage_status}")
    if latest_available_date:
        lines.append(f"最新有效行情日期: {latest_available_date}")
    if note:
        lines.append(f"说明: {note}")
    lines.append("============================================================")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_pending_outputs(
    reports_dir: Path,
    trading_date: str,
    source_path: Path,
    amount_min: float,
    vol_shrink_ratio: float,
    merged_count: int,
    stage_status: str,
    reason: str,
    latest_available_date: str,
    snapshot_quality: str = "",
    rejected_count: int | None = None,
) -> Dict[str, Any]:
    pd.DataFrame().to_csv(reports_dir / "daily_candidates_all.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame().to_csv(reports_dir / "daily_candidates_top20.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(
        [
            {
                "trading_date": trading_date,
                "code": "",
                "name": "",
                "source_file": str(source_path),
                "snapshot_quality": snapshot_quality,
                "source_trade_date": latest_available_date,
                "paused": snapshot_quality == "REPLAY_PROXY",
                "is_st": False,
                "reject_reason": stage_status,
                "reject_stage": "DATA_WINDOW",
            }
        ]
    ).to_csv(reports_dir / "daily_candidates_errors.csv", index=False, encoding="utf-8-sig")
    _write_summary(
        reports_dir / "daily_candidates_summary.txt",
        trading_date=trading_date,
        source_path=source_path,
        amount_min=amount_min,
        vol_shrink_ratio=vol_shrink_ratio,
        merged_count=merged_count,
        selected_count=0,
        rejected_count=int(rejected_count if rejected_count is not None else merged_count),
        stage_status=stage_status,
        note=reason,
        latest_available_date=latest_available_date,
    )
    payload = {
        "stage_status": stage_status,
        "success": False,
        "trading_date": trading_date,
        "error": reason,
        "latest_available_date": latest_available_date,
        "source_path": str(source_path),
    }
    _write_stage01_status(reports_dir, payload)
    return payload


def _extract_route_a_features(row: pd.Series, trading_date: str, base_dir: Path) -> tuple[dict[str, Any] | None, str]:
    source_path = _resolve_source_path(row, base_dir)
    if source_path is None:
        return None, "SOURCE_FILE_NOT_FOUND"

    try:
        source_df = _read_source_csv(str(source_path)).copy()
    except Exception as exc:
        return None, f"SOURCE_FILE_READ_ERROR:{exc}"

    date_col = _pick_col(source_df, ["date", "trade_date", "trading_date", "datetime", "dt"])
    close_col = _pick_col(source_df, ["close", "close_price", "latest_price"])
    volume_col = _pick_col(source_df, ["volume", "vol"])
    amount_col = _pick_col(source_df, ["amount", "turnover", "turnover_amount"])
    if not date_col or not close_col or not volume_col or not amount_col:
        return None, "MISSING_PRICE_COLUMNS"

    source_df[date_col] = pd.to_datetime(source_df[date_col], errors="coerce")
    source_df = source_df[source_df[date_col].notna()].copy()
    if source_df.empty:
        return None, "EMPTY_SOURCE_DATA"

    source_df = source_df.sort_values(date_col).reset_index(drop=True)
    source_df["trade_date"] = source_df[date_col].dt.strftime("%Y-%m-%d")
    source_df = source_df[source_df["trade_date"] <= trading_date].copy()
    if source_df.empty:
        return None, "NO_HISTORY_BEFORE_TRADING_DATE"

    exact_df = source_df[source_df["trade_date"] == trading_date].copy()
    if exact_df.empty:
        return None, "NO_EXACT_TRADING_DATE_BAR"

    source_df["close_num"] = pd.to_numeric(source_df[close_col], errors="coerce")
    source_df["volume_num"] = pd.to_numeric(source_df[volume_col], errors="coerce")
    source_df["amount_num"] = pd.to_numeric(source_df[amount_col], errors="coerce")
    source_df["ma5"] = source_df["close_num"].rolling(5, min_periods=5).mean()
    source_df["ma20"] = source_df["close_num"].rolling(20, min_periods=20).mean()
    source_df["vol_ma5"] = source_df["volume_num"].rolling(5, min_periods=5).mean()
    source_df["close_shift_50"] = source_df["close_num"].shift(50)

    feature_row = source_df.iloc[-1]
    close_value = _safe_float(feature_row["close_num"])
    amount_value = _safe_float(feature_row["amount_num"])
    ma5_value = _safe_float(feature_row["ma5"])
    ma20_value = _safe_float(feature_row["ma20"])
    vol_ma5_value = _safe_float(feature_row["vol_ma5"])
    volume_value = _safe_float(feature_row["volume_num"])
    close_shift_50_value = _safe_float(feature_row["close_shift_50"])

    if close_value is None or amount_value is None or ma5_value is None or ma20_value is None or vol_ma5_value is None or volume_value is None:
        return None, "INSUFFICIENT_MA_OR_VOLUME_HISTORY"
    if close_shift_50_value is None or close_shift_50_value <= 0:
        return None, "INSUFFICIENT_RPS50_HISTORY"

    ma20_bias = abs(close_value - ma20_value) / ma20_value if ma20_value > 0 else None
    close_return_50 = close_value / close_shift_50_value - 1.0
    return (
        {
            "feature_trade_date": str(feature_row["trade_date"]),
            "close_price": round(close_value, 4),
            "amount": float(amount_value),
            "turnover_amount": float(amount_value),
            "volume": float(volume_value),
            "ma5": round(ma5_value, 4),
            "ma20": round(ma20_value, 4),
            "vol_ma5": round(vol_ma5_value, 4),
            "ma20_bias": round(float(ma20_bias), 6) if ma20_bias is not None else None,
            "close_return_50": float(close_return_50),
        },
        "",
    )


def _build_candidate_reason(row: pd.Series, vol_shrink_ratio: float) -> str:
    return (
        f"RouteA 强势股缩量回踩: close={row['close_price']:.2f} > ma20={row['ma20']:.2f}, "
        f"close < ma5={row['ma5']:.2f}, volume={row['volume']:.0f} < vol_ma5*ratio={row['vol_ma5'] * vol_shrink_ratio:.0f}"
    )


def run(
    trading_date: str,
    base_dir: str = BASE_DIR,
    amount_min: float | None = None,
    vol_shrink_ratio: float | None = None,
) -> Dict[str, Any]:
    print(f"    --> [DEBUG] Stage 01 候选层开始，日期: {trading_date}")
    base_path = Path(base_dir)
    reports_dir = base_path / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    backtest_path = reports_dir / "batch_backtest_summary.csv"
    snapshot_path = reports_dir / "market_signal_snapshot.csv"

    amount_min = float(amount_min if amount_min is not None else _get_setting("DAILY_CANDIDATES_AMOUNT_MIN", 2e8))
    vol_shrink_ratio = float(
        vol_shrink_ratio if vol_shrink_ratio is not None else _get_setting("DAILY_CANDIDATES_VOL_SHRINK_RATIO", 0.8)
    )

    if generate_market_signal_snapshot and hasattr(generate_market_signal_snapshot, "run"):
        print("    --> [DEBUG] 正在拉起快照补数...")
        generate_market_signal_snapshot.run(
            trading_date=trading_date,
            base_dir=base_dir,
            candidate_path=str(backtest_path),
        )

    if not backtest_path.exists() or not snapshot_path.exists():
        payload = {
            "stage_status": "FAILED",
            "error": f"缺少输入文件。底池:{backtest_path.exists()} 快照:{snapshot_path.exists()}",
        }
        _write_stage01_status(reports_dir, payload)
        return payload

    df_backtest = pd.read_csv(backtest_path, encoding="utf-8-sig")
    df_snapshot = pd.read_csv(snapshot_path, encoding="utf-8-sig")
    print(f"    --> [DEBUG] 成功加载回测摘要: {len(df_backtest)} 行")
    print(f"    --> [DEBUG] 成功加载行情快照: {len(df_snapshot)} 行")

    backtest_max_date = _extract_max_date(df_backtest, ["end_date", "trade_date", "date"])
    snapshot_max_date = _extract_max_date(df_snapshot, ["source_trade_date", "trade_date", "date"])
    latest_available_date = max([x for x in (backtest_max_date, snapshot_max_date) if x], default="")
    all_snapshot_replay_proxy = bool(
        not df_snapshot.empty
        and "snapshot_quality" in df_snapshot.columns
        and df_snapshot["snapshot_quality"].fillna("").astype(str).str.upper().eq("REPLAY_PROXY").all()
    )

    trading_ts = pd.to_datetime(trading_date, errors="coerce")
    latest_ts = pd.to_datetime(latest_available_date, errors="coerce") if latest_available_date else pd.NaT
    if (pd.notna(trading_ts) and pd.notna(latest_ts) and latest_ts < trading_ts) or all_snapshot_replay_proxy:
        stage_status, reason = _classify_data_pending_status(trading_date, latest_available_date)
        return _write_pending_outputs(
            reports_dir=reports_dir,
            trading_date=trading_date,
            source_path=backtest_path,
            amount_min=amount_min,
            vol_shrink_ratio=vol_shrink_ratio,
            merged_count=len(df_backtest),
            stage_status=stage_status,
            reason=reason,
            latest_available_date=latest_available_date,
            snapshot_quality="REPLAY_PROXY" if all_snapshot_replay_proxy else "",
        )

    df_backtest["code"] = df_backtest["code"].map(_normalize_code)
    df_snapshot["code"] = df_snapshot["code"].map(_normalize_code)
    df = pd.merge(df_backtest, df_snapshot, on="code", how="inner")
    print(f"    --> [DEBUG] Inner Merge 匹配到的股票: {len(df)} 行")
    if df.empty:
        payload = {
            "stage_status": "FAILED",
            "error": "快照与底池合并后，匹配结果为 0 行，请检查 code 字段格式是否一致。",
        }
        _write_stage01_status(reports_dir, payload)
        return payload

    if "name" in df.columns:
        df["name"] = df["name"].fillna("").astype(str)
    else:
        df["name"] = pd.Series([""] * len(df), index=df.index, dtype="object")
    df["is_st"] = df.apply(_is_st_flag, axis=1)
    if "paused" in df.columns:
        df["paused"] = df["paused"].apply(_is_paused_flag)
    else:
        df["paused"] = False

    feature_records: list[dict[str, Any]] = []
    error_records: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        feature_payload, feature_error = _extract_route_a_features(row=row, trading_date=trading_date, base_dir=base_path)
        base_record = {
            "trading_date": trading_date,
            "code": row.get("code", ""),
            "name": row.get("name", ""),
            "source_file": row.get("source_file", ""),
            "snapshot_quality": row.get("snapshot_quality", ""),
            "source_trade_date": row.get("source_trade_date", ""),
            "paused": row.get("paused", False),
            "is_st": row.get("is_st", False),
        }
        if feature_payload is None:
            error_records.append({**base_record, "reject_reason": feature_error, "reject_stage": "FEATURE_BUILD"})
            continue
        feature_records.append({**base_record, **feature_payload})

    feature_df = pd.DataFrame(feature_records)
    if feature_df.empty:
        error_df = pd.DataFrame(error_records)
        error_df.to_csv(reports_dir / "daily_candidates_errors.csv", index=False, encoding="utf-8-sig")
        pd.DataFrame().to_csv(reports_dir / "daily_candidates_all.csv", index=False, encoding="utf-8-sig")
        pd.DataFrame().to_csv(reports_dir / "daily_candidates_top20.csv", index=False, encoding="utf-8-sig")

        reason_set = {
            str(value).strip()
            for value in error_df.get("reject_reason", pd.Series(dtype=object)).tolist()
            if str(value).strip()
        }
        if reason_set == {"NO_EXACT_TRADING_DATE_BAR"}:
            stage_status, reason = _classify_data_pending_status(trading_date, latest_available_date)
            return _write_pending_outputs(
                reports_dir=reports_dir,
                trading_date=trading_date,
                source_path=backtest_path,
                amount_min=amount_min,
                vol_shrink_ratio=vol_shrink_ratio,
                merged_count=len(df),
                stage_status=stage_status,
                reason=reason,
                latest_available_date=latest_available_date,
                snapshot_quality="REPLAY_PROXY" if all_snapshot_replay_proxy else "",
                rejected_count=len(error_df),
            )

        _write_summary(
            reports_dir / "daily_candidates_summary.txt",
            trading_date=trading_date,
            source_path=backtest_path,
            amount_min=amount_min,
            vol_shrink_ratio=vol_shrink_ratio,
            merged_count=len(df),
            selected_count=0,
            rejected_count=len(error_df),
        )
        payload = {"stage_status": "FAILED", "error": "候选特征构建失败，未形成有效 Route A 候选。"}
        _write_stage01_status(reports_dir, payload)
        return payload

    df_feature = df.merge(feature_df, on=["code"], how="inner", suffixes=("", "_feature"))
    for col in (
        "name_feature",
        "source_file_feature",
        "snapshot_quality_feature",
        "source_trade_date_feature",
        "paused_feature",
        "is_st_feature",
    ):
        if col in df_feature.columns:
            base_col = col.replace("_feature", "")
            df_feature[base_col] = df_feature[col]
            df_feature = df_feature.drop(columns=[col])

    returns_rank = df_feature["close_return_50"].rank(pct=True, method="average")
    df_feature["rps50"] = returns_rank.round(6)

    reject_records: list[dict[str, Any]] = error_records.copy()
    selected_df = df_feature.copy()
    masks = {
        "ST_SECURITY_FILTERED": selected_df["is_st"].astype(bool),
        "PAUSED_SECURITY_FILTERED": selected_df["paused"].astype(bool),
        "AMOUNT_BELOW_MIN": pd.to_numeric(selected_df["amount"], errors="coerce").fillna(0.0) < amount_min,
        "ROUTE_A_CONDITION_FAILED": ~(
            (pd.to_numeric(selected_df["close_price"], errors="coerce") > pd.to_numeric(selected_df["ma20"], errors="coerce"))
            & (pd.to_numeric(selected_df["close_price"], errors="coerce") < pd.to_numeric(selected_df["ma5"], errors="coerce"))
            & (
                pd.to_numeric(selected_df["volume"], errors="coerce")
                < pd.to_numeric(selected_df["vol_ma5"], errors="coerce") * float(vol_shrink_ratio)
            )
        ),
    }

    for reason, mask in masks.items():
        aligned_mask = mask.reindex(selected_df.index, fill_value=False)
        rejected = selected_df[aligned_mask].copy()
        if not rejected.empty:
            reject_records.extend(
                rejected.assign(reject_reason=reason, reject_stage="FILTER")[
                    [
                        "trading_date",
                        "code",
                        "name",
                        "source_file",
                        "snapshot_quality",
                        "source_trade_date",
                        "paused",
                        "is_st",
                        "reject_reason",
                        "reject_stage",
                    ]
                ].to_dict(orient="records")
            )
        selected_df = selected_df[~aligned_mask].copy()

    if selected_df.empty:
        error_df = pd.DataFrame(reject_records)
        error_df.to_csv(reports_dir / "daily_candidates_errors.csv", index=False, encoding="utf-8-sig")
        pd.DataFrame().to_csv(reports_dir / "daily_candidates_all.csv", index=False, encoding="utf-8-sig")
        pd.DataFrame().to_csv(reports_dir / "daily_candidates_top20.csv", index=False, encoding="utf-8-sig")
        _write_summary(
            reports_dir / "daily_candidates_summary.txt",
            trading_date=trading_date,
            source_path=backtest_path,
            amount_min=amount_min,
            vol_shrink_ratio=vol_shrink_ratio,
            merged_count=len(df),
            selected_count=0,
            rejected_count=len(error_df),
        )
        payload = {"stage_status": "FAILED", "error": "所有标的均未通过 Route A 候选过滤。"}
        _write_stage01_status(reports_dir, payload)
        return payload

    selected_df["ma20_bias"] = pd.to_numeric(selected_df["ma20_bias"], errors="coerce").fillna(999.0)
    selected_df["route_a_signal"] = True
    selected_df["candidate_reason"] = selected_df.apply(lambda row: _build_candidate_reason(row, vol_shrink_ratio), axis=1)
    selected_df = selected_df.sort_values(["rps50", "ma20_bias"], ascending=[False, True]).reset_index(drop=True)
    selected_df["rank"] = range(1, len(selected_df) + 1)
    selected_df["score"] = (
        (pd.to_numeric(selected_df["rps50"], errors="coerce").fillna(0.0) * 100.0)
        - (pd.to_numeric(selected_df["ma20_bias"], errors="coerce").fillna(0.0) * 100.0)
    ).round(2)
    selected_df["score"] = selected_df["score"].clip(lower=0.0)
    selected_df["heat_level"] = "正常"
    selected_df["action"] = "正常跟踪"

    error_df = pd.DataFrame(reject_records)
    selected_df.to_csv(reports_dir / "daily_candidates_all.csv", index=False, encoding="utf-8-sig")
    selected_df.head(20).to_csv(reports_dir / "daily_candidates_top20.csv", index=False, encoding="utf-8-sig")
    error_df.to_csv(reports_dir / "daily_candidates_errors.csv", index=False, encoding="utf-8-sig")
    _write_summary(
        reports_dir / "daily_candidates_summary.txt",
        trading_date=trading_date,
        source_path=backtest_path,
        amount_min=amount_min,
        vol_shrink_ratio=vol_shrink_ratio,
        merged_count=len(df),
        selected_count=len(selected_df),
        rejected_count=len(error_df),
    )

    print(f"    --> [DEBUG] Route A 最终候选数: {len(selected_df)}")
    print(f"    --> [DEBUG] 候选错误/剔除数: {len(error_df)}")
    payload = {
        "stage_status": "SUCCESS_EXECUTED",
        "success": True,
        "trading_date": trading_date,
        "candidate_count": int(len(selected_df)),
        "rejected_count": int(len(error_df)),
        "amount_min": float(amount_min),
        "vol_shrink_ratio": float(vol_shrink_ratio),
    }
    _write_stage01_status(reports_dir, payload)
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description="Route A 候选股生成层")
    parser.add_argument("--trading-date", required=True)
    parser.add_argument("--base-dir", default=BASE_DIR)
    parser.add_argument("--amount-min", type=float, default=None)
    parser.add_argument("--vol-shrink-ratio", type=float, default=None)
    args, _ = parser.parse_known_args()

    res = run(
        trading_date=args.trading_date,
        base_dir=args.base_dir,
        amount_min=args.amount_min,
        vol_shrink_ratio=args.vol_shrink_ratio,
    )
    if res.get("stage_status") == "FAILED":
        print(f"ERROR: {res.get('error')}")
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
