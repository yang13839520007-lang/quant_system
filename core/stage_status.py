# -*- coding: utf-8 -*-
"""
Created on Wed Mar 18 14:44:34 2026

@author: DELL
"""

# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional


SUCCESS_EXECUTED = "SUCCESS_EXECUTED"
SUCCESS_REUSED = "SUCCESS_REUSED"
SUCCESS_REPAIRED = "SUCCESS_REPAIRED"
FAILED = "FAILED"
SKIPPED = "SKIPPED"
NON_TRADING_DAY = "NON_TRADING_DAY"
WAITING_MARKET_DATA = "WAITING_MARKET_DATA"
DATA_STALE = "DATA_STALE"

SUCCESS_STATUS_SET = {
    SUCCESS_EXECUTED,
    SUCCESS_REUSED,
    SUCCESS_REPAIRED,
}

ALL_STAGE_STATUS_SET = {
    SUCCESS_EXECUTED,
    SUCCESS_REUSED,
    SUCCESS_REPAIRED,
    FAILED,
    SKIPPED,
    NON_TRADING_DAY,
    WAITING_MARKET_DATA,
    DATA_STALE,
}

DATA_PENDING_STATUS_SET = {
    NON_TRADING_DAY,
    WAITING_MARKET_DATA,
    DATA_STALE,
}


def _safe_upper(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip().upper()


def _collect_text_blob(stage_result: Optional[Dict[str, Any]]) -> str:
    if not isinstance(stage_result, dict):
        return ""

    text_parts: List[str] = []
    for _, value in stage_result.items():
        if value is None:
            continue
        if isinstance(value, (str, int, float, bool)):
            text_parts.append(str(value))
        elif isinstance(value, (list, tuple)):
            for item in value:
                if item is not None:
                    text_parts.append(str(item))
        elif isinstance(value, dict):
            for sub_v in value.values():
                if sub_v is not None:
                    text_parts.append(str(sub_v))
    return " | ".join(text_parts).upper()


def _contains_any(text: str, keywords: Iterable[str]) -> bool:
    if not text:
        return False
    return any(keyword.upper() in text for keyword in keywords)


def normalize_stage_status(
    stage_result: Optional[Dict[str, Any]] = None,
    raw_status: Optional[str] = None,
) -> str:
    """
    将历史 SUCCESS / OK / DONE 等统一升级为：
    - SUCCESS_EXECUTED
    - SUCCESS_REUSED
    - SUCCESS_REPAIRED
    - FAILED
    - SKIPPED
    - NON_TRADING_DAY
    - WAITING_MARKET_DATA
    - DATA_STALE
    """
    result = stage_result or {}
    status = _safe_upper(raw_status or result.get("stage_status") or result.get("status"))

    if status in ALL_STAGE_STATUS_SET:
        return status

    text_blob = _collect_text_blob(result)

    if result.get("error") or result.get("exception") or result.get("traceback"):
        return FAILED
    if result.get("success") is False or result.get("failed") is True:
        return FAILED

    if (
        result.get("skipped") is True
        or result.get("enabled") is False
        or status.startswith("SKIPPED")
        or status == "SKIP"
        or _contains_any(text_blob, ["跳过", "SKIPPED", "DISABLED"])
    ):
        return SKIPPED

    if (
        result.get("repaired") is True
        or result.get("auto_repair_applied") is True
        or bool(result.get("repair_actions"))
        or _contains_any(
            text_blob,
            [
                "SUCCESS_REPAIRED",
                "REPAIRED",
                "AUTO_REPAIR",
                "自动修正",
                "自动修复",
                "修正",
                "修复",
            ],
        )
    ):
        return SUCCESS_REPAIRED

    if (
        result.get("reused") is True
        or result.get("artifact_reused") is True
        or result.get("used_existing_artifact") is True
        or result.get("reuse_mode") is True
        or result.get("cache_hit") is True
        or _contains_any(
            text_blob,
            [
                "SUCCESS_REUSED",
                "REUSED",
                "REUSE",
                "复用既有工件",
                "复用工件",
                "工件复用",
                "USING EXISTING ARTIFACT",
                "EXISTING ARTIFACT",
                "CACHE HIT",
            ],
        )
    ):
        return SUCCESS_REUSED

    if status in {"SUCCESS", "OK", "DONE", "COMPLETED", "PASS"}:
        return SUCCESS_EXECUTED

    if status in {"FAILED", "ERROR", "EXCEPTION"}:
        return FAILED

    if status in {"SKIPPED", "SKIP", "DISABLED"}:
        return SKIPPED

    if "FAIL" in status or "ERROR" in status or "EXCEPTION" in status:
        return FAILED

    if "SKIP" in status:
        return SKIPPED

    return SUCCESS_EXECUTED


def is_success_stage(stage_status: str) -> bool:
    return _safe_upper(stage_status) in SUCCESS_STATUS_SET


def is_data_pending_stage(stage_status: str) -> bool:
    return _safe_upper(stage_status) in DATA_PENDING_STATUS_SET


def build_stage_status_counts(stage_results: List[Dict[str, Any]]) -> Dict[str, int]:
    counts = {
        SUCCESS_EXECUTED: 0,
        SUCCESS_REUSED: 0,
        SUCCESS_REPAIRED: 0,
        FAILED: 0,
        SKIPPED: 0,
        NON_TRADING_DAY: 0,
        WAITING_MARKET_DATA: 0,
        DATA_STALE: 0,
    }
    for row in stage_results:
        status = normalize_stage_status(row, row.get("stage_status"))
        counts[status] = counts.get(status, 0) + 1
    return counts


def derive_run_mode_label(stage_results: List[Dict[str, Any]]) -> str:
    """
    运行模式标识：
    - FAILED_ORCHESTRATION：存在失败阶段
    - STABLE_DISPATCH_REUSE：存在复用阶段，当前属于可稳定调度版
    - FULL_REALTIME_RECOMPUTE：无复用阶段，属于全阶段实时重算生产版
    """
    counts = build_stage_status_counts(stage_results)
    if counts.get(FAILED, 0) > 0:
        return "FAILED_ORCHESTRATION"
    if counts.get(NON_TRADING_DAY, 0) > 0:
        return NON_TRADING_DAY
    if counts.get(DATA_STALE, 0) > 0:
        return DATA_STALE
    if counts.get(WAITING_MARKET_DATA, 0) > 0:
        return WAITING_MARKET_DATA
    if counts.get(SUCCESS_REUSED, 0) > 0:
        return "STABLE_DISPATCH_REUSE"
    return "FULL_REALTIME_RECOMPUTE"
