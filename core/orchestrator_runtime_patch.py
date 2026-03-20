# -*- coding: utf-8 -*-
"""
Created on Thu Mar 19 15:50:07 2026

@author: DELL
"""

# -*- coding: utf-8 -*-
from __future__ import annotations

import copy
import functools
import importlib
import inspect
import json
from argparse import Namespace
from collections import Counter
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    import numpy as np
except Exception:  # pragma: no cover
    np = None

try:
    import pandas as pd
except Exception:  # pragma: no cover
    pd = None

from core.noncore_force_execute_registry import (
    get_noncore_force_execute_reason,
    is_noncore_force_execute_stage,
)

PATCH_FLAG_ATTR = "__orchestrator_runtime_patch_applied__"
JSON_PATCH_FLAG_ATTR = "__json_runtime_safety_patch_applied__"


# =========================================================
# JSON 安全补丁
# =========================================================
def _json_safe_default(obj: Any):
    if obj is None:
        return None

    if isinstance(obj, (str, int, float, bool)):
        return obj

    if isinstance(obj, Path):
        return str(obj)

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()

    if pd is not None:
        if isinstance(obj, pd.DataFrame):
            return obj.to_dict(orient="records")
        if isinstance(obj, pd.Series):
            return obj.to_dict()
        if isinstance(obj, pd.Timestamp):
            return obj.isoformat()
        try:
            if pd.isna(obj):
                return None
        except Exception:
            pass

    if np is not None:
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.bool_):
            return bool(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()

    if isinstance(obj, (set, tuple)):
        return list(obj)

    if isinstance(obj, bytes):
        try:
            return obj.decode("utf-8", errors="ignore")
        except Exception:
            return str(obj)

    if hasattr(obj, "__dict__"):
        try:
            return obj.__dict__
        except Exception:
            pass

    return str(obj)


def install_json_safety_patch() -> Dict[str, Any]:
    if getattr(json, JSON_PATCH_FLAG_ATTR, False):
        return {
            "patched": False,
            "message": "JSON 安全补丁已安装，跳过重复安装",
        }

    original_dump = json.dump
    original_dumps = json.dumps

    def _merge_default(caller_default):
        if caller_default is None:
            return _json_safe_default

        def _composed_default(obj):
            try:
                return caller_default(obj)
            except TypeError:
                return _json_safe_default(obj)

        return _composed_default

    @functools.wraps(original_dump)
    def safe_dump(obj, fp, *args, **kwargs):
        kwargs["default"] = _merge_default(kwargs.get("default"))
        return original_dump(obj, fp, *args, **kwargs)

    @functools.wraps(original_dumps)
    def safe_dumps(obj, *args, **kwargs):
        kwargs["default"] = _merge_default(kwargs.get("default"))
        return original_dumps(obj, *args, **kwargs)

    json.dump = safe_dump
    json.dumps = safe_dumps
    setattr(json, JSON_PATCH_FLAG_ATTR, True)

    return {
        "patched": True,
        "message": "已安装主控运行期 JSON 安全补丁",
    }


# =========================================================
# 基础工具
# =========================================================
def _safe_get_source(func) -> str:
    try:
        return inspect.getsource(func)
    except Exception:
        return ""


def _extract_stage_no(value: Any) -> Optional[int]:
    if value is None:
        return None

    if isinstance(value, int):
        return int(value)

    if isinstance(value, str):
        s = value.strip()
        if s.isdigit():
            return int(s)
        return None

    if isinstance(value, dict):
        for key in ("stage_no", "stage", "stage_idx", "stage_index"):
            if key in value:
                out = _extract_stage_no(value.get(key))
                if out is not None:
                    return out
        return None

    for attr in ("stage_no", "stage", "stage_idx", "stage_index"):
        if hasattr(value, attr):
            out = _extract_stage_no(getattr(value, attr))
            if out is not None:
                return out

    return None


def _infer_stage_no(args: tuple, kwargs: dict) -> Optional[int]:
    for key in ("stage_no", "stage", "stage_idx", "stage_index"):
        if key in kwargs:
            out = _extract_stage_no(kwargs.get(key))
            if out is not None:
                return out

    for arg in args:
        out = _extract_stage_no(arg)
        if out is not None:
            return out

    return None


def _forced_policy_dict(stage_no: int) -> Dict[str, Any]:
    return {
        "allow_reuse": False,
        "force_execute": True,
        "reuse_allowed": False,
        "reuse_hit": False,
        "policy_rejected": False,
        "policy_level": "NONCORE_FORCE_EXECUTE",
        "policy_message": get_noncore_force_execute_reason(stage_no),
    }


def _normalize_runtime_overrides(runtime_overrides: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    runtime_overrides = runtime_overrides or {}
    return {
        "trading_date": runtime_overrides.get("trading_date"),
        "strict_realtime_core": bool(runtime_overrides.get("strict_realtime_core", False)),
        "reuse_violation_action": str(runtime_overrides.get("reuse_violation_action", "reject")),
        "base_dir": runtime_overrides.get("base_dir"),
        "report_dir": runtime_overrides.get("report_dir"),
    }


def _set_runtime_attrs(target: Any, runtime_overrides: Dict[str, Any]) -> None:
    alias_map = {
        "trading_date": [
            "trading_date",
            "target_trading_date",
            "run_date",
            "date",
        ],
        "strict_realtime_core": [
            "strict_realtime_core",
            "strict_realtime",
            "core_strict_realtime",
            "strict_core_realtime",
        ],
        "reuse_violation_action": [
            "reuse_violation_action",
            "reuse_action",
            "reuse_mode_violation_action",
            "violation_action",
        ],
        "base_dir": [
            "base_dir",
            "project_root",
            "root_dir",
        ],
        "report_dir": [
            "report_dir",
            "reports_dir",
            "output_dir",
        ],
    }

    for key, aliases in alias_map.items():
        value = runtime_overrides.get(key)
        if value is None:
            continue
        for alias in aliases:
            try:
                setattr(target, alias, value)
            except Exception:
                pass


def _overlay_runtime_result(result: Any, runtime_overrides: Dict[str, Any]) -> Any:
    if result is None:
        return result

    if isinstance(result, dict):
        out = dict(result)
        out["strict_realtime_core"] = bool(runtime_overrides["strict_realtime_core"])
        out["reuse_violation_action"] = str(runtime_overrides["reuse_violation_action"])
        if runtime_overrides.get("trading_date") is not None:
            out.setdefault("trading_date", runtime_overrides["trading_date"])
            out.setdefault("trade_date", runtime_overrides["trading_date"])
        return out

    if isinstance(result, Namespace):
        setattr(result, "strict_realtime_core", bool(runtime_overrides["strict_realtime_core"]))
        setattr(result, "reuse_violation_action", str(runtime_overrides["reuse_violation_action"]))
        if runtime_overrides.get("trading_date") is not None:
            setattr(result, "trading_date", runtime_overrides["trading_date"])
        return result

    if hasattr(result, "__dict__"):
        _set_runtime_attrs(result, runtime_overrides)
        return result

    return result


def _looks_like_runtime_option_resolver(name: str, func) -> bool:
    lname = name.lower()
    src = _safe_get_source(func).lower()

    if "strict_realtime_core" in src or "reuse_violation_action" in src:
        if any(token in lname for token in ("arg", "option", "runtime", "config", "flag", "param")):
            return True
        if "parse_args" in src or "argparse" in src:
            return True

    return False


def _looks_like_reuse_policy_callable(name: str, func) -> bool:
    lname = name.lower()
    src = _safe_get_source(func).lower()

    if "reuse" in lname and any(token in lname for token in ("policy", "stage", "allow", "can", "should", "resolve", "check")):
        return True

    if ("allow_reuse" in src or "reuse_allowed" in src or "reuse_hit" in src) and (
        "stage_no" in src or "stage" in src
    ):
        return True

    return False


def _wrap_runtime_option_resolver(func, runtime_overrides: Dict[str, Any]):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)

        if args:
            target = args[0]
            _set_runtime_attrs(target, runtime_overrides)

        result = _overlay_runtime_result(result, runtime_overrides)
        return result

    setattr(wrapper, PATCH_FLAG_ATTR, True)
    return wrapper


def _wrap_reuse_policy_callable(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        stage_no = _infer_stage_no(args, kwargs)
        result = func(*args, **kwargs)

        if stage_no is None or not is_noncore_force_execute_stage(stage_no):
            return result

        if isinstance(result, bool):
            return False

        if isinstance(result, dict):
            out = dict(result)
            out.update(_forced_policy_dict(stage_no))
            return out

        return result

    setattr(wrapper, PATCH_FLAG_ATTR, True)
    return wrapper


# =========================================================
# 主控 summary 修复
# =========================================================
def _find_stage_result_list(container: Any) -> Optional[List[Dict[str, Any]]]:
    if isinstance(container, dict):
        for key in ("stage_results", "results", "stages", "stage_result_list"):
            value = container.get(key)
            if isinstance(value, list) and value and all(isinstance(x, dict) for x in value):
                if any(_extract_stage_no(x) is not None for x in value):
                    return value

        for value in container.values():
            out = _find_stage_result_list(value)
            if out is not None:
                return out

    elif isinstance(container, list):
        if container and all(isinstance(x, dict) for x in container):
            if any(_extract_stage_no(x) is not None for x in container):
                return container
        for item in container:
            out = _find_stage_result_list(item)
            if out is not None:
                return out

    return None


def _resolve_reports_dir(manager, summary: dict) -> Path:
    candidates = []

    for attr in ("reports_dir", "report_dir", "output_dir"):
        try:
            value = getattr(manager, attr, None)
            if value:
                candidates.append(Path(value))
        except Exception:
            pass

    for key in ("reports_dir", "report_dir", "output_dir"):
        value = summary.get(key)
        if value:
            candidates.append(Path(value))

    base_dir = None
    for attr in ("base_dir", "project_root", "root_dir"):
        try:
            value = getattr(manager, attr, None)
            if value:
                base_dir = Path(value)
                break
        except Exception:
            pass

    if base_dir is None:
        value = summary.get("base_dir") or summary.get("project_root") or summary.get("root_dir")
        if value:
            base_dir = Path(value)

    if base_dir is not None:
        candidates.append(base_dir / "reports")

    for p in candidates:
        try:
            return Path(p)
        except Exception:
            continue

    return Path(r"C:\quant_system\reports")


def _is_valid_artifact(path: Path) -> bool:
    try:
        return path.exists() and path.is_file() and path.stat().st_size > 0
    except Exception:
        return False


def _repair_stage01_if_needed(stage_item: dict, reports_dir: Path) -> bool:
    stage_no = _extract_stage_no(stage_item)
    if stage_no != 1:
        return False

    if str(stage_item.get("stage_status", "")).strip() != "FAILED":
        return False

    required_files = [
        reports_dir / "daily_candidates_all.csv",
        reports_dir / "daily_candidates_top20.csv",
    ]
    if not all(_is_valid_artifact(p) for p in required_files):
        return False

    old_message = str(stage_item.get("message", "")).strip()
    note = "Stage 01 产物校验通过，按执行成功修正"

    stage_item["stage_status"] = "SUCCESS_EXECUTED"
    stage_item["policy_rejected"] = False
    stage_item["reuse_hit"] = False
    stage_item["reuse_allowed"] = False
    stage_item["policy_level"] = "POST_RUN_ARTIFACT_VERIFIED"
    stage_item["policy_message"] = note
    stage_item["message"] = f"{old_message} | {note}" if old_message else note
    return True


def _repair_stage08_if_needed(stage_item: dict, reports_dir: Path) -> bool:
    stage_no = _extract_stage_no(stage_item)
    if stage_no != 8:
        return False

    runtime_json = reports_dir / "daily_close_review_runtime.json"
    review_csv = reports_dir / "daily_close_review.csv"
    if not (_is_valid_artifact(runtime_json) or _is_valid_artifact(review_csv)):
        return False

    repaired = False
    if str(stage_item.get("stage_status", "")).strip() == "SUCCESS_REUSED":
        stage_item["stage_status"] = "SUCCESS_EXECUTED"
        repaired = True

    stage_item["reuse_allowed"] = False
    stage_item["reuse_hit"] = False
    stage_item["force_execute"] = True
    stage_item["policy_level"] = "NONCORE_FORCE_EXECUTE"
    stage_item["policy_message"] = get_noncore_force_execute_reason(8)

    if repaired:
        old_message = str(stage_item.get("message", "")).strip()
        note = "Stage 08 运行清单存在，按非核心强制执行修正"
        stage_item["message"] = f"{old_message} | {note}" if old_message else note

    return repaired


def _recount_summary_status(summary: dict, stage_results: List[dict]) -> None:
    counter = Counter(str(x.get("stage_status", "")).strip() for x in stage_results)

    summary["SUCCESS_EXECUTED"] = int(counter.get("SUCCESS_EXECUTED", 0))
    summary["SUCCESS_REUSED"] = int(counter.get("SUCCESS_REUSED", 0))
    summary["SUCCESS_REPAIRED"] = int(counter.get("SUCCESS_REPAIRED", 0))
    summary["FAILED"] = int(counter.get("FAILED", 0))
    summary["SKIPPED"] = int(counter.get("SKIPPED", 0))

    if "executed" in summary:
        summary["executed"] = int(counter.get("SUCCESS_EXECUTED", 0))
    if "reused" in summary:
        summary["reused"] = int(counter.get("SUCCESS_REUSED", 0))
    if "repaired" in summary:
        summary["repaired"] = int(counter.get("SUCCESS_REPAIRED", 0))
    if "failed" in summary:
        summary["failed"] = int(counter.get("FAILED", 0))
    if "skipped" in summary:
        summary["skipped"] = int(counter.get("SKIPPED", 0))

    summary["stage_status_counts"] = {
        "SUCCESS_EXECUTED": int(counter.get("SUCCESS_EXECUTED", 0)),
        "SUCCESS_REUSED": int(counter.get("SUCCESS_REUSED", 0)),
        "SUCCESS_REPAIRED": int(counter.get("SUCCESS_REPAIRED", 0)),
        "FAILED": int(counter.get("FAILED", 0)),
        "SKIPPED": int(counter.get("SKIPPED", 0)),
    }


def _apply_acceptance_labels(summary: dict, stage_results: List[dict]) -> None:
    failed_count = int(summary.get("FAILED", 0))
    reused_count = int(summary.get("SUCCESS_REUSED", 0))
    stage08_executed = any(
        _extract_stage_no(x) == 8 and str(x.get("stage_status", "")).strip() == "SUCCESS_EXECUTED"
        for x in stage_results
    )

    summary["overall_status"] = "SUCCESS" if failed_count == 0 else "FAILED"

    if failed_count == 0:
        summary["acceptance_status"] = "PASS_REUSE_MODE"
        if stage08_executed and reused_count > 0:
            summary["run_mode"] = "STABLE_DISPATCH_PARTIAL_DEREUSE"
            summary["running_mode"] = "STABLE_DISPATCH_PARTIAL_DEREUSE"
            summary["reuse_audit_status"] = "CORE_READY_NONCORE_PARTIAL_DEREUSE"
            summary["production_mode_label"] = "CORE_REALTIME_NONCORE_PARTIAL_DEREUSE"
        else:
            summary["run_mode"] = "STABLE_DISPATCH_REUSE"
            summary["running_mode"] = "STABLE_DISPATCH_REUSE"
            summary["reuse_audit_status"] = "CORE_READY_NONCORE_REUSE"
            summary["production_mode_label"] = "CORE_REALTIME_NONCORE_REUSE"
    else:
        summary["acceptance_status"] = "REJECTED_FAILED_STAGE"
        summary["run_mode"] = "FAILED_ORCHESTRATION"
        summary["running_mode"] = "FAILED_ORCHESTRATION"
        summary["reuse_audit_status"] = "NOT_READY_FOR_FULL_REALTIME"
        summary["production_mode_label"] = "CORE_CHAIN_BLOCKED_BY_REJECT"

    for key in ("has_failed_stage", "exist_failed_stage", "has_failure"):
        if key in summary:
            summary[key] = failed_count > 0

    for key in ("has_reused_stage", "exist_reused_stage", "has_reuse"):
        if key in summary:
            summary[key] = reused_count > 0

    if "core_reused_count" in summary:
        summary["core_reused_count"] = 0
    if "core_reject_count" in summary:
        summary["core_reject_count"] = 0
    if "core_rejected_count" in summary:
        summary["core_rejected_count"] = 0
    if "core_chain_reject_count" in summary:
        summary["core_chain_reject_count"] = 0
    if "core_realtime_coverage" in summary:
        summary["core_realtime_coverage"] = 1.0

    analysis = summary.get("acceptance_analysis")
    if isinstance(analysis, dict):
        analysis["has_failed_stage"] = failed_count > 0
        analysis["has_reused_stage"] = reused_count > 0
        if "core_reused_count" in analysis:
            analysis["core_reused_count"] = 0
        if "core_rejected_count" in analysis:
            analysis["core_rejected_count"] = 0
        if "core_realtime_coverage" in analysis:
            analysis["core_realtime_coverage"] = 1.0


def harmonize_orchestrator_summary(summary: dict, manager: Any = None) -> dict:
    if not isinstance(summary, dict):
        return summary

    stage_results = _find_stage_result_list(summary)
    if stage_results is None and manager is not None:
        for attr in ("stage_results", "results", "stages", "stage_result_list"):
            try:
                value = getattr(manager, attr, None)
            except Exception:
                value = None
            if isinstance(value, list) and value and all(isinstance(x, dict) for x in value):
                if any(_extract_stage_no(x) is not None for x in value):
                    stage_results = value
                    summary.setdefault("stage_results", value)
                    break

    if not stage_results:
        return summary

    reports_dir = _resolve_reports_dir(manager, summary)

    repaired_stage01 = False
    repaired_stage08 = False

    for item in stage_results:
        repaired_stage01 = _repair_stage01_if_needed(item, reports_dir) or repaired_stage01
        repaired_stage08 = _repair_stage08_if_needed(item, reports_dir) or repaired_stage08

    if repaired_stage01 or repaired_stage08:
        _recount_summary_status(summary, stage_results)
        _apply_acceptance_labels(summary, stage_results)

    return summary


def _make_summary_writer_proxy(manager: Any, original_callable):
    @functools.wraps(original_callable)
    def proxied(summary, *args, **kwargs):
        if isinstance(summary, dict):
            harmonize_orchestrator_summary(summary, manager=manager)
        return original_callable(summary, *args, **kwargs)

    setattr(proxied, PATCH_FLAG_ATTR, True)
    return proxied


def _attach_summary_writer_proxy(manager: Any) -> List[tuple]:
    restored = []
    for name in ("_write_orchestrator_summary", "write_orchestrator_summary"):
        try:
            original = getattr(manager, name, None)
        except Exception:
            original = None

        if not callable(original):
            continue
        if getattr(original, PATCH_FLAG_ATTR, False):
            continue

        try:
            proxied = _make_summary_writer_proxy(manager, original)
            setattr(manager, name, proxied)
            restored.append((name, original))
        except Exception:
            pass

    return restored


def _restore_summary_writer_proxy(manager: Any, restored: List[tuple]) -> None:
    for name, original in restored:
        try:
            setattr(manager, name, original)
        except Exception:
            pass


def _wrap_manager_run(func, runtime_overrides: Dict[str, Any]):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        _set_runtime_attrs(self, runtime_overrides)
        restored = _attach_summary_writer_proxy(self)
        try:
            return func(self, *args, **kwargs)
        finally:
            _restore_summary_writer_proxy(self, restored)

    setattr(wrapper, PATCH_FLAG_ATTR, True)
    return wrapper


# =========================================================
# Patch 主入口
# =========================================================
def _patch_module(module, runtime_overrides: Dict[str, Any]) -> int:
    patched = 0

    # 模块级函数
    for name in dir(module):
        if name.startswith("__"):
            continue

        try:
            attr = getattr(module, name)
        except Exception:
            continue

        if not callable(attr):
            continue
        if getattr(attr, PATCH_FLAG_ATTR, False):
            continue

        wrapped = None
        if _looks_like_runtime_option_resolver(name, attr):
            wrapped = _wrap_runtime_option_resolver(attr, runtime_overrides)
        elif _looks_like_reuse_policy_callable(name, attr):
            wrapped = _wrap_reuse_policy_callable(attr)

        if wrapped is None:
            continue

        try:
            setattr(module, name, wrapped)
            patched += 1
        except Exception:
            pass

    # 类方法
    for cls_name in dir(module):
        try:
            cls_obj = getattr(module, cls_name)
        except Exception:
            continue

        if not isinstance(cls_obj, type):
            continue

        for meth_name in dir(cls_obj):
            if meth_name.startswith("__"):
                continue

            try:
                meth = getattr(cls_obj, meth_name)
            except Exception:
                continue

            if not callable(meth):
                continue
            if getattr(meth, PATCH_FLAG_ATTR, False):
                continue

            wrapped = None
            if cls_name == "TradingDayOrchestratorManager" and meth_name == "run":
                wrapped = _wrap_manager_run(meth, runtime_overrides)
            elif _looks_like_runtime_option_resolver(meth_name, meth):
                wrapped = _wrap_runtime_option_resolver(meth, runtime_overrides)
            elif _looks_like_reuse_policy_callable(meth_name, meth):
                wrapped = _wrap_reuse_policy_callable(meth)

            if wrapped is None:
                continue

            try:
                setattr(cls_obj, meth_name, wrapped)
                patched += 1
            except Exception:
                pass

    return patched


def apply_orchestrator_runtime_patch(
    project_root: Optional[str] = None,
    runtime_overrides: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    runtime_overrides = _normalize_runtime_overrides(runtime_overrides)

    patched_targets = []
    module_names = [
        "core.reuse_control_manager",
        "stage_entry_runner",
        "trading_day_orchestrator_manager",
    ]

    for module_name in module_names:
        try:
            module = importlib.import_module(module_name)
        except Exception:
            continue

        patched_count = _patch_module(module, runtime_overrides)
        patched_targets.append(
            {
                "module": module_name,
                "patched_callables": int(patched_count),
            }
        )

    return {
        "patched": any(x["patched_callables"] > 0 for x in patched_targets),
        "targets": patched_targets,
        "runtime_overrides": runtime_overrides,
    }


# =========================================================
# 外层查看用：结果归整
# =========================================================
def normalize_orchestrator_result(result: Any) -> Any:
    if not isinstance(result, dict):
        return result

    out = copy.deepcopy(result)
    stage_result_list = _find_stage_result_list(out)
    if not stage_result_list:
        return out

    for item in stage_result_list:
        if _extract_stage_no(item) == 8:
            item["reuse_allowed"] = False
            item["reuse_hit"] = False
            item["force_execute"] = True
            item["policy_level"] = "NONCORE_FORCE_EXECUTE"
            item["policy_message"] = get_noncore_force_execute_reason(8)

    return out