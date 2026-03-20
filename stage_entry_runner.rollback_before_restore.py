# -*- coding: utf-8 -*-
"""
Created on Wed Mar 18 19:03:03 2026

@author: DELL
"""

# -*- coding: utf-8 -*-
from __future__ import annotations

import importlib
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, Optional

from core.stage_status import normalize_stage_status


def bootstrap_paths(base_dir: str = r"C:\quant_system") -> str:
    """
    兼容旧脚本：
    scripts.generate_daily_candidates / scripts.generate_trade_plan 等
    仍可能执行 from stage_entry_runner import bootstrap_paths
    """
    base_dir = str(base_dir)
    if base_dir not in sys.path:
        sys.path.insert(0, base_dir)
    return base_dir


class StageEntryRunner:
    """
    通用阶段入口执行器

    支持：
    - entry_type = function: "module.function"
    - entry_type = method  : "module.Class.method"
    - entry_type = callable: 直接传入 callable 对象
    """

    def __init__(self, base_dir: str = r"C:\quant_system") -> None:
        self.base_dir = bootstrap_paths(base_dir)

    def run(
        self,
        stage_no: int,
        stage_name: str,
        entry_type: str,
        entry_target: Any,
        entry_kwargs: Optional[Dict[str, Any]] = None,
        enabled: bool = True,
    ) -> Dict[str, Any]:
        started_at = datetime.now()
        start_ts = time.time()

        if not enabled:
            return {
                "stage_no": stage_no,
                "stage_name": stage_name,
                "entry_type": entry_type,
                "entry_target": self._safe_target_repr(entry_target),
                "started_at": started_at.strftime("%Y-%m-%d %H:%M:%S"),
                "ended_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "duration_sec": 0.0,
                "stage_status": "SKIPPED",
                "success": False,
                "skipped": True,
                "error_message": "",
            }

        entry_kwargs = dict(entry_kwargs or {})

        try:
            stage_callable = self._resolve_entry(entry_type, entry_target, entry_kwargs)
            raw_result = stage_callable(**entry_kwargs)
            result_dict = self._coerce_result_dict(raw_result)

            stage_status = normalize_stage_status(
                stage_result=result_dict,
                raw_status=result_dict.get("stage_status") or result_dict.get("status"),
            )

            final_result: Dict[str, Any] = {
                "stage_no": stage_no,
                "stage_name": stage_name,
                "entry_type": entry_type,
                "entry_target": self._safe_target_repr(entry_target),
                "started_at": started_at.strftime("%Y-%m-%d %H:%M:%S"),
                "ended_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "duration_sec": round(time.time() - start_ts, 4),
                "stage_status": stage_status,
                "success": stage_status.startswith("SUCCESS"),
                "error_message": "",
            }

            for key, value in result_dict.items():
                if key not in final_result:
                    final_result[key] = value

            return final_result

        except Exception as exc:
            return {
                "stage_no": stage_no,
                "stage_name": stage_name,
                "entry_type": entry_type,
                "entry_target": self._safe_target_repr(entry_target),
                "started_at": started_at.strftime("%Y-%m-%d %H:%M:%S"),
                "ended_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "duration_sec": round(time.time() - start_ts, 4),
                "stage_status": "FAILED",
                "success": False,
                "error_message": str(exc),
                "traceback": traceback.format_exc(),
            }

    def _resolve_entry(
        self,
        entry_type: str,
        entry_target: Any,
        entry_kwargs: Dict[str, Any],
    ) -> Callable[..., Any]:
        entry_type = str(entry_type).strip().lower()

        if entry_type == "callable":
            if not callable(entry_target):
                raise TypeError("entry_type=callable 时，entry_target 必须为可调用对象")
            return entry_target

        if not isinstance(entry_target, str) or "." not in entry_target:
            raise ValueError("entry_target 必须为点路径字符串，如 module.function / module.Class.method")

        if entry_type == "function":
            module_name, func_name = entry_target.rsplit(".", 1)
            module = importlib.import_module(module_name)
            func = getattr(module, func_name)
            if not callable(func):
                raise TypeError(f"目标函数不可调用: {entry_target}")
            return func

        if entry_type == "method":
            parts = entry_target.split(".")
            if len(parts) < 3:
                raise ValueError(f"method 入口格式错误: {entry_target}")

            module_name = ".".join(parts[:-2])
            class_name = parts[-2]
            method_name = parts[-1]

            module = importlib.import_module(module_name)
            cls = getattr(module, class_name)

            init_kwargs = entry_kwargs.pop("__init_kwargs__", {}) or {}
            instance = cls(**init_kwargs)
            method = getattr(instance, method_name)

            if not callable(method):
                raise TypeError(f"目标方法不可调用: {entry_target}")
            return method

        raise ValueError(f"不支持的 entry_type: {entry_type}")

    def _coerce_result_dict(self, raw_result: Any) -> Dict[str, Any]:
        if raw_result is None:
            return {}
        if isinstance(raw_result, dict):
            return raw_result
        if isinstance(raw_result, bool):
            return {"success": raw_result}
        if isinstance(raw_result, str):
            return {"message": raw_result}
        return {"result": raw_result}

    def _safe_target_repr(self, entry_target: Any) -> str:
        if isinstance(entry_target, str):
            return entry_target
        if callable(entry_target):
            return getattr(entry_target, "__name__", str(entry_target))
        return str(entry_target)


def run_stage(
    stage_no: int,
    stage_name: str,
    entry_type: str,
    entry_target: Any,
    entry_kwargs: Optional[Dict[str, Any]] = None,
    enabled: bool = True,
    base_dir: str = r"C:\quant_system",
) -> Dict[str, Any]:
    runner = StageEntryRunner(base_dir=base_dir)
    return runner.run(
        stage_no=stage_no,
        stage_name=stage_name,
        entry_type=entry_type,
        entry_target=entry_target,
        entry_kwargs=entry_kwargs,
        enabled=enabled,
    )