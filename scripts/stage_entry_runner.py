# -*- coding: utf-8 -*-
"""
Created on Wed Mar 18 14:12:33 2026

@author: DELL
"""

import os
import sys
import importlib
import inspect
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


def bootstrap_paths(current_file: str) -> Tuple[Path, Path]:
    script_dir = Path(current_file).resolve().parent
    base_dir = script_dir.parent

    script_dir_str = str(script_dir)
    base_dir_str = str(base_dir)

    if script_dir_str not in sys.path:
        sys.path.insert(0, script_dir_str)
    if base_dir_str not in sys.path:
        sys.path.insert(0, base_dir_str)

    return script_dir, base_dir


def set_common_env(base_dir: Path, trade_date: str) -> None:
    os.environ["TARGET_TRADE_DATE"] = str(trade_date).strip()
    os.environ["QUANT_SYSTEM_BASE_DIR"] = str(base_dir)


def safe_public_names(module) -> List[str]:
    return [name for name in dir(module) if not name.startswith("_")]


def invalidate_and_import(module_names: Sequence[str]):
    importlib.invalidate_caches()
    last_error = None
    for module_name in module_names:
        try:
            if module_name in sys.modules:
                return importlib.reload(sys.modules[module_name]), module_name
            return importlib.import_module(module_name), module_name
        except Exception as exc:
            last_error = exc
    raise ImportError(f"未找到可用模块: {list(module_names)} | last_error={last_error}")


def discover_module_names(
    script_dir: Path,
    base_dir: Path,
    preferred_modules: Optional[Sequence[str]] = None,
    file_keywords: Optional[Sequence[str]] = None,
) -> List[str]:
    preferred_modules = list(preferred_modules or [])
    file_keywords = [str(x).lower() for x in (file_keywords or [])]

    results: List[str] = []
    seen = set()

    def _add(name: str) -> None:
        if not name or name in seen:
            return
        seen.add(name)
        results.append(name)

    for name in preferred_modules:
        _add(name)

    if file_keywords:
        for py_file in script_dir.glob("*.py"):
            stem = py_file.stem
            lowered = stem.lower()
            if all(keyword in lowered for keyword in file_keywords):
                _add(stem)

        core_dir = base_dir / "core"
        if core_dir.exists():
            for py_file in core_dir.glob("*.py"):
                stem = py_file.stem
                lowered = stem.lower()
                if all(keyword in lowered for keyword in file_keywords):
                    _add(f"core.{stem}")

    return results


def _required_missing_params(func, provided_keys: Sequence[str]) -> List[str]:
    sig = inspect.signature(func)
    missing = []

    for name, param in sig.parameters.items():
        if param.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
            continue
        if param.default is not inspect._empty:
            continue
        if name == "self":
            continue
        if name not in provided_keys:
            missing.append(name)

    return missing


def call_with_supported_kwargs(func, kwargs: Dict[str, Any]):
    sig = inspect.signature(func)
    params = sig.parameters

    if any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values()):
        return func(**{k: v for k, v in kwargs.items() if v is not None})

    accepted = {}
    for key, value in kwargs.items():
        if value is None:
            continue
        if key in params:
            accepted[key] = value
    return func(**accepted)


def instantiate_class_with_supported_kwargs(cls, kwargs: Dict[str, Any]):
    sig = inspect.signature(cls)
    params = sig.parameters

    if any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values()):
        return cls(**{k: v for k, v in kwargs.items() if v is not None})

    accepted = {}
    for key, value in kwargs.items():
        if value is None:
            continue
        if key in params:
            accepted[key] = value
    return cls(**accepted)


def _match_keyword_groups(name: str, keyword_groups: Sequence[Sequence[str]]) -> bool:
    lowered = name.lower()
    for group in keyword_groups:
        if all(keyword.lower() in lowered for keyword in group):
            return True
    return False


def resolve_entry(
    module,
    init_kwargs: Dict[str, Any],
    function_candidates: Sequence[str],
    class_name_candidates: Optional[Sequence[str]] = None,
    class_keywords: Optional[Sequence[str]] = None,
    method_candidates: Optional[Sequence[str]] = None,
    function_keyword_groups: Optional[Sequence[Sequence[str]]] = None,
):
    class_name_candidates = list(class_name_candidates or [])
    class_keywords = [str(x).lower() for x in (class_keywords or [])]
    method_candidates = list(method_candidates or ["run", "execute", "start", "generate", "build", "review", "check", "main"])
    function_keyword_groups = list(function_keyword_groups or [])

    provided_keys = list(init_kwargs.keys())
    deferred_candidates = []

    for name in function_candidates:
        obj = getattr(module, name, None)
        if callable(obj) and not inspect.isclass(obj):
            missing = _required_missing_params(obj, provided_keys)
            if not missing:
                return obj, f"{module.__name__}.{name}", "function"
            deferred_candidates.append((obj, f"{module.__name__}.{name}", missing))

    if function_keyword_groups:
        for public_name in safe_public_names(module):
            obj = getattr(module, public_name, None)
            if callable(obj) and not inspect.isclass(obj):
                if _match_keyword_groups(public_name, function_keyword_groups):
                    missing = _required_missing_params(obj, provided_keys)
                    if not missing:
                        return obj, f"{module.__name__}.{public_name}", "function"
                    deferred_candidates.append((obj, f"{module.__name__}.{public_name}", missing))

    for class_name in class_name_candidates:
        cls = getattr(module, class_name, None)
        if inspect.isclass(cls):
            try:
                instance = instantiate_class_with_supported_kwargs(cls, init_kwargs)
            except Exception:
                continue
            for method_name in method_candidates:
                method = getattr(instance, method_name, None)
                if callable(method):
                    missing = _required_missing_params(method, provided_keys)
                    if not missing:
                        return method, f"{module.__name__}.{class_name}.{method_name}", "method"
                    deferred_candidates.append((method, f"{module.__name__}.{class_name}.{method_name}", missing))

    if class_keywords:
        for public_name in safe_public_names(module):
            obj = getattr(module, public_name, None)
            if inspect.isclass(obj):
                lowered = public_name.lower()
                if any(keyword in lowered for keyword in class_keywords):
                    try:
                        instance = instantiate_class_with_supported_kwargs(obj, init_kwargs)
                    except Exception:
                        continue
                    for method_name in method_candidates:
                        method = getattr(instance, method_name, None)
                        if callable(method):
                            missing = _required_missing_params(method, provided_keys)
                            if not missing:
                                return method, f"{module.__name__}.{public_name}.{method_name}", "method"
                            deferred_candidates.append((method, f"{module.__name__}.{public_name}.{method_name}", missing))

    if deferred_candidates:
        deferred_candidates.sort(key=lambda x: len(x[2]))
        _, best_name, best_missing = deferred_candidates[0]
        raise AttributeError(
            f"找到近似入口但参数仍不完整: {best_name} | 缺失参数={best_missing}"
        )

    raise AttributeError(
        f"未找到可调用入口。模块={module.__name__}，公共对象={safe_public_names(module)}"
    )


def print_stage_header(
    stage_title: str,
    trade_date: str,
    reports_dir: str,
    entry_type: str,
    entry_name: str,
    unknown_args: Optional[Sequence[str]] = None,
    extra_lines: Optional[Iterable[str]] = None,
) -> None:
    print("============================================================")
    print(stage_title)
    print(f"目标交易日  : {trade_date}")
    print(f"输出目录    : {reports_dir}")
    print(f"入口类型    : {entry_type}")
    print(f"调用入口    : {entry_name}")
    if extra_lines:
        for line in extra_lines:
            if line:
                print(line)
    if unknown_args:
        print(f"忽略未知参数: {list(unknown_args)}")
    print("============================================================")


def print_result(result) -> None:
    if isinstance(result, tuple):
        print(result)
        return

    if isinstance(result, dict):
        summary_text = result.get("summary_text")
        if summary_text:
            print(summary_text)

        preferred_keys = [
            "output_path",
            "top_path",
            "all_path",
            "error_path",
            "summary_path",
            "txt_path",
            "detail_path",
            "details_path",
            "risk_checked_path",
            "stage_status_path",
            "summary_csv_path",
            "error_log_path",
            "portfolio_plan_path",
            "reconciliation_path",
            "attribution_path",
            "review_path",
            "validation_path",
        ]
        printed = set()
        for key in preferred_keys:
            value = result.get(key)
            if value and key not in printed:
                print(f"{key}: {value}")
                printed.add(key)
        return

    if result is not None:
        print(result)


def first_existing(paths: Sequence[str]) -> Optional[str]:
    for path in paths:
        if path and os.path.exists(path):
            return path
    return None


def build_reuse_result(
    stage_name: str,
    trade_date: str,
    artifact_paths: Dict[str, str],
    extra_text: str = "",
) -> Dict[str, Any]:
    lines = [
        "============================================================",
        f"{stage_name}复用既有工件",
        f"目标交易日: {trade_date}",
    ]
    if extra_text:
        lines.append(extra_text)
    for key, value in artifact_paths.items():
        if value:
            lines.append(f"{key}: {value}")
    lines.append("============================================================")
    return {
        "summary_text": "\n".join(lines),
        **artifact_paths,
    }