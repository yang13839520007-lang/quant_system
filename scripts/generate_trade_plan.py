# -*- coding: utf-8 -*-
"""
Created on Thu Mar 19 11:31:50 2026

@author: DELL
"""

from __future__ import annotations

import argparse
import importlib.util
import inspect
import sys
import traceback
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
TARGET_FILE = PROJECT_ROOT / "generate_trade_plan.py"
MODULE_ALIAS = "_quant_system_root_generate_trade_plan"


def _load_root_module():
    if MODULE_ALIAS in sys.modules:
        return sys.modules[MODULE_ALIAS]

    if not TARGET_FILE.exists():
        raise FileNotFoundError(f"未找到根模块文件: {TARGET_FILE}")

    spec = importlib.util.spec_from_file_location(MODULE_ALIAS, TARGET_FILE)
    if spec is None or spec.loader is None:
        raise ImportError(f"无法加载模块: {TARGET_FILE}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[MODULE_ALIAS] = module
    spec.loader.exec_module(module)
    return module


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="每日交易计划生成脚本入口")
    parser.add_argument("--trading-date", dest="trading_date", default=None)
    parser.add_argument("--project-root", dest="project_root", default=None)
    parser.add_argument("--candidates-path", dest="candidates_path", default=None)
    parser.add_argument("--output-dir", dest="output_dir", default=None)
    return parser


def _resolve_runtime_kwargs(**kwargs: Any) -> dict[str, Any]:
    trading_date = kwargs.get("trading_date") or kwargs.get("trade_date") or kwargs.get("target_trading_date")
    project_root = (
        kwargs.get("project_root")
        or kwargs.get("base_dir")
        or kwargs.get("project_dir")
        or kwargs.get("root_dir")
        or str(PROJECT_ROOT)
    )
    output_dir = kwargs.get("output_dir") or kwargs.get("reports_dir") or str(Path(project_root) / "reports")
    candidates_path = (
        kwargs.get("candidates_path")
        or kwargs.get("candidate_path")
        or kwargs.get("candidate_file")
        or kwargs.get("candidates_file")
        or str(Path(output_dir) / "daily_candidates_all.csv")
    )

    runtime_kwargs = {
        "trading_date": trading_date,
        "trade_date": trading_date,
        "target_trading_date": trading_date,
        "project_root": str(project_root),
        "base_dir": str(project_root),
        "project_dir": str(project_root),
        "root_dir": str(project_root),
        "candidates_path": str(candidates_path),
        "candidate_path": str(candidates_path),
        "candidate_file": str(candidates_path),
        "candidates_file": str(candidates_path),
        "output_dir": str(output_dir),
        "reports_dir": str(output_dir),
    }
    return runtime_kwargs


def _invoke_callable(func, runtime_kwargs: dict[str, Any]):
    sig = inspect.signature(func)
    params = list(sig.parameters.values())
    accepts_var_kwargs = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params)

    if accepts_var_kwargs:
        clean_kwargs = {k: v for k, v in runtime_kwargs.items() if v is not None}
        return func(**clean_kwargs)

    call_kwargs: dict[str, Any] = {}
    positional_args: list[Any] = []

    for param in params:
        if param.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
            continue

        value = None
        provided = False

        if param.name in runtime_kwargs and runtime_kwargs[param.name] is not None:
            value = runtime_kwargs[param.name]
            provided = True
        elif param.default is inspect._empty:
            name = param.name.lower()
            if "date" in name:
                value = runtime_kwargs.get("trading_date")
                provided = value is not None
            elif any(token in name for token in ["project", "root", "base"]):
                value = runtime_kwargs.get("project_root")
                provided = value is not None
            elif any(token in name for token in ["candidate", "plan_file", "input_file"]):
                value = runtime_kwargs.get("candidates_path")
                provided = value is not None
            elif any(token in name for token in ["output", "report"]):
                value = runtime_kwargs.get("output_dir")
                provided = value is not None

        if param.kind == inspect.Parameter.KEYWORD_ONLY:
            if provided:
                call_kwargs[param.name] = value
            elif param.default is inspect._empty:
                raise TypeError(f"无法为参数赋值: {param.name}")
        else:
            if provided:
                positional_args.append(value)
            elif param.default is inspect._empty:
                raise TypeError(f"无法为参数赋值: {param.name}")

    return func(*positional_args, **call_kwargs)


def main(**kwargs: Any):
    if not kwargs:
        args, _ = _build_parser().parse_known_args()
        kwargs = vars(args)

    runtime_kwargs = _resolve_runtime_kwargs(**kwargs)
    module = _load_root_module()

    for entry_name in ("main", "run", "generate_trade_plan"):
        func = getattr(module, entry_name, None)
        if callable(func):
            return _invoke_callable(func, runtime_kwargs)

    raise AttributeError(f"{TARGET_FILE.name} 未找到可调用入口: main/run/generate_trade_plan")


def run(**kwargs: Any):
    return main(**kwargs)


def generate_trade_plan(**kwargs: Any):
    return main(**kwargs)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc()
        raise