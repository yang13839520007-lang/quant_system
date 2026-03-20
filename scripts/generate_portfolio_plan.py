import os
import sys
import argparse
import importlib
import inspect


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(SCRIPT_DIR)
REPORTS_DIR = os.path.join(BASE_DIR, "reports")
DEFAULT_TRADE_PLAN_PATH = os.path.join(REPORTS_DIR, "daily_trade_plan_all.csv")

if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)


MODULE_CANDIDATES = [
    "portfolio_plan_manager",
    "core.portfolio_builder",
]

FUNCTION_CANDIDATES = [
    "generate_portfolio_plan",
    "build_portfolio_plan",
    "run_portfolio_plan",
    "execute_portfolio_plan",
    "main",
]

CLASS_CANDIDATES = [
    "PortfolioPlanManager",
    "PortfolioBuilder",
    "DailyPortfolioPlanManager",
]

METHOD_CANDIDATES = [
    "run",
    "execute",
    "start",
    "build",
    "generate",
    "main",
]


def _call_with_supported_kwargs(func, kwargs):
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


def _instantiate_class_with_supported_kwargs(cls, kwargs):
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


def _resolve_module():
    last_error = None
    for module_name in MODULE_CANDIDATES:
        try:
            if module_name in sys.modules:
                return importlib.reload(sys.modules[module_name]), module_name
            return importlib.import_module(module_name), module_name
        except Exception as exc:
            last_error = exc
    raise ImportError(f"无法导入组合计划模块: {MODULE_CANDIDATES} | last_error={last_error}")


def _safe_public_names(module):
    return [name for name in dir(module) if not name.startswith("_")]


def _resolve_entry(module, init_kwargs):
    for name in FUNCTION_CANDIDATES:
        obj = getattr(module, name, None)
        if callable(obj) and not inspect.isclass(obj):
            return obj, f"{module.__name__}.{name}", "function"

    for name in CLASS_CANDIDATES:
        cls = getattr(module, name, None)
        if inspect.isclass(cls):
            instance = _instantiate_class_with_supported_kwargs(cls, init_kwargs)
            for method_name in METHOD_CANDIDATES:
                method = getattr(instance, method_name, None)
                if callable(method):
                    return method, f"{module.__name__}.{name}.{method_name}", "method"

    for name in _safe_public_names(module):
        obj = getattr(module, name, None)
        if inspect.isclass(obj):
            lowered = name.lower()
            if "portfolio" in lowered or "builder" in lowered:
                try:
                    instance = _instantiate_class_with_supported_kwargs(obj, init_kwargs)
                except Exception:
                    continue
                for method_name in METHOD_CANDIDATES:
                    method = getattr(instance, method_name, None)
                    if callable(method):
                        return method, f"{module.__name__}.{name}.{method_name}", "method"

    raise AttributeError(
        "未在组合计划模块中找到可调用入口。\n"
        f"模块: {module.__name__}\n"
        f"公共对象: {_safe_public_names(module)}"
    )


def _print_result(result):
    if isinstance(result, dict):
        summary_text = result.get("summary_text")
        output_path = result.get("output_path") or result.get("portfolio_plan_path")
        top_path = result.get("top_path") or result.get("top5_path")
        summary_path = result.get("summary_path") or result.get("txt_path")

        if summary_text:
            print(summary_text)
        if output_path:
            print(f"组合计划文件: {output_path}")
        if top_path:
            print(f"TOP文件    : {top_path}")
        if summary_path:
            print(f"摘要文件    : {summary_path}")
    elif result is not None:
        print(result)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--trade-plan-path", dest="trade_plan_path", default=DEFAULT_TRADE_PLAN_PATH)
    parser.add_argument("--reports-dir", dest="reports_dir", default=REPORTS_DIR)
    parser.add_argument("--capital", dest="capital", type=float, default=None)
    parser.add_argument("--trade-date", dest="trade_date", default=os.environ.get("TARGET_TRADE_DATE", "2026-03-17"))
    args, unknown_args = parser.parse_known_args()

    trade_plan_path = os.path.abspath(args.trade_plan_path)
    reports_dir = os.path.abspath(args.reports_dir)
    trade_date = str(args.trade_date).strip()

    os.environ["TARGET_TRADE_DATE"] = trade_date
    os.environ["QUANT_SYSTEM_BASE_DIR"] = BASE_DIR

    os.makedirs(reports_dir, exist_ok=True)
    importlib.invalidate_caches()

    module, module_name = _resolve_module()

    shared_kwargs = {
        "trade_plan_path": trade_plan_path,
        "reports_dir": reports_dir,
        "capital": args.capital,
        "trade_date": trade_date,
        "target_trade_date": trade_date,
        "base_dir": BASE_DIR,
        "project_root": BASE_DIR,
    }

    entry, entry_name, entry_type = _resolve_entry(module, shared_kwargs)

    print("============================================================")
    print("组合计划开始生成")
    print(f"目标交易日  : {trade_date}")
    print(f"交易计划文件: {trade_plan_path}")
    print(f"输出目录    : {reports_dir}")
    print(f"入口类型    : {entry_type}")
    print(f"调用入口    : {entry_name}")
    if args.capital is not None:
        print(f"总资金      : {args.capital}")
    if unknown_args:
        print(f"忽略未知参数: {unknown_args}")
    print("============================================================")

    result = _call_with_supported_kwargs(entry, shared_kwargs)
    _print_result(result)


if __name__ == "__main__":
    main()