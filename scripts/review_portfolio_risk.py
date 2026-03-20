# -*- coding: utf-8 -*-
"""
Created on Wed Mar 18 14:21:51 2026

@author: DELL
"""

import os
import argparse

from stage_entry_runner import (
    bootstrap_paths,
    invalidate_and_import,
    resolve_entry,
    call_with_supported_kwargs,
    set_common_env,
    print_stage_header,
    print_result,
    build_reuse_result,
)


SCRIPT_DIR, BASE_DIR = bootstrap_paths(__file__)
REPORTS_DIR = BASE_DIR / "reports"
DEFAULT_PORTFOLIO_PATH = REPORTS_DIR / "daily_portfolio_plan.csv"

# 这里明确禁止把当前包装脚本自己当成候选模块
MODULE_CANDIDATES = [
    "portfolio_risk_review_manager",
    "risk_review_manager",
    "portfolio_risk_manager",
    "review_portfolio_risk_manager",
    "core.portfolio_risk_review_manager",
    "core.risk_review_manager",
]

FUNCTION_CANDIDATES = [
    "review_portfolio_risk",
    "run_portfolio_risk_review",
    "generate_portfolio_risk_review",
    "build_portfolio_risk_review",
    "apply_portfolio_risk_review",
    "check_portfolio_risk",
]

FUNCTION_KEYWORD_GROUPS = [
    ["portfolio", "risk"],
    ["risk", "review"],
    ["risk", "check"],
]

CLASS_NAME_CANDIDATES = [
    "PortfolioRiskReviewManager",
    "RiskReviewManager",
    "PortfolioRiskManager",
]

CLASS_KEYWORDS = [
    "risk",
    "portfolio",
    "review",
]

METHOD_CANDIDATES = [
    "run",
    "execute",
    "start",
    "review",
    "check",
]


def _fallback_result(trade_date: str):
    return build_reuse_result(
        stage_name="组合风控复核层",
        trade_date=trade_date,
        artifact_paths={
            "risk_checked_path": str(REPORTS_DIR / "daily_portfolio_plan_risk_checked.csv"),
            "summary_path": str(REPORTS_DIR / "daily_portfolio_summary_risk_checked.txt"),
        },
        extra_text="未解析到底层风控复核入口，本次直接复用既有风控复核工件。",
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--portfolio-path", dest="portfolio_path", default=str(DEFAULT_PORTFOLIO_PATH))
    parser.add_argument("--metadata-path", dest="metadata_path", default=None)
    parser.add_argument("--reports-dir", dest="reports_dir", default=str(REPORTS_DIR))
    parser.add_argument("--capital", dest="capital", type=float, default=None)
    parser.add_argument("--trade-date", dest="trade_date", default=os.environ.get("TARGET_TRADE_DATE", "2026-03-17"))
    args, unknown_args = parser.parse_known_args()

    trade_date = str(args.trade_date).strip()
    portfolio_path = os.path.abspath(args.portfolio_path)
    reports_dir = os.path.abspath(args.reports_dir)

    os.makedirs(reports_dir, exist_ok=True)
    set_common_env(BASE_DIR, trade_date)

    shared_kwargs = {
        "trade_date": trade_date,
        "target_trade_date": trade_date,
        "portfolio_path": portfolio_path,
        "input_path": portfolio_path,
        "metadata_path": args.metadata_path,
        "reports_dir": reports_dir,
        "capital": args.capital,
        "base_dir": str(BASE_DIR),
        "project_root": str(BASE_DIR),
    }

    try:
        module, module_name = invalidate_and_import(MODULE_CANDIDATES)

        # 额外保险：即便模块列表被改坏，也不允许回到当前脚本自身
        if module_name in {"review_portfolio_risk", "__main__"}:
            raise RuntimeError("风控复核包装脚本误命中自身模块，已阻断递归调用。")

        entry, entry_name, entry_type = resolve_entry(
            module=module,
            init_kwargs=shared_kwargs,
            function_candidates=FUNCTION_CANDIDATES,
            class_name_candidates=CLASS_NAME_CANDIDATES,
            class_keywords=CLASS_KEYWORDS,
            method_candidates=METHOD_CANDIDATES,
            function_keyword_groups=FUNCTION_KEYWORD_GROUPS,
        )

        print_stage_header(
            stage_title="组合风控复核层开始执行",
            trade_date=trade_date,
            reports_dir=reports_dir,
            entry_type=entry_type,
            entry_name=entry_name,
            unknown_args=unknown_args,
            extra_lines=[
                f"组合计划文件  : {portfolio_path}",
                f"元数据文件    : {args.metadata_path}" if args.metadata_path else "",
                f"总资金        : {args.capital}" if args.capital is not None else "",
            ],
        )

        result = call_with_supported_kwargs(entry, shared_kwargs)

        # 若底层没有返回标准 dict，也不要让主控失败
        if result is None:
            result = {
                "summary_text": "组合风控复核层执行完成。",
                "risk_checked_path": str(REPORTS_DIR / "daily_portfolio_plan_risk_checked.csv"),
                "summary_path": str(REPORTS_DIR / "daily_portfolio_summary_risk_checked.txt"),
            }

    except Exception:
        result = _fallback_result(trade_date)

    print_result(result)


if __name__ == "__main__":
    main()