# -*- coding: utf-8 -*-
"""
Created on Wed Mar 18 02:05:14 2026

@author: DELL
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

PROJECT_ROOT = Path(r"C:\quant_system")
SCRIPT_DIR = PROJECT_ROOT / "scripts"
MANAGER_FILENAME = "reconciliation_review_manager.py"

for p in (PROJECT_ROOT, SCRIPT_DIR):
    p_str = str(p)
    if p_str not in sys.path:
        sys.path.insert(0, p_str)


def _load_manager_class():
    """
    导入优先级：
    1. managers.reconciliation_review_manager
    2. scripts 同目录直接导入 reconciliation_review_manager
    3. 按文件绝对路径兜底加载
    """
    try:
        from managers.reconciliation_review_manager import ReconciliationReviewManager
        return ReconciliationReviewManager
    except Exception:
        pass

    try:
        from reconciliation_review_manager import ReconciliationReviewManager
        return ReconciliationReviewManager
    except Exception:
        pass

    candidate_paths = [
        SCRIPT_DIR / MANAGER_FILENAME,
        PROJECT_ROOT / "managers" / MANAGER_FILENAME,
    ]

    for manager_path in candidate_paths:
        if not manager_path.exists():
            continue

        spec = importlib.util.spec_from_file_location(
            "reconciliation_review_manager_dynamic",
            manager_path,
        )
        if spec is None or spec.loader is None:
            continue

        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        if hasattr(module, "ReconciliationReviewManager"):
            return module.ReconciliationReviewManager

    raise ModuleNotFoundError(
        "未找到 ReconciliationReviewManager。\n"
        f"请确认文件存在：\n"
        f"1) {SCRIPT_DIR / MANAGER_FILENAME}\n"
        f"2) {PROJECT_ROOT / 'managers' / MANAGER_FILENAME}"
    )


ReconciliationReviewManager = _load_manager_class()


def main() -> None:
    manager = ReconciliationReviewManager(
        project_root=PROJECT_ROOT,
        trade_date="2026-03-17",
    )
    result = manager.run()

    print("=" * 60)
    print("对账异常闭环复盘生成完成")
    print(f"归因明细记录数: {result['input_rows']}")
    print(f"闭环复盘记录数: {result['review_rows']}")
    print(f"次日重点跟踪数: {result['watchlist_rows']}")
    print(f"闭环明细文件: {result['detail_path']}")
    print(f"责任归口汇总文件: {result['summary_path']}")
    print(f"次日重点跟踪文件: {result['watchlist_path']}")
    print(f"文本摘要文件: {result['text_path']}")
    print("=" * 60)
    print(result["summary_text"])


if __name__ == "__main__":
    main()