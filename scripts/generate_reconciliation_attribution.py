# -*- coding: utf-8 -*-
"""
Created on Wed Mar 18 01:41:11 2026

@author: DELL
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

PROJECT_ROOT = Path(r"C:\quant_system")
SCRIPT_DIR = PROJECT_ROOT / "scripts"
MANAGER_FILENAME = "reconciliation_attribution_manager.py"

for p in (PROJECT_ROOT, SCRIPT_DIR):
    p_str = str(p)
    if p_str not in sys.path:
        sys.path.insert(0, p_str)


def _load_manager_class():
    """
    导入优先级：
    1. managers.reconciliation_attribution_manager
    2. scripts 同目录直接导入 reconciliation_attribution_manager
    3. 按文件绝对路径兜底加载
    """
    try:
        from managers.reconciliation_attribution_manager import ReconciliationAttributionManager
        return ReconciliationAttributionManager
    except Exception:
        pass

    try:
        from reconciliation_attribution_manager import ReconciliationAttributionManager
        return ReconciliationAttributionManager
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
            "reconciliation_attribution_manager_dynamic",
            manager_path,
        )
        if spec is None or spec.loader is None:
            continue

        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        if hasattr(module, "ReconciliationAttributionManager"):
            return module.ReconciliationAttributionManager

    raise ModuleNotFoundError(
        "未找到 ReconciliationAttributionManager。"
        "请确认以下任一路径存在文件：\n"
        f"1) {SCRIPT_DIR / MANAGER_FILENAME}\n"
        f"2) {PROJECT_ROOT / 'managers' / MANAGER_FILENAME}"
    )


ReconciliationAttributionManager = _load_manager_class()


def main() -> None:
    manager = ReconciliationAttributionManager(
        project_root=PROJECT_ROOT,
        trade_date="2026-03-17",
        slippage_threshold=0.01,
        position_deviation_threshold=0.10,
        full_fill_threshold=0.98,
    )
    result = manager.run()

    print("=" * 60)
    print("对账异常归因生成完成")
    print(f"对账明细记录数: {result['recon_rows']}")
    print(f"异常条数: {result['anomaly_rows']}")
    print(f"涉及异常标的数: {result['anomaly_codes']}")
    print(f"异常明细文件: {result['detail_path']}")
    print(f"异常汇总文件: {result['summary_path']}")
    print(f"优先级文件: {result['priority_path']}")
    print(f"文本摘要文件: {result['text_path']}")
    print("=" * 60)
    print(result["summary_text"])


if __name__ == "__main__":
    main()