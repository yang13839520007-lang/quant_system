from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

PROJECT_ROOT = Path(r"C:\quant_system")
SCRIPT_DIR = PROJECT_ROOT / "scripts"

for p in (PROJECT_ROOT, SCRIPT_DIR):
    p_str = str(p)
    if p_str not in sys.path:
        sys.path.insert(0, p_str)


def _load_class(module_filename: str, class_name: str):
    module_stem = module_filename.replace(".py", "")

    try:
        module = __import__(module_stem, fromlist=[class_name])
        if hasattr(module, class_name):
            return getattr(module, class_name)
    except Exception:
        pass

    try:
        module = __import__(f"managers.{module_stem}", fromlist=[class_name])
        if hasattr(module, class_name):
            return getattr(module, class_name)
    except Exception:
        pass

    candidate_paths = [
        SCRIPT_DIR / module_filename,
        PROJECT_ROOT / "managers" / module_filename,
    ]
    for module_path in candidate_paths:
        if not module_path.exists():
            continue
        spec = importlib.util.spec_from_file_location(f"dynamic_{module_stem}", module_path)
        if spec is None or spec.loader is None:
            continue
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        if hasattr(module, class_name):
            return getattr(module, class_name)

    raise ModuleNotFoundError(
        f"未找到 {class_name}。请确认以下任一路径存在文件:\n"
        f"1) {SCRIPT_DIR / module_filename}\n"
        f"2) {PROJECT_ROOT / 'managers' / module_filename}"
    )


ReconciliationReplayValidationManager = _load_class(
    module_filename="reconciliation_replay_validation_manager.py",
    class_name="ReconciliationReplayValidationManager",
)
ReconciliationAttributionManager = _load_class(
    module_filename="reconciliation_attribution_manager.py",
    class_name="ReconciliationAttributionManager",
)
ReconciliationReviewManager = _load_class(
    module_filename="reconciliation_review_manager.py",
    class_name="ReconciliationReviewManager",
)


def main() -> None:
    manager = ReconciliationReplayValidationManager(
        project_root=PROJECT_ROOT,
        trade_date="2026-03-17",
        attribution_manager_class=ReconciliationAttributionManager,
        review_manager_class=ReconciliationReviewManager,
        slippage_threshold=0.01,
        position_deviation_threshold=0.10,
        full_fill_threshold=0.98,
    )
    result = manager.run()

    print("=" * 60)
    print("对账异常注入回放验证完成")
    print(f"注入对账明细行数: {result['injected_rows']}")
    print(f"预期验证案例数: {result['expected_cases']}")
    print(f"归因层输出异常数: {result['attribution_rows']}")
    print(f"闭环复盘输出数: {result['review_rows']}")
    print(f"验证明细数: {result['validation_rows']}")
    print(f"异常命中数: {result['hit_count']}")
    print(f"异常漏判数: {result['miss_count']}")
    print(f"正常样本误报校验: {'通过' if result['normal_case_pass'] == 1 else '失败'}")
    print(f"注入明细文件: {result['injected_detail_path']}")
    print(f"预期案例文件: {result['expected_cases_path']}")
    print(f"归因明细文件: {result['attr_detail_path']}")
    print(f"归因汇总文件: {result['attr_summary_path']}")
    print(f"归因优先级文件: {result['attr_priority_path']}")
    print(f"闭环明细文件: {result['review_detail_path']}")
    print(f"闭环汇总文件: {result['review_summary_path']}")
    print(f"次日跟踪文件: {result['review_watchlist_path']}")
    print(f"验证明细文件: {result['validation_detail_path']}")
    print(f"验证汇总文件: {result['validation_summary_path']}")
    print(f"文本摘要文件: {result['text_path']}")
    print("=" * 60)
    print(result["summary_text"])


if __name__ == "__main__":
    main()
