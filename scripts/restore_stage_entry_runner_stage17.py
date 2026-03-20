# -*- coding: utf-8 -*-
"""
Created on Thu Mar 19 09:15:56 2026

@author: DELL
"""

from __future__ import annotations

import shutil
from pathlib import Path


def read_text_auto(path: Path) -> tuple[str, str]:
    last_error = None
    for encoding in ("utf-8", "utf-8-sig", "gbk", "gb18030"):
        try:
            return path.read_text(encoding=encoding), encoding
        except Exception as exc:
            last_error = exc
    raise last_error  # type: ignore[misc]


def write_text(path: Path, text: str, encoding: str = "utf-8") -> None:
    path.write_text(text, encoding=encoding, newline="\n")


def find_latest_backup(project_root: Path) -> Path | None:
    candidates = sorted(
        project_root.glob("stage_entry_runner.stage17_backup_*.py"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return candidates[0] if candidates else None


def strip_stage17_patch_block(text: str) -> str:
    begin_marks = [
        "# ===== STAGE17_OUTPUT_GUARD_PATCH_V1 BEGIN =====",
        "# ===== STAGE17_OUTPUT_GUARD_PATCH_V2 BEGIN =====",
    ]
    end_marks = [
        "# ===== STAGE17_OUTPUT_GUARD_PATCH_V1 END =====",
        "# ===== STAGE17_OUTPUT_GUARD_PATCH_V2 END =====",
    ]

    for begin_mark, end_mark in zip(begin_marks, end_marks):
        if begin_mark in text and end_mark in text:
            start = text.index(begin_mark)
            end = text.index(end_mark) + len(end_mark)
            text = (text[:start].rstrip() + "\n").rstrip() + "\n"
            text = text[:start] + text[end:]
            return text

    return text


def restore_from_backup(project_root: Path, target_path: Path, backup_path: Path) -> None:
    shutil.copy2(backup_path, target_path)


def restore_by_stripping_patch(target_path: Path) -> None:
    text, encoding = read_text_auto(target_path)
    original_text = text

    # 还原可能被 rename 的函数名
    text = text.replace("def _stage17_wrapped_original_run_stage_entry(", "def run_stage_entry(")
    text = text.replace("def _stage17_wrapped_original_execute_stage_entry(", "def execute_stage_entry(")
    text = text.replace("def _stage17_wrapped_original_run_stage(", "def run_stage(")
    text = text.replace("def _stage17_wrapped_original_execute_stage(", "def execute_stage(")

    # 移除 patch block
    text = strip_stage17_patch_block(text)

    if text == original_text:
        raise RuntimeError("未发现可回滚的 Stage17 补丁痕迹，且未找到备份文件。")

    write_text(target_path, text, encoding=encoding)


def main() -> int:
    project_root = Path(__file__).resolve().parent.parent
    target_path = project_root / "stage_entry_runner.py"

    if not target_path.exists():
        raise FileNotFoundError(f"未找到目标文件: {target_path}")

    latest_backup = find_latest_backup(project_root)

    rollback_snapshot = project_root / "stage_entry_runner.rollback_before_restore.py"
    shutil.copy2(target_path, rollback_snapshot)

    if latest_backup is not None:
        restore_from_backup(project_root=project_root, target_path=target_path, backup_path=latest_backup)
        print("=" * 60)
        print("stage_entry_runner.py 已从备份恢复")
        print(f"目标文件      : {target_path}")
        print(f"恢复来源备份  : {latest_backup}")
        print(f"回滚保护副本  : {rollback_snapshot}")
        print("=" * 60)
        return 0

    restore_by_stripping_patch(target_path)
    print("=" * 60)
    print("未找到备份文件，已按文本方式移除 Stage17 补丁")
    print(f"目标文件      : {target_path}")
    print(f"回滚保护副本  : {rollback_snapshot}")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())