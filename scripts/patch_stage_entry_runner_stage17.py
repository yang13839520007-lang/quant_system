# -*- coding: utf-8 -*-
"""
Created on Thu Mar 19 09:01:50 2026

@author: DELL
"""

from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path


PATCH_MARK = "STAGE17_OUTPUT_GUARD_PATCH_V2"
CANDIDATE_PUBLIC_FUNCS = [
    "run_stage_entry",
    "execute_stage_entry",
    "run_stage",
    "execute_stage",
]


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


def _regex_func_def(func_name: str) -> str:
    return rf"^def\s+{re.escape(func_name)}\s*\("


def build_helper_block(wrapped_func_names: list[str]) -> str:
    wrapper_defs: list[str] = []

    for func_name in wrapped_func_names:
        original_name = f"_stage17_wrapped_original_{func_name}"
        wrapper_defs.append(
            f"""
def {func_name}(*args, **kwargs):
    stage_no = _stage17_extract_stage_no(*args, **kwargs)
    project_root = _stage17_extract_project_root(*args, **kwargs)

    preclean_removed_files = _stage17_safe_purge(stage_no=stage_no, project_root=project_root)

    try:
        stage_result = {original_name}(*args, **kwargs)
    except Exception:
        _stage17_safe_purge(stage_no=stage_no, project_root=project_root)
        raise

    if isinstance(stage_result, dict):
        stage_result.setdefault("preclean_removed_files", preclean_removed_files)
        stage_result.setdefault("preclean_removed_count", len(preclean_removed_files))

        stage_status = str(stage_result.get("stage_status", "") or "").upper()
        message = str(stage_result.get("message", "") or "")
        policy_message = str(stage_result.get("policy_message", "") or "")
        policy_rejected = bool(stage_result.get("policy_rejected", False))

        if policy_rejected or ("已拒绝" in message) or ("已拒绝" in policy_message):
            reject_removed_files = _stage17_safe_purge(stage_no=stage_no, project_root=project_root)
            stage_result["reject_removed_files"] = reject_removed_files
            stage_result["reject_removed_count"] = len(reject_removed_files)

        elif stage_status == "FAILED":
            failed_removed_files = _stage17_safe_purge(stage_no=stage_no, project_root=project_root)
            stage_result["failed_removed_files"] = failed_removed_files
            stage_result["failed_removed_count"] = len(failed_removed_files)

    return stage_result
""".rstrip()
        )

    helper_block = f"""

# ===== {PATCH_MARK} BEGIN =====
from pathlib import Path as _Stage17Path


def _stage17_extract_stage_no(*args, **kwargs):
    if "stage_no" in kwargs:
        try:
            return int(kwargs["stage_no"])
        except Exception:
            return None

    if args:
        first = args[0]
        if isinstance(first, int):
            return first
        if isinstance(first, dict) and "stage_no" in first:
            try:
                return int(first["stage_no"])
            except Exception:
                return None
    return None


def _stage17_extract_project_root(*args, **kwargs):
    for key in ("base_dir", "project_root", "root_dir", "root_path", "base_path"):
        value = kwargs.get(key)
        if value:
            try:
                return str(_Stage17Path(value).resolve())
            except Exception:
                return str(value)

    for item in args:
        if isinstance(item, dict):
            for key in ("base_dir", "project_root", "root_dir", "root_path", "base_path"):
                value = item.get(key)
                if value:
                    try:
                        return str(_Stage17Path(value).resolve())
                    except Exception:
                        return str(value)

    try:
        return str(_Stage17Path(__file__).resolve().parent)
    except Exception:
        return "."


def _stage17_safe_purge(stage_no, project_root):
    if stage_no in (None, ""):
        return []

    try:
        from core.stage_output_guard import purge_stage_output_files
        return purge_stage_output_files(stage_no=int(stage_no), project_root=project_root)
    except Exception:
        return []


{chr(10).join(wrapper_defs)}
# ===== {PATCH_MARK} END =====
""".rstrip()

    return helper_block + "\n"


def rename_public_funcs(text: str) -> tuple[str, list[str]]:
    wrapped: list[str] = []

    for func_name in CANDIDATE_PUBLIC_FUNCS:
        original_name = f"_stage17_wrapped_original_{func_name}"

        if re.search(_regex_func_def(original_name), text, flags=re.MULTILINE):
            wrapped.append(func_name)
            continue

        pattern = _regex_func_def(func_name)
        if re.search(pattern, text, flags=re.MULTILINE):
            text = re.sub(
                pattern,
                f"def {original_name}(",
                text,
                count=1,
                flags=re.MULTILINE,
            )
            wrapped.append(func_name)

    return text, wrapped


def main() -> int:
    project_root = Path(__file__).resolve().parent.parent
    target_path = project_root / "stage_entry_runner.py"

    if not target_path.exists():
        raise FileNotFoundError(f"未找到目标文件: {target_path}")

    text, encoding = read_text_auto(target_path)

    if PATCH_MARK in text:
        print("=" * 60)
        print("stage_entry_runner.py 已存在 Stage17 输出隔离补丁，跳过重复打补丁")
        print(f"目标文件: {target_path}")
        print("=" * 60)
        return 0

    patched_text, wrapped_funcs = rename_public_funcs(text)
    if not wrapped_funcs:
        raise RuntimeError(
            "未识别到可包装的公开阶段执行函数。"
            f" 候选函数名: {CANDIDATE_PUBLIC_FUNCS}"
        )

    helper_block = build_helper_block(wrapped_funcs)
    patched_text = patched_text.rstrip() + "\n\n" + helper_block

    backup_name = f"stage_entry_runner.stage17_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.py"
    backup_path = target_path.with_name(backup_name)

    write_text(backup_path, text, encoding=encoding)
    write_text(target_path, patched_text, encoding=encoding)

    print("=" * 60)
    print("Stage17 输出隔离补丁已完成")
    print(f"目标文件 : {target_path}")
    print(f"备份文件 : {backup_path}")
    print(f"包装函数 : {wrapped_funcs}")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())