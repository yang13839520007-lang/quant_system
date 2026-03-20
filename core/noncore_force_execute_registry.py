# -*- coding: utf-8 -*-
"""
Created on Thu Mar 19 15:08:59 2026

@author: DELL
"""

# -*- coding: utf-8 -*-
from __future__ import annotations

NONCORE_FORCE_EXECUTE_STAGE_NOS = {
    8,
}

NONCORE_FORCE_EXECUTE_REASON_MAP = {
    8: "收盘复盘层进入非核心去复用化阶段，要求每次主控运行重新执行并覆盖输出。",
}


def is_noncore_force_execute_stage(stage_no: int) -> bool:
    return int(stage_no) in NONCORE_FORCE_EXECUTE_STAGE_NOS


def get_noncore_force_execute_reason(stage_no: int) -> str:
    return NONCORE_FORCE_EXECUTE_REASON_MAP.get(int(stage_no), "")