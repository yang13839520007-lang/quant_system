# -*- coding: utf-8 -*-
"""
Created on Thu Mar 19 15:08:21 2026

@author: DELL
"""

# -*- coding: utf-8 -*-
from __future__ import annotations

import sys
from pathlib import Path


def _bootstrap():
    current_file = Path(__file__).resolve()
    project_root = current_file.parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    return project_root


_bootstrap()

from generate_close_review import main  # noqa: E402


if __name__ == "__main__":
    main()