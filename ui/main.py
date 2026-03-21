from __future__ import annotations

import sys

from ui.config import load_config
from ui.log_manager import LogManager


def main() -> int:
    try:
        from PySide6.QtWidgets import QApplication
        from ui.main_window import MainWindow
    except ModuleNotFoundError as exc:
        print(f"启动失败：缺少依赖 {exc.name}。请先执行 `python -m pip install -r requirements_ui.txt`。")
        return 1

    config = load_config()
    log_manager = LogManager(config.logs_dir)

    app = QApplication(sys.argv)
    app.setApplicationName(config.window_title)
    app.setStyle("Fusion")

    window = MainWindow(config=config, log_manager=log_manager)
    window.show()
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())
