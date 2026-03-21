from __future__ import annotations

from datetime import datetime
import logging

from PySide6.QtCore import QObject, Signal

from ui.config import AppConfig
from ui.data_loader import DashboardSnapshot, ReportDataLoader


class SnapshotRefreshWorker(QObject):
    """Load dashboard data off the UI thread to avoid blocking the main window."""

    finished = Signal(object, int)
    failed = Signal(str)

    def __init__(self, config: AppConfig, logger: logging.Logger) -> None:
        super().__init__()
        self.config = config
        self.logger = logger

    def run(self) -> None:
        started_at = datetime.now()
        try:
            loader = ReportDataLoader(config=self.config, logger=self.logger)
            snapshot: DashboardSnapshot = loader.load_snapshot()
        except Exception as exc:  # pragma: no cover - defensive branch
            self.logger.exception("后台刷新失败")
            self.failed.emit(str(exc))
            return

        duration_ms = int((datetime.now() - started_at).total_seconds() * 1000)
        self.finished.emit(snapshot, duration_ms)
