from __future__ import annotations

from pathlib import Path
import logging

from PySide6.QtCore import QDate, QThread, QTimer, Qt, QUrl
from PySide6.QtGui import QDesktopServices
from PySide6.QtWidgets import (
    QDateEdit,
    QHBoxLayout,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QSplitter,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)

from ui.alert_manager import AlertManager
from ui.config import AppConfig
from ui.data_loader import DashboardSnapshot
from ui.log_manager import LogManager
from ui.orchestrator_runner import OrchestratorRunner
from ui.refresh_worker import SnapshotRefreshWorker
from ui.widgets.log_viewer import LogViewer
from ui.widgets.status_panel import StatusPanel
from ui.widgets.summary_viewer import SummaryViewer
from ui.widgets.table_page import TablePage


class MainWindow(QMainWindow):
    """Main desktop terminal window for monitoring the quant pipeline."""

    def __init__(self, config: AppConfig, log_manager: LogManager, parent=None) -> None:
        super().__init__(parent)
        self.config = config
        self.log_manager = log_manager
        self.logger = log_manager.logger
        self.runner = OrchestratorRunner(config=config, logger=self.logger)
        self.alert_manager = AlertManager(logger=self.logger, parent=self)
        self.snapshot: DashboardSnapshot | None = None
        self.runner_stage_text = "未运行"
        self.runner_status_text = "空闲"
        self._refresh_thread: QThread | None = None
        self._refresh_worker: SnapshotRefreshWorker | None = None
        self._refresh_pending = False
        self._refresh_reason = "startup"

        self.setWindowTitle(config.window_title)
        self.resize(config.window_width, config.window_height)

        self.status_panel = StatusPanel(self)
        self.refresh_button = QPushButton("刷新数据", self)
        self.run_button = QPushButton("运行今日主控", self)
        self.open_reports_button = QPushButton("打开 reports 目录", self)
        self.open_logs_button = QPushButton("打开日志目录", self)
        self.trading_date_edit = QDateEdit(self)
        self.tabs = QTabWidget(self)
        self.summary_viewer = SummaryViewer("主控摘要", self)
        self.log_viewer = LogViewer("运行日志", max_lines=config.runtime_log_limit, parent=self)
        self.stage_status_page = TablePage("主控阶段状态", show_summary=False, parent=self)
        self.page_widgets: dict[str, TablePage] = {}
        self.refresh_timer = QTimer(self)

        self._build_ui()
        self._connect_signals()
        self._prime_log_viewer()
        self.request_refresh("startup")
        self.refresh_timer.start(config.auto_refresh_ms)

    def request_refresh(self, reason: str = "manual") -> None:
        if self._refresh_thread is not None:
            self._refresh_pending = True
            self._refresh_reason = reason
            self.statusBar().showMessage("刷新任务仍在进行，本次刷新已排队。")
            return

        self._refresh_reason = reason
        self.refresh_button.setEnabled(False)
        self.statusBar().showMessage(f"正在后台刷新数据: {reason}")

        self._refresh_thread = QThread()
        self._refresh_worker = SnapshotRefreshWorker(config=self.config, logger=self.logger)
        self._refresh_worker.moveToThread(self._refresh_thread)
        self._refresh_thread.started.connect(self._refresh_worker.run)
        self._refresh_worker.finished.connect(self._on_snapshot_ready)
        self._refresh_worker.failed.connect(self._on_refresh_failed)
        self._refresh_worker.finished.connect(self._refresh_thread.quit)
        self._refresh_worker.failed.connect(self._refresh_thread.quit)
        self._refresh_thread.finished.connect(self._cleanup_refresh_thread)
        self._refresh_thread.start()

    def _build_ui(self) -> None:
        central = QWidget(self)
        root_layout = QVBoxLayout(central)
        root_layout.setContentsMargins(12, 12, 12, 12)
        root_layout.setSpacing(10)

        control_row = QHBoxLayout()
        self.trading_date_edit.setCalendarPopup(True)
        self.trading_date_edit.setDisplayFormat("yyyy-MM-dd")
        self.trading_date_edit.setDate(QDate.currentDate())
        control_row.addWidget(self.trading_date_edit)
        control_row.addWidget(self.run_button)
        control_row.addWidget(self.refresh_button)
        control_row.addWidget(self.open_reports_button)
        control_row.addWidget(self.open_logs_button)
        control_row.addStretch(1)

        root_layout.addLayout(control_row)
        root_layout.addWidget(self.status_panel)
        root_layout.addWidget(self.tabs, stretch=1)

        for spec in self.config.report_specs:
            page_widget = TablePage(spec.title, show_summary=True, parent=self)
            self.page_widgets[spec.key] = page_widget
            self.tabs.addTab(page_widget, spec.title)

        summary_tab = QWidget(self)
        summary_layout = QVBoxLayout(summary_tab)
        summary_layout.setContentsMargins(8, 8, 8, 8)

        splitter = QSplitter(Qt.Orientation.Vertical, summary_tab)
        splitter.addWidget(self.summary_viewer)
        splitter.addWidget(self.stage_status_page)
        splitter.addWidget(self.log_viewer)
        splitter.setStretchFactor(0, 2)
        splitter.setStretchFactor(1, 3)
        splitter.setStretchFactor(2, 3)

        summary_layout.addWidget(splitter)
        self.tabs.addTab(summary_tab, "摘要/日志")

        self.setCentralWidget(central)
        self.statusBar().showMessage(f"自动刷新间隔: {self.config.auto_refresh_ms // 1000} 秒")

    def _connect_signals(self) -> None:
        self.refresh_button.clicked.connect(lambda: self.request_refresh("manual"))
        self.run_button.clicked.connect(self.run_orchestrator)
        self.open_reports_button.clicked.connect(lambda: self._open_directory(self.config.reports_dir))
        self.open_logs_button.clicked.connect(lambda: self._open_directory(self.config.logs_dir))
        self.refresh_timer.timeout.connect(lambda: self.request_refresh("auto"))

        self.runner.run_started.connect(self._on_runner_started)
        self.runner.output_line.connect(self._append_runtime_line)
        self.runner.stage_changed.connect(self._on_runner_stage_changed)
        self.runner.run_finished.connect(self._on_runner_finished)
        self.runner.start_rejected.connect(
            lambda message: self.alert_manager.notify_error("主控无法启动", message, blocking=False)
        )

    def _prime_log_viewer(self) -> None:
        self.log_viewer.set_text(self.log_manager.get_recent_text())

    def _apply_snapshot(self, snapshot: DashboardSnapshot) -> None:
        for spec in self.config.report_specs:
            page_data = snapshot.pages.get(spec.key)
            if page_data is not None:
                self.page_widgets[spec.key].update_page(page_data)

        self.stage_status_page.update_page(snapshot.stage_status_page)
        self.summary_viewer.set_content(
            snapshot.orchestrator_summary_text,
            status=f"主控状态: {snapshot.orchestrator_status}",
        )

        qdate = QDate.fromString(snapshot.trading_date, "yyyy-MM-dd")
        if qdate.isValid():
            self.trading_date_edit.setDate(qdate)

        reports_status = (
            f"{snapshot.key_files_present}/{snapshot.key_files_total} 关键报表存在 | "
            f"{snapshot.reports_file_count} 个文件 | 最近: {snapshot.reports_last_modified}"
        )
        orchestrator_status = self.runner_status_text if self.runner.is_running else snapshot.orchestrator_status
        current_stage = self.runner_stage_text if self.runner.is_running else snapshot.current_stage
        alert_status = self._build_alert_status(snapshot)
        self.status_panel.update_values(
            {
                "trading_day": snapshot.trading_date,
                "last_refresh": snapshot.refreshed_at.strftime("%Y-%m-%d %H:%M:%S"),
                "reports_status": reports_status,
                "orchestrator_status": orchestrator_status,
                "current_stage": current_stage,
                "error_status": snapshot.error_status,
                "alert_status": alert_status,
            }
        )

    def run_orchestrator(self) -> None:
        trading_date = self.trading_date_edit.date().toString("yyyy-MM-dd")
        self.runner.start(trading_date)

    def _on_runner_started(self, trading_date: str) -> None:
        self.runner_status_text = f"运行中 / {trading_date}"
        self.runner_stage_text = "主控已启动"
        self.run_button.setEnabled(False)
        self._append_runtime_line(f"[ui] 已启动主控任务: {trading_date}")
        if self.snapshot is not None:
            self._apply_snapshot(self.snapshot)

    def _on_runner_stage_changed(self, stage_text: str) -> None:
        self.runner_stage_text = stage_text
        self._append_runtime_line(f"[ui] 当前阶段: {stage_text}")
        if self.snapshot is not None:
            self._apply_snapshot(self.snapshot)

    def _on_runner_finished(self, success: bool, exit_code: int, message: str) -> None:
        self.runner_status_text = "空闲"
        self.runner_stage_text = "已结束"
        self.run_button.setEnabled(True)
        self._append_runtime_line(f"[ui] {message}")
        self.request_refresh("post-run")

        trading_date = self.trading_date_edit.date().toString("yyyy-MM-dd")
        self.alert_manager.notify_run_finished(success, trading_date, message)
        if not success:
            QMessageBox.critical(
                self,
                "主控运行失败",
                f"{message}\n请检查摘要/日志页中的 stdout/stderr 和阶段状态。",
            )

    def _on_snapshot_ready(self, snapshot: object, duration_ms: int) -> None:
        if not isinstance(snapshot, DashboardSnapshot):
            self._on_refresh_failed("后台刷新返回了无效数据。")
            return

        self.snapshot = snapshot
        self._apply_snapshot(snapshot)
        alerts = self.alert_manager.evaluate_snapshot(snapshot)
        self.alert_manager.process_alerts(alerts)
        self.refresh_button.setEnabled(True)
        self.statusBar().showMessage(f"数据刷新完成，用时 {duration_ms} ms")
        self.logger.info("UI 数据刷新完成: %s ms", duration_ms)

    def _on_refresh_failed(self, message: str) -> None:
        self.refresh_button.setEnabled(True)
        self.statusBar().showMessage("刷新失败，请检查日志。")
        self.logger.error("UI 数据刷新失败: %s", message)
        self.alert_manager.notify_error("刷新失败", f"刷新数据时发生错误：{message}", blocking=False)

    def _cleanup_refresh_thread(self) -> None:
        if self._refresh_worker is not None:
            self._refresh_worker.deleteLater()
        if self._refresh_thread is not None:
            self._refresh_thread.deleteLater()
        self._refresh_worker = None
        self._refresh_thread = None

        if self._refresh_pending:
            self._refresh_pending = False
            self.request_refresh(f"queued-{self._refresh_reason}")

    def _build_alert_status(self, snapshot: DashboardSnapshot) -> str:
        alerts = self.alert_manager.evaluate_snapshot(snapshot)
        if not alerts:
            return "无告警"
        return f"{alerts[0].title} 等 {len(alerts)} 项"

    def _append_runtime_line(self, line: str) -> None:
        self.logger.info(line)
        self.log_viewer.append_line(line)

    def _open_directory(self, path: Path) -> None:
        path.mkdir(parents=True, exist_ok=True)
        ok = QDesktopServices.openUrl(QUrl.fromLocalFile(str(path)))
        if not ok:
            self.alert_manager.notify_error(
                "打开目录失败",
                f"无法打开目录：{path}\n请确认系统资源管理器可用，或手动打开该路径。",
                blocking=False,
            )
