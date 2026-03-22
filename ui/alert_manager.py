from __future__ import annotations

from dataclasses import dataclass
import logging

import pandas as pd
from PySide6.QtCore import QObject
from PySide6.QtGui import QIcon
from PySide6.QtWidgets import QApplication, QMessageBox, QStyle, QSystemTrayIcon, QWidget

from ui.data_loader import DashboardSnapshot, PageData


KEYWORD_COLUMNS = ("action", "next_day_action", "management_action", "risk_review_note", "action_reason")


@dataclass(frozen=True)
class AlertRecord:
    code: str
    severity: str
    title: str
    message: str


class AlertManager(QObject):
    """Centralized alert evaluation and desktop notifications."""

    def __init__(self, logger: logging.Logger, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.logger = logger
        self.parent_widget = parent
        self._seen_alerts: set[str] = set()
        self._tray_icon = self._build_tray_icon(parent)

    def evaluate_snapshot(self, snapshot: DashboardSnapshot) -> list[AlertRecord]:
        alerts: list[AlertRecord] = []

        if not snapshot.reports_dir_exists:
            alerts.append(
                AlertRecord(
                    code="reports_dir_missing",
                    severity="warning",
                    title="报表目录未就绪",
                    message="报表目录不存在，界面已按空白监控模式启动。",
                )
            )

        failed_stage_count = self._count_failed_stages(snapshot)
        if failed_stage_count > 0:
            alerts.append(
                AlertRecord(
                    code="orchestrator_failed",
                    severity="error",
                    title="主控失败提醒",
                    message=f"检测到 {failed_stage_count} 个失败阶段，请查看摘要/日志页。",
                )
            )

        open_execution_page = snapshot.pages.get("open_execution")
        if open_execution_page is not None:
            buy_count = self._count_buy_signals(open_execution_page.dataframe)
            if buy_count > 0:
                alerts.append(
                    AlertRecord(
                        code="buy_signal",
                        severity="info",
                        title="可执行买入提醒",
                        message=f"开盘执行委托中检测到 {buy_count} 条买入信号。",
                    )
                )

        keyword_hits = self._detect_risk_keywords(
            snapshot.pages.get("close_review"),
            snapshot.pages.get("next_day_management"),
            snapshot.pages.get("portfolio"),
        )
        if keyword_hits:
            alerts.append(
                AlertRecord(
                    code="risk_keyword",
                    severity="warning",
                    title="止损/止盈提醒",
                    message=f"检测到关键词：{', '.join(keyword_hits[:4])}",
                )
            )

        intraday_page = snapshot.pages.get("intraday_recheck")
        if intraday_page is not None:
            intraday_count = len(intraday_page.dataframe.index)
            intraday_text = intraday_page.summary_text
            if intraday_count > 0 or any(word in intraday_text for word in ("改判", "确认", "recheck")):
                alerts.append(
                    AlertRecord(
                        code="intraday_recheck",
                        severity="warning",
                        title="盘中改判提醒",
                        message=f"盘中复检存在 {intraday_count} 条记录，请及时确认。",
                    )
                )

        degraded_pages = [page.spec.title for page in snapshot.pages.values() if page.load_error]
        if degraded_pages:
            alerts.append(
                AlertRecord(
                    code="report_read_error",
                    severity="error",
                    title="报表读取异常",
                    message=f"以下页面读取失败后已降级为空表：{', '.join(degraded_pages)}",
                )
            )

        return alerts

    def process_alerts(self, alerts: list[AlertRecord]) -> None:
        for alert in alerts:
            signature = f"{alert.code}:{alert.title}:{alert.message}"
            if signature in self._seen_alerts:
                continue
            self._seen_alerts.add(signature)
            self._show_alert(alert)

    def notify_run_finished(self, success: bool, trading_date: str, message: str) -> None:
        severity = "info" if success else "error"
        title = "主控运行完成提醒" if success else "主控运行失败提醒"
        alert = AlertRecord(
            code=f"run_finished_{'success' if success else 'failure'}",
            severity=severity,
            title=title,
            message=f"{trading_date} | {message}",
        )
        self._show_alert(alert, force=True, blocking=not success)

    def notify_error(self, title: str, message: str, blocking: bool = True) -> None:
        self._show_alert(
            AlertRecord(code="manual_error", severity="error", title=title, message=message),
            force=True,
            blocking=blocking,
        )

    def _show_alert(self, alert: AlertRecord, force: bool = False, blocking: bool = False) -> None:
        self.logger.warning("UI 提醒 | %s | %s", alert.title, alert.message)
        if self._tray_icon is not None:
            self._tray_icon.showMessage(
                alert.title,
                alert.message,
                self._icon_for_message(alert.severity),
                6000,
            )

        if blocking or (alert.severity == "error" and force):
            message_box = QMessageBox(self.parent_widget)
            message_box.setWindowTitle(alert.title)
            message_box.setText(alert.message)
            message_box.setIcon(self._message_box_icon(alert.severity))
            message_box.exec()

    def _build_tray_icon(self, parent: QWidget | None) -> QSystemTrayIcon | None:
        if not QSystemTrayIcon.isSystemTrayAvailable():
            return None

        app = QApplication.instance()
        if app is None:
            return None

        style = app.style()
        icon: QIcon = style.standardIcon(QStyle.StandardPixmap.SP_ComputerIcon)
        tray_icon = QSystemTrayIcon(icon, parent)
        tray_icon.setToolTip("A股量化交易监控终端")
        tray_icon.show()
        return tray_icon

    def _icon_for_message(self, severity: str) -> QSystemTrayIcon.MessageIcon:
        if severity == "error":
            return QSystemTrayIcon.MessageIcon.Critical
        if severity == "warning":
            return QSystemTrayIcon.MessageIcon.Warning
        return QSystemTrayIcon.MessageIcon.Information

    def _message_box_icon(self, severity: str) -> QMessageBox.Icon:
        if severity == "error":
            return QMessageBox.Icon.Critical
        if severity == "warning":
            return QMessageBox.Icon.Warning
        return QMessageBox.Icon.Information

    def _count_failed_stages(self, snapshot: DashboardSnapshot) -> int:
        dataframe = snapshot.stage_status_page.dataframe
        if "stage_status" in dataframe.columns and not dataframe.empty:
            return int((dataframe["stage_status"].astype(str) == "FAILED").sum())

        stage_results = snapshot.orchestrator_summary_json.get("stage_results", [])
        if not isinstance(stage_results, list):
            return 0
        return sum(1 for item in stage_results if str(item.get("stage_status", "")).upper() == "FAILED")

    def _count_buy_signals(self, dataframe: pd.DataFrame) -> int:
        if dataframe.empty:
            return 0

        for column_name in ("action", "order_type", "status"):
            if column_name in dataframe.columns:
                series = dataframe[column_name].astype(str)
                buy_hits = series.str.contains("BUY|买", case=False, regex=True, na=False)
                if buy_hits.any():
                    return int(buy_hits.sum())
        return int(len(dataframe.index))

    def _detect_risk_keywords(self, *pages: PageData | None) -> list[str]:
        keywords = ("止损", "止盈", "stop_loss", "stoploss", "take_profit", "takeprofit")
        hits: list[str] = []
        for page in pages:
            if page is None:
                continue

            for keyword in keywords:
                if keyword in page.summary_text and keyword not in hits:
                    hits.append(keyword)

            dataframe = page.dataframe
            if dataframe.empty:
                continue

            for column_name in dataframe.columns:
                if column_name not in KEYWORD_COLUMNS and "action" not in column_name.lower():
                    continue
                series = dataframe[column_name].astype(str)
                for keyword in keywords:
                    if keyword in hits:
                        continue
                    if series.str.contains(keyword, case=False, regex=False, na=False).any():
                        hits.append(keyword)
        return hits
