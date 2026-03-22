from __future__ import annotations

from PySide6.QtWidgets import QGroupBox, QLabel, QPlainTextEdit, QVBoxLayout

from ui.display_colors import get_label_style
from ui.display_formatters import format_status_panel_value
from ui.display_labels import get_message


class SummaryViewer(QGroupBox):
    """Read-only viewer for text summaries."""

    def __init__(self, title: str, parent=None) -> None:
        super().__init__(title, parent)
        self.status_label = QLabel(get_message("waiting_load", "等待加载"), self)
        self.text_edit = QPlainTextEdit(self)
        self.text_edit.setReadOnly(True)
        self._build_ui()

    def set_content(self, text: str, status: str = "") -> None:
        display_status = (
            format_status_panel_value(status, field_name="summary_status")
            if status else get_message("loaded_summary", "已加载摘要")
        )
        self.status_label.setText(display_status)
        self.status_label.setStyleSheet(get_label_style(display_status, field_name="summary_status"))
        self.text_edit.setPlainText(text)

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.addWidget(self.status_label)
        layout.addWidget(self.text_edit, stretch=1)
