from __future__ import annotations

from PySide6.QtWidgets import QGroupBox, QLabel, QPlainTextEdit, QVBoxLayout


class SummaryViewer(QGroupBox):
    """Read-only viewer for text summaries."""

    def __init__(self, title: str, parent=None) -> None:
        super().__init__(title, parent)
        self.status_label = QLabel("等待加载", self)
        self.text_edit = QPlainTextEdit(self)
        self.text_edit.setReadOnly(True)
        self._build_ui()

    def set_content(self, text: str, status: str = "") -> None:
        self.status_label.setText(status or "已加载摘要")
        self.text_edit.setPlainText(text)

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.addWidget(self.status_label)
        layout.addWidget(self.text_edit, stretch=1)
