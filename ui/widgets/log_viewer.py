from __future__ import annotations

from collections import deque

from PySide6.QtWidgets import QGroupBox, QLabel, QPlainTextEdit, QVBoxLayout

from ui.display_labels import get_message


class LogViewer(QGroupBox):
    """Read-only runtime log viewer with line capping."""

    def __init__(self, title: str, max_lines: int = 800, parent=None) -> None:
        super().__init__(title, parent)
        self.max_lines = max_lines
        self._lines: deque[str] = deque(maxlen=max_lines)
        self.status_label = QLabel(get_message("waiting_logs", "等待主控日志输出"), self)
        self.text_edit = QPlainTextEdit(self)
        self.text_edit.setReadOnly(True)
        self._build_ui()

    def append_line(self, line: str) -> None:
        self._lines.append(line)
        self.status_label.setText(self._build_status_text())
        self.text_edit.setPlainText("\n".join(self._lines))
        scrollbar = self.text_edit.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())

    def set_text(self, text: str) -> None:
        self._lines.clear()
        if text:
            for line in text.splitlines():
                self._lines.append(line)
        self.status_label.setText(self._build_status_text())
        self.text_edit.setPlainText("\n".join(self._lines))

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.addWidget(self.status_label)
        layout.addWidget(self.text_edit, stretch=1)

    def _build_status_text(self) -> str:
        return f"已显示最近 {len(self._lines)} 行原始日志"
