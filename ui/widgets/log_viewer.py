from __future__ import annotations

from collections import deque

from PySide6.QtWidgets import QGroupBox, QLabel, QPlainTextEdit, QVBoxLayout


class LogViewer(QGroupBox):
    """Read-only runtime log viewer with line capping."""

    def __init__(self, title: str, max_lines: int = 800, parent=None) -> None:
        super().__init__(title, parent)
        self.max_lines = max_lines
        self._lines: deque[str] = deque(maxlen=max_lines)
        self.status_label = QLabel("等待运行日志", self)
        self.text_edit = QPlainTextEdit(self)
        self.text_edit.setReadOnly(True)
        self._build_ui()

    def append_line(self, line: str) -> None:
        self._lines.append(line)
        self.status_label.setText(f"最近 {len(self._lines)} 行")
        self.text_edit.setPlainText("\n".join(self._lines))
        scrollbar = self.text_edit.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())

    def set_text(self, text: str) -> None:
        self._lines.clear()
        if text:
            for line in text.splitlines():
                self._lines.append(line)
        self.status_label.setText(f"最近 {len(self._lines)} 行")
        self.text_edit.setPlainText("\n".join(self._lines))

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.addWidget(self.status_label)
        layout.addWidget(self.text_edit, stretch=1)
