from __future__ import annotations

from PySide6.QtWidgets import QFrame, QGridLayout, QLabel, QSizePolicy

from ui.display_colors import get_label_style
from ui.display_formatters import format_status_panel_value
from ui.display_labels import get_status_panel_label


class StatusPanel(QFrame):
    """Top-level status card grid shown above the report tabs."""

    FIELD_ORDER = (
        ("trading_day", get_status_panel_label("trading_day")),
        ("last_refresh", get_status_panel_label("last_refresh")),
        ("reports_status", get_status_panel_label("reports_status")),
        ("orchestrator_status", get_status_panel_label("orchestrator_status")),
        ("current_stage", get_status_panel_label("current_stage")),
        ("error_status", get_status_panel_label("error_status")),
        ("alert_status", get_status_panel_label("alert_status")),
    )

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self.setFrameShape(QFrame.Shape.StyledPanel)
        self.setObjectName("statusPanel")
        self._value_labels: dict[str, QLabel] = {}
        self._build_ui()

    def update_values(self, values: dict[str, str]) -> None:
        for key, _label in self.FIELD_ORDER:
            target = self._value_labels[key]
            raw_value = values.get(key, "-")
            display_value = format_status_panel_value(raw_value, field_name=key)
            target.setText(display_value)
            target.setStyleSheet(get_label_style(display_value, field_name=key))

    def _build_ui(self) -> None:
        layout = QGridLayout(self)
        layout.setContentsMargins(12, 10, 12, 10)
        layout.setHorizontalSpacing(16)
        layout.setVerticalSpacing(10)

        for index, (key, title) in enumerate(self.FIELD_ORDER):
            row = index // 2
            column = (index % 2) * 2
            title_label = QLabel(title, self)
            title_label.setStyleSheet("font-weight: 600; color: #334155;")
            value_label = QLabel("-", self)
            value_label.setWordWrap(True)
            value_label.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
            value_label.setStyleSheet(get_label_style("-", field_name=key))

            layout.addWidget(title_label, row, column)
            layout.addWidget(value_label, row, column + 1)
            self._value_labels[key] = value_label
