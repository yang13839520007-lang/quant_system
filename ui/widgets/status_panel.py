from __future__ import annotations

from PySide6.QtWidgets import QFrame, QGridLayout, QLabel, QSizePolicy


class StatusPanel(QFrame):
    """Top-level status card grid shown above the report tabs."""

    FIELD_ORDER = (
        ("trading_day", "当前交易日"),
        ("last_refresh", "最近刷新"),
        ("reports_status", "reports 状态"),
        ("orchestrator_status", "主控运行状态"),
        ("current_stage", "当前阶段"),
        ("error_status", "错误状态"),
        ("alert_status", "告警状态"),
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
            value = str(values.get(key, "-"))
            target.setText(value)
            target.setStyleSheet(self._style_for_value(value))

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
            value_label.setStyleSheet(self._style_for_value("-"))

            layout.addWidget(title_label, row, column)
            layout.addWidget(value_label, row, column + 1)
            self._value_labels[key] = value_label

    def _style_for_value(self, value: str) -> str:
        normalized = value.upper()
        if any(token in normalized for token in ("FAILED", "错误", "异常")):
            return "color: #b91c1c; font-weight: 600;"
        if any(token in normalized for token in ("提醒", "WARNING", "告警")):
            return "color: #b45309; font-weight: 600;"
        if any(token in normalized for token in ("SUCCESS", "正常", "无告警", "完成")):
            return "color: #166534; font-weight: 600;"
        return "color: #0f172a;"
