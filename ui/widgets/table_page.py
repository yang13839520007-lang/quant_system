from __future__ import annotations

from typing import Any

import pandas as pd
from PySide6.QtCore import QAbstractTableModel, QModelIndex, QSortFilterProxyModel, Qt
from PySide6.QtGui import QFont
from PySide6.QtWidgets import (
    QAbstractItemView,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QPlainTextEdit,
    QTableView,
    QVBoxLayout,
    QWidget,
)

from ui.data_loader import PageData
from ui.display_colors import get_background_brush, get_foreground_brush
from ui.display_formatters import EMPTY_DISPLAY, format_display_value
from ui.display_labels import get_column_label, get_column_tooltip, get_message, get_page_field_guide


class PandasTableModel(QAbstractTableModel):
    """Minimal pandas-backed table model for Qt views."""

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self._dataframe = pd.DataFrame()

    def set_dataframe(self, dataframe: pd.DataFrame) -> None:
        self.beginResetModel()
        self._dataframe = dataframe.copy()
        self.endResetModel()

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:
        if parent.isValid():
            return 0
        return len(self._dataframe.index)

    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:
        if parent.isValid():
            return 0
        return len(self._dataframe.columns)

    def data(self, index: QModelIndex, role: int = Qt.ItemDataRole.DisplayRole) -> Any:
        if not index.isValid():
            return None

        value = self._dataframe.iat[index.row(), index.column()]
        column_name = str(self._dataframe.columns[index.column()])
        if role == Qt.ItemDataRole.DisplayRole:
            return format_display_value(value, column_name=column_name, context="table")
        if role == Qt.ItemDataRole.ForegroundRole:
            return get_foreground_brush(value, column_name=column_name)
        if role == Qt.ItemDataRole.BackgroundRole:
            return get_background_brush(value, column_name=column_name)
        if role == Qt.ItemDataRole.FontRole:
            if get_foreground_brush(value, column_name=column_name) is not None:
                font = QFont()
                font.setBold(True)
                return font
            return None
        if role == Qt.ItemDataRole.UserRole:
            return self._sortable_value(value)
        return None

    def headerData(self, section: int, orientation: Qt.Orientation, role: int = Qt.ItemDataRole.DisplayRole) -> Any:
        if orientation == Qt.Orientation.Horizontal:
            if not 0 <= section < len(self._dataframe.columns):
                return "" if role == Qt.ItemDataRole.DisplayRole else None

            column_name = str(self._dataframe.columns[section])
            if role == Qt.ItemDataRole.DisplayRole:
                return get_column_label(column_name) or column_name
            if role == Qt.ItemDataRole.ToolTipRole:
                return get_column_tooltip(column_name)
            return None
        if role != Qt.ItemDataRole.DisplayRole:
            return None
        return str(section + 1)

    def _sortable_value(self, value: Any) -> Any:
        if pd.isna(value):
            return ""
        return value


class TableFilterProxyModel(QSortFilterProxyModel):
    """Filter rows by case-insensitive substring across all columns."""

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self._query = ""

    def set_query(self, query: str) -> None:
        self._query = query.casefold().strip()
        self.invalidateFilter()

    def filterAcceptsRow(self, source_row: int, source_parent: QModelIndex) -> bool:
        if not self._query:
            return True

        source_model = self.sourceModel()
        if source_model is None:
            return True

        for column in range(source_model.columnCount()):
            index = source_model.index(source_row, column, source_parent)
            value = str(source_model.data(index, Qt.ItemDataRole.DisplayRole) or "").casefold()
            if self._query in value:
                return True
        return False

    def lessThan(self, left: QModelIndex, right: QModelIndex) -> bool:
        left_value = self.sourceModel().data(left, Qt.ItemDataRole.UserRole)
        right_value = self.sourceModel().data(right, Qt.ItemDataRole.UserRole)

        try:
            return float(left_value) < float(right_value)
        except (TypeError, ValueError):
            return str(left_value) < str(right_value)


class TablePage(QWidget):
    """Generic report page: metadata, search box, sortable table, optional summary."""

    def __init__(self, title: str, show_summary: bool = True, parent=None) -> None:
        super().__init__(parent)
        self.title = title
        self.show_summary = show_summary

        self.title_label = QLabel(title, self)
        self.status_label = QLabel(get_message("waiting_load", "等待加载"), self)
        self.file_label = QLabel("-", self)
        self.field_guide_label = QLabel("", self)
        self.filter_state_label = QLabel(self._build_filter_text(0, 0), self)
        self.search_input = QLineEdit(self)
        self.table_view = QTableView(self)
        self.summary_label = QLabel(get_message("summary_label", "文字摘要"), self)
        self.summary_text = QPlainTextEdit(self)
        self.summary_text.setReadOnly(True)

        self.model = PandasTableModel(self)
        self.proxy_model = TableFilterProxyModel(self)
        self.proxy_model.setSourceModel(self.model)
        self._build_ui()

    def update_page(self, page: PageData) -> None:
        header = self.table_view.horizontalHeader()
        sort_section = header.sortIndicatorSection()
        sort_order = header.sortIndicatorOrder()

        self.title_label.setText(page.spec.title)
        self.status_label.setText(page.status_text)
        self.file_label.setText(page.file_status_text or EMPTY_DISPLAY)
        self._update_field_guide(page)
        self.model.set_dataframe(page.dataframe)
        self.summary_text.setPlainText(page.summary_text if page.summary_text else get_message("no_summary", "暂无摘要"))

        if sort_section >= 0 and sort_section < self.proxy_model.columnCount():
            self.proxy_model.sort(sort_section, sort_order)

        self._apply_column_policy()
        self._update_filter_state()

    def _build_ui(self) -> None:
        root_layout = QVBoxLayout(self)
        root_layout.setContentsMargins(12, 12, 12, 12)
        root_layout.setSpacing(8)

        self.title_label.setStyleSheet("font-size: 16px; font-weight: 600;")
        self.status_label.setWordWrap(True)
        self.file_label.setWordWrap(True)
        self.file_label.setStyleSheet("color: #475569;")
        self.field_guide_label.setWordWrap(True)
        self.field_guide_label.setStyleSheet("background: #f8fafc; color: #334155; border: 1px solid #e2e8f0; border-radius: 6px; padding: 8px;")
        self.field_guide_label.hide()
        self.filter_state_label.setStyleSheet("color: #334155;")

        root_layout.addWidget(self.title_label)
        root_layout.addWidget(self.status_label)
        root_layout.addWidget(self.file_label)
        root_layout.addWidget(self.field_guide_label)

        filter_row = QHBoxLayout()
        filter_row.addWidget(QLabel(get_message("search_label", "关键字筛选"), self))
        self.search_input.setPlaceholderText(get_message("search_placeholder", "输入关键字筛选当前表格"))
        self.search_input.textChanged.connect(self.proxy_model.set_query)
        self.search_input.textChanged.connect(lambda _text: self._update_filter_state())
        filter_row.addWidget(self.search_input, stretch=1)
        filter_row.addWidget(self.filter_state_label)
        root_layout.addLayout(filter_row)

        self.table_view.setModel(self.proxy_model)
        self.table_view.setSortingEnabled(True)
        self.table_view.setAlternatingRowColors(True)
        self.table_view.setWordWrap(False)
        self.table_view.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.table_view.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self.table_view.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.table_view.verticalHeader().setVisible(False)
        horizontal_header = self.table_view.horizontalHeader()
        horizontal_header.setVisible(True)
        horizontal_header.setMinimumHeight(28)
        horizontal_header.setDefaultAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
        horizontal_header.setStretchLastSection(False)
        root_layout.addWidget(self.table_view, stretch=1)

        if self.show_summary:
            root_layout.addWidget(self.summary_label)
            self.summary_text.setPlaceholderText(get_message("no_summary", "暂无摘要"))
            self.summary_text.setMaximumHeight(140)
            root_layout.addWidget(self.summary_text)
        else:
            self.summary_label.hide()
            self.summary_text.hide()

    def _apply_column_policy(self) -> None:
        header = self.table_view.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.ResizeMode.Interactive)

        column_count = self.proxy_model.columnCount()
        if column_count == 0:
            return

        preview_columns = min(4, column_count)
        for column in range(preview_columns):
            header.setSectionResizeMode(column, QHeaderView.ResizeMode.ResizeToContents)

        header.setSectionResizeMode(column_count - 1, QHeaderView.ResizeMode.Stretch)

    def _update_filter_state(self) -> None:
        visible_rows = self.proxy_model.rowCount()
        total_rows = self.model.rowCount()
        self.filter_state_label.setText(self._build_filter_text(visible_rows, total_rows))

    def _build_filter_text(self, visible_rows: int, total_rows: int) -> str:
        return get_message("filter_state_template", "显示 {visible} / {total} 行").format(
            visible=visible_rows,
            total=total_rows,
        )

    def _update_field_guide(self, page: PageData) -> None:
        guide_text = get_page_field_guide(page.spec.key, list(page.dataframe.columns))
        if not guide_text:
            self.field_guide_label.hide()
            self.field_guide_label.clear()
            return
        self.field_guide_label.setText(f"字段说明：{guide_text}")
        self.field_guide_label.show()
