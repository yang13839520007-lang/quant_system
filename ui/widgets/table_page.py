from __future__ import annotations

from typing import Any

import pandas as pd
from PySide6.QtCore import QAbstractTableModel, QModelIndex, QSortFilterProxyModel, Qt
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
        if role == Qt.ItemDataRole.DisplayRole:
            if pd.isna(value):
                return ""
            return str(value)
        if role == Qt.ItemDataRole.UserRole:
            return self._sortable_value(value)
        return None

    def headerData(self, section: int, orientation: Qt.Orientation, role: int = Qt.ItemDataRole.DisplayRole) -> Any:
        if role != Qt.ItemDataRole.DisplayRole:
            return None
        if orientation == Qt.Orientation.Horizontal:
            if 0 <= section < len(self._dataframe.columns):
                return str(self._dataframe.columns[section])
            return ""
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
        self.status_label = QLabel("等待加载", self)
        self.file_label = QLabel("-", self)
        self.filter_state_label = QLabel("显示 0 / 0 行", self)
        self.search_input = QLineEdit(self)
        self.table_view = QTableView(self)
        self.summary_label = QLabel("摘要", self)
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
        self.file_label.setText(page.file_status_text)
        self.model.set_dataframe(page.dataframe)
        self.summary_text.setPlainText(page.summary_text if page.summary_text else "暂无摘要")

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
        self.filter_state_label.setStyleSheet("color: #334155;")

        root_layout.addWidget(self.title_label)
        root_layout.addWidget(self.status_label)
        root_layout.addWidget(self.file_label)

        filter_row = QHBoxLayout()
        filter_row.addWidget(QLabel("搜索/筛选", self))
        self.search_input.setPlaceholderText("输入关键字，按行过滤")
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
        self.table_view.horizontalHeader().setStretchLastSection(False)
        root_layout.addWidget(self.table_view, stretch=1)

        if self.show_summary:
            root_layout.addWidget(self.summary_label)
            self.summary_text.setPlaceholderText("暂无摘要")
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

        last_column = column_count - 1
        header.setSectionResizeMode(last_column, QHeaderView.ResizeMode.Stretch)

    def _update_filter_state(self) -> None:
        visible_rows = self.proxy_model.rowCount()
        total_rows = self.model.rowCount()
        self.filter_state_label.setText(f"显示 {visible_rows} / {total_rows} 行")
