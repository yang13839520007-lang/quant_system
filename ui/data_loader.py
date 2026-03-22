from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
import json
import logging
import re
from typing import Any

import pandas as pd
from pandas.errors import EmptyDataError, ParserError

from ui.config import AppConfig, ReportSpec
from ui.display_labels import get_message, get_page_label


CSV_ENCODINGS = ("utf-8-sig", "utf-8", "gb18030", "gbk")
TEXT_ENCODINGS = ("utf-8-sig", "utf-8", "gb18030", "gbk")
CANDIDATES_SUMMARY_DATE_PATTERN = re.compile(r"目标交易日[:：]\s*(\d{4}-\d{2}-\d{2})")


@dataclass
class PageData:
    spec: ReportSpec
    csv_path: Path
    summary_path: Path | None
    dataframe: pd.DataFrame = field(default_factory=pd.DataFrame)
    summary_text: str = ""
    status_text: str = ""
    file_status_text: str = ""
    load_error: str = ""
    row_count: int = 0
    column_count: int = 0
    exists: bool = False
    summary_exists: bool = False
    last_modified_text: str = "-"


@dataclass
class DashboardSnapshot:
    trading_date: str
    refreshed_at: datetime
    reports_dir_exists: bool
    reports_file_count: int
    key_files_present: int
    key_files_total: int
    reports_last_modified: str
    orchestrator_status: str
    current_stage: str
    error_status: str
    pages: dict[str, PageData]
    stage_status_page: PageData
    orchestrator_summary_text: str
    orchestrator_summary_json: dict[str, Any]


class ReportDataLoader:
    """Read report outputs with strong fault tolerance for the UI."""

    def __init__(self, config: AppConfig, logger: logging.Logger) -> None:
        self.config = config
        self.logger = logger

    def load_snapshot(self) -> DashboardSnapshot:
        refreshed_at = datetime.now()
        reports_dir = self.config.reports_dir
        reports_dir_exists = reports_dir.exists()

        pages: dict[str, PageData] = {}
        key_files_present = 0
        for spec in self.config.report_specs:
            page = self._load_page(spec)
            pages[spec.key] = page
            if page.exists:
                key_files_present += 1

        stage_status_page = self._load_page(
            ReportSpec(
                key="stage_status",
                title=get_page_label("stage_status"),
                csv_name="daily_orchestrator_stage_status.csv",
                summary_name=None,
                empty_message="阶段状态文件不存在/尚未生成",
            )
        )

        summary_json_path = reports_dir / "daily_orchestrator_summary.json"
        summary_txt_path = reports_dir / "daily_orchestrator_summary.txt"
        summary_json = self._safe_read_json(summary_json_path)
        summary_text = self._safe_read_text(summary_txt_path, "主控摘要尚未生成。")

        trading_date = self._resolve_trading_date(summary_json, pages)
        reports_file_count, reports_last_modified = self._describe_reports_dir(reports_dir)
        orchestrator_status = self._resolve_orchestrator_status(summary_json, stage_status_page)
        current_stage = self._resolve_current_stage(summary_json, stage_status_page)
        error_status = self._resolve_error_status(reports_dir_exists, summary_json, pages, stage_status_page)

        return DashboardSnapshot(
            trading_date=trading_date,
            refreshed_at=refreshed_at,
            reports_dir_exists=reports_dir_exists,
            reports_file_count=reports_file_count,
            key_files_present=key_files_present,
            key_files_total=len(self.config.report_specs),
            reports_last_modified=reports_last_modified,
            orchestrator_status=orchestrator_status,
            current_stage=current_stage,
            error_status=error_status,
            pages=pages,
            stage_status_page=stage_status_page,
            orchestrator_summary_text=summary_text,
            orchestrator_summary_json=summary_json,
        )

    def _load_page(self, spec: ReportSpec) -> PageData:
        csv_path = self.config.reports_dir / spec.csv_name
        summary_path = self.config.reports_dir / spec.summary_name if spec.summary_name else None

        dataframe, status_text, load_error = self._safe_read_csv(csv_path, spec.empty_message)
        summary_text = self._safe_read_text(summary_path, "摘要文件不存在/尚未生成。") if summary_path else ""

        exists = csv_path.exists()
        row_count = int(len(dataframe.index)) if exists else 0
        column_count = int(len(dataframe.columns)) if exists else 0
        last_modified_text = self._format_timestamp(csv_path) if exists else "-"
        summary_status = summary_path.name if summary_path and summary_path.exists() else "摘要未生成"
        file_status_text = (
            f"{csv_path.name} | {row_count} 行 / {column_count} 列 | 最近更新 {last_modified_text} | {summary_status}"
            if exists
            else f"{csv_path.name} | 文件不存在/尚未生成 | {summary_status}"
        )

        if spec.key == "candidates":
            summary_text, status_text, file_status_text = self._align_candidates_summary(
                dataframe=dataframe,
                summary_text=summary_text,
                status_text=status_text,
                file_status_text=file_status_text,
                summary_exists=bool(summary_path and summary_path.exists()),
            )

        return PageData(
            spec=spec,
            csv_path=csv_path,
            summary_path=summary_path,
            dataframe=dataframe,
            summary_text=summary_text,
            status_text=status_text,
            file_status_text=file_status_text,
            load_error=load_error,
            row_count=row_count,
            column_count=column_count,
            exists=exists,
            summary_exists=bool(summary_path and summary_path.exists()),
            last_modified_text=last_modified_text,
        )

    def _align_candidates_summary(
        self,
        dataframe: pd.DataFrame,
        summary_text: str,
        status_text: str,
        file_status_text: str,
        summary_exists: bool,
    ) -> tuple[str, str, str]:
        if not summary_exists:
            return summary_text, status_text, file_status_text

        table_trade_date = self._extract_page_trade_date(dataframe)
        summary_trade_date = self._extract_candidates_summary_date(summary_text)
        if table_trade_date and summary_trade_date and table_trade_date == summary_trade_date:
            return summary_text, status_text, file_status_text

        warning_text = (
            "摘要与当前交易日不一致。\n"
            f"当前表格交易日：{table_trade_date or '无法识别'}\n"
            f"摘要目标交易日：{summary_trade_date or '无法识别'}\n"
            "为避免误读，当前候选页不展示旧摘要内容。"
        )
        updated_status = f"{status_text} 候选摘要与当前交易日不一致。".strip()
        updated_file_status = f"{file_status_text} | 摘要交易日不一致"
        return warning_text, updated_status, updated_file_status

    def _safe_read_csv(self, path: Path, missing_message: str) -> tuple[pd.DataFrame, str, str]:
        if not path.exists():
            return pd.DataFrame(), missing_message, ""

        last_error = ""
        for encoding in CSV_ENCODINGS:
            try:
                dataframe = pd.read_csv(path, encoding=encoding, on_bad_lines="skip")
                row_count = len(dataframe.index)
                column_count = len(dataframe.columns)
                return dataframe, f"已加载 {row_count} 行 / {column_count} 列。", ""
            except EmptyDataError:
                return pd.DataFrame(), "文件存在，但内容为空。", ""
            except (UnicodeDecodeError, ParserError, ValueError) as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                continue
            except Exception as exc:  # pragma: no cover - defensive branch
                last_error = f"{type(exc).__name__}: {exc}"
                self.logger.exception("读取 CSV 失败: %s", path)
                break

        self.logger.warning("CSV 读取失败，降级为空表: %s | %s", path, last_error)
        return pd.DataFrame(), f"读取失败，已降级为空表。请检查文件编码或列结构。{last_error}", last_error

    def _safe_read_text(self, path: Path | None, missing_message: str) -> str:
        if path is None or not path.exists():
            return missing_message

        for encoding in TEXT_ENCODINGS:
            try:
                return path.read_text(encoding=encoding).strip() or "摘要文件为空。"
            except UnicodeDecodeError:
                continue
            except Exception as exc:  # pragma: no cover - defensive branch
                self.logger.exception("读取文本失败: %s", path)
                return f"读取文本失败：{exc}"

        return "无法解析文本编码。"

    def _safe_read_json(self, path: Path) -> dict[str, Any]:
        if not path.exists():
            return {}
        try:
            with path.open("r", encoding="utf-8") as fh:
                payload = json.load(fh)
            return payload if isinstance(payload, dict) else {}
        except Exception as exc:  # pragma: no cover - defensive branch
            self.logger.warning("读取 JSON 失败: %s | %s", path, exc)
            return {}

    def _resolve_trading_date(self, summary_json: dict[str, Any], pages: dict[str, PageData]) -> str:
        summary_date = str(summary_json.get("trading_date", "")).strip()
        if summary_date:
            return summary_date

        for page in pages.values():
            for column_name in ("trading_date", "trade_date", "date"):
                if column_name in page.dataframe.columns and not page.dataframe.empty:
                    value = str(page.dataframe.iloc[0][column_name]).strip()
                    if value and value.lower() != "nan":
                        return value
        return datetime.now().strftime("%Y-%m-%d")

    def _extract_page_trade_date(self, dataframe: pd.DataFrame) -> str:
        if dataframe.empty:
            return ""
        for column_name in ("trading_date", "trade_date", "date"):
            if column_name in dataframe.columns:
                value = str(dataframe.iloc[0][column_name]).strip()
                if value and value.lower() != "nan":
                    return value
        return ""

    def _extract_candidates_summary_date(self, summary_text: str) -> str:
        if not summary_text:
            return ""
        match = CANDIDATES_SUMMARY_DATE_PATTERN.search(summary_text)
        if not match:
            return ""
        return match.group(1).strip()

    def _describe_reports_dir(self, reports_dir: Path) -> tuple[int, str]:
        if not reports_dir.exists():
            return 0, "报表目录不存在"

        files = [item for item in reports_dir.iterdir() if item.is_file()]
        if not files:
            return 0, "报表目录存在，但当前没有报表文件"

        latest_file = max(files, key=lambda item: item.stat().st_mtime)
        latest_text = f"{latest_file.name} @ {self._format_timestamp(latest_file)}"
        return len(files), latest_text

    def _resolve_orchestrator_status(self, summary_json: dict[str, Any], stage_status_page: PageData) -> str:
        overall_status = str(summary_json.get("overall_status", "")).strip()
        if overall_status:
            acceptance = str(summary_json.get("acceptance_status", "")).strip()
            return f"{overall_status} / {acceptance}" if acceptance else overall_status

        dataframe = stage_status_page.dataframe
        if "stage_status" in dataframe.columns and not dataframe.empty:
            return str(dataframe.iloc[-1]["stage_status"])
        return "NOT_RUN"

    def _resolve_current_stage(self, summary_json: dict[str, Any], stage_status_page: PageData) -> str:
        dataframe = stage_status_page.dataframe
        if {"stage_name", "stage_status"}.issubset(dataframe.columns) and not dataframe.empty:
            last_row = dataframe.iloc[-1]
            return f"{last_row['stage_name']} / {last_row['stage_status']}"

        stage_results = summary_json.get("stage_results", [])
        if isinstance(stage_results, list) and stage_results:
            last_result = stage_results[-1]
            stage_name = str(last_result.get("stage_name", "未知阶段"))
            stage_status = str(last_result.get("stage_status", "UNKNOWN"))
            return f"{stage_name} / {stage_status}"
        return "暂无阶段信息"

    def _resolve_error_status(
        self,
        reports_dir_exists: bool,
        summary_json: dict[str, Any],
        pages: dict[str, PageData],
        stage_status_page: PageData,
    ) -> str:
        if not reports_dir_exists:
            return get_message("reports_missing_mode", "未找到报表目录，已进入空白监控模式")

        if self._count_failed_stages(summary_json, stage_status_page) > 0:
            return "主控存在失败阶段，请查看摘要/日志页"

        page_errors = [page.spec.title for page in pages.values() if page.load_error]
        if page_errors:
            return f"以下页面读取降级为空表：{', '.join(page_errors)}"
        return "正常"

    def _count_failed_stages(self, summary_json: dict[str, Any], stage_status_page: PageData) -> int:
        dataframe = stage_status_page.dataframe
        if "stage_status" in dataframe.columns and not dataframe.empty:
            return int((dataframe["stage_status"].astype(str) == "FAILED").sum())

        stage_results = summary_json.get("stage_results", [])
        if not isinstance(stage_results, list):
            return 0
        return sum(1 for item in stage_results if str(item.get("stage_status", "")).upper() == "FAILED")

    def _format_timestamp(self, path: Path) -> str:
        try:
            return datetime.fromtimestamp(path.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
        except FileNotFoundError:
            return "-"
