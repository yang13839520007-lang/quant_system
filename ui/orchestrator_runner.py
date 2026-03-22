from __future__ import annotations

import locale
import os
from pathlib import Path
import logging
import re
import subprocess
import threading

from PySide6.QtCore import QObject, QThread, Signal

from ui.config import AppConfig
from ui.display_labels import format_status_text


STAGE_PATTERN = re.compile(r"\[Stage\s+(?P<stage_no>\d+)\]\s+(?P<stage_name>.+?)\s+->\s+(?P<status>[A-Z_]+)")
FALLBACK_ENCODINGS = ("utf-8", "gbk", "cp936")


class OrchestratorWorker(QObject):
    """Run the existing orchestrator in a background thread."""

    output_line = Signal(str)
    stage_changed = Signal(str)
    finished = Signal(bool, int, str)

    def __init__(self, command: list[str], workdir: Path, logger: logging.Logger) -> None:
        super().__init__()
        self.command = command
        self.workdir = Path(workdir)
        self.logger = logger

    def run(self) -> None:
        self.logger.info("启动主控子进程: %s", self.command)
        try:
            process = subprocess.Popen(
                self.command,
                cwd=str(self.workdir),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=False,
                bufsize=0,
                env=self._build_process_env(),
            )
        except FileNotFoundError as exc:
            target = exc.filename or self.command[0]
            message = f"无法启动主控，找不到可执行文件：{target}"
            self.logger.exception(message)
            self.finished.emit(False, -1, message)
            return
        except Exception as exc:  # pragma: no cover - defensive branch
            message = f"无法启动主控进程：{exc}"
            self.logger.exception(message)
            self.finished.emit(False, -1, message)
            return

        def consume(stream, prefix: str) -> None:
            if stream is None:
                return
            for raw_line in iter(stream.readline, b""):
                line = self._decode_output_line(raw_line)
                if not line:
                    continue
                payload = f"[{prefix}] {line}"
                self.output_line.emit(payload)
                self._emit_stage_if_needed(line)
            stream.close()

        stdout_thread = threading.Thread(target=consume, args=(process.stdout, "stdout"), daemon=True)
        stderr_thread = threading.Thread(target=consume, args=(process.stderr, "stderr"), daemon=True)
        stdout_thread.start()
        stderr_thread.start()
        stdout_thread.join()
        stderr_thread.join()

        exit_code = process.wait()
        success = exit_code == 0
        message = "主控运行完成。" if success else f"主控运行失败，退出码 {exit_code}。"
        self.logger.info(message)
        self.finished.emit(success, exit_code, message)

    def _build_process_env(self) -> dict[str, str]:
        env = os.environ.copy()
        # Prefer UTF-8 for the Python child process, but keep decode fallback on the parent side.
        env.setdefault("PYTHONIOENCODING", "utf-8")
        env.setdefault("PYTHONUTF8", "1")
        return env

    def _decode_output_line(self, raw_line: bytes) -> str:
        for encoding in _candidate_encodings():
            try:
                return raw_line.decode(encoding).rstrip("\r\n")
            except UnicodeDecodeError:
                continue
        return raw_line.decode("utf-8", errors="replace").rstrip("\r\n")

    def _emit_stage_if_needed(self, line: str) -> None:
        match = STAGE_PATTERN.search(line)
        if not match:
            return
        stage_name = match.group("stage_name").strip()
        status = format_status_text(match.group("status").strip())
        self.stage_changed.emit(f"{stage_name} / {status}")


class OrchestratorRunner(QObject):
    """Thread manager around the orchestrator subprocess worker."""

    run_started = Signal(str)
    output_line = Signal(str)
    stage_changed = Signal(str)
    run_finished = Signal(bool, int, str)
    start_rejected = Signal(str)

    def __init__(self, config: AppConfig, logger: logging.Logger) -> None:
        super().__init__()
        self.config = config
        self.logger = logger
        self._thread: QThread | None = None
        self._worker: OrchestratorWorker | None = None
        self._is_running = False

    @property
    def is_running(self) -> bool:
        return self._is_running

    def start(self, trading_date: str) -> bool:
        if self._is_running:
            self.start_rejected.emit("当前已有主控任务在执行，请等待本次运行结束。")
            return False

        interpreter_path = Path(self.config.python_executable)
        if not interpreter_path.exists():
            self.start_rejected.emit(
                f"Python 解释器不存在：{interpreter_path}。请检查 ui/ui_config.toml 的 python_executable。"
            )
            return False

        if not self.config.orchestrator_entry.exists():
            self.start_rejected.emit(
                f"主控桥接入口不存在：{self.config.orchestrator_entry}。请检查 ui/ui_config.toml 的 orchestrator_entry。"
            )
            return False

        command = [
            str(interpreter_path),
            str(self.config.orchestrator_entry),
            "--project-root",
            str(self.config.project_root),
            "--trading-date",
            trading_date,
        ]

        self._thread = QThread()
        self._worker = OrchestratorWorker(command=command, workdir=self.config.project_root, logger=self.logger)
        self._worker.moveToThread(self._thread)
        self._thread.started.connect(self._worker.run)
        self._worker.output_line.connect(self.output_line.emit)
        self._worker.stage_changed.connect(self.stage_changed.emit)
        self._worker.finished.connect(self._handle_finished)
        self._worker.finished.connect(self._thread.quit)
        self._thread.finished.connect(self._cleanup_thread)

        self._is_running = True
        self.run_started.emit(trading_date)
        self._thread.start()
        return True

    def _handle_finished(self, success: bool, exit_code: int, message: str) -> None:
        self._is_running = False
        self.run_finished.emit(success, exit_code, message)

    def _cleanup_thread(self) -> None:
        if self._worker is not None:
            self._worker.deleteLater()
        if self._thread is not None:
            self._thread.deleteLater()
        self._worker = None
        self._thread = None


def _candidate_encodings() -> tuple[str, ...]:
    preferred = locale.getpreferredencoding(False).strip().lower()
    encodings: list[str] = []
    for encoding in (*FALLBACK_ENCODINGS, preferred):
        normalized = encoding.strip().lower()
        if normalized and normalized not in encodings:
            encodings.append(normalized)
    return tuple(encodings)
