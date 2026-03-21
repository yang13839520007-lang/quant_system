from __future__ import annotations

from collections import deque
from logging.handlers import RotatingFileHandler
from pathlib import Path
import logging


class MemoryLogHandler(logging.Handler):
    """Keep a recent in-memory log window for the UI viewer."""

    def __init__(self, max_lines: int = 1000) -> None:
        super().__init__()
        self._lines: deque[str] = deque(maxlen=max_lines)

    def emit(self, record: logging.LogRecord) -> None:
        self._lines.append(self.format(record))

    def get_text(self) -> str:
        return "\n".join(self._lines)


class LogManager:
    """Configure file + memory logging for the desktop app."""

    def __init__(self, log_dir: Path, max_memory_lines: int = 1500) -> None:
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.log_file = self.log_dir / "quant_ui.log"
        self.logger = logging.getLogger("quant_ui")
        self.logger.setLevel(logging.INFO)
        self.logger.propagate = False

        if not self.logger.handlers:
            formatter = logging.Formatter(
                "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )

            file_handler = RotatingFileHandler(
                self.log_file,
                maxBytes=1_000_000,
                backupCount=3,
                encoding="utf-8",
            )
            file_handler.setFormatter(formatter)

            self.memory_handler = MemoryLogHandler(max_lines=max_memory_lines)
            self.memory_handler.setFormatter(formatter)

            self.logger.addHandler(file_handler)
            self.logger.addHandler(self.memory_handler)
        else:
            self.memory_handler = self._find_memory_handler(self.logger)

        self.logger.info("UI 日志系统已初始化: %s", self.log_file)

    def get_recent_text(self) -> str:
        return self.memory_handler.get_text() if self.memory_handler else ""

    def _find_memory_handler(self, logger: logging.Logger) -> MemoryLogHandler | None:
        for handler in logger.handlers:
            if isinstance(handler, MemoryLogHandler):
                return handler
        return None
