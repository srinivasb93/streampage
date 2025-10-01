"""Shared logging configuration for the Streampage project."""
import logging
import os
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import Optional

_LOGGER_CONFIGURED = False


def _resolve_log_dir(custom_dir: Optional[str] = None) -> Path:
    """Return the directory where log files should be stored."""
    if custom_dir:
        return Path(custom_dir).expanduser().resolve()
    env_dir = os.getenv("STREAMPAGE_LOG_DIR")
    if env_dir:
        return Path(env_dir).expanduser().resolve()
    project_root = Path(__file__).resolve().parents[1]
    return project_root / "logs"


def configure_logging(level: int = logging.INFO, log_dir: Optional[str] = None, log_filename: str = "streampage.log") -> None:
    """Configure application-wide logging.

    Creates a TimedRotatingFileHandler that keeps seven days of logs and ensures
    the configuration only occurs once per Python process.
    """
    global _LOGGER_CONFIGURED
    if _LOGGER_CONFIGURED:
        return

    target_dir = _resolve_log_dir(log_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    log_path = target_dir / log_filename

    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    file_handler = TimedRotatingFileHandler(
        log_path,
        when="midnight",
        backupCount=7,
        encoding="utf-8",
        utc=False,
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    for handler in list(root_logger.handlers):
        root_logger.removeHandler(handler)

    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    _LOGGER_CONFIGURED = True


__all__ = ["configure_logging"]
