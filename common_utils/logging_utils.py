"""Shared logging configuration for the Streampage project."""
import logging
import os
import sys
import time
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


def _env_flag(name: str, default: bool) -> bool:
    """Read a boolean env var accepting 0/1, true/false, yes/no."""
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


class SharedTimedRotatingFileHandler(TimedRotatingFileHandler):
    """TimedRotatingFileHandler that survives a rollover it is not allowed to do.

    The Streamlit app and any ad-hoc script or scheduled job write the same log
    file, and Windows will not rename a file another process holds open. The stock
    handler is unusable in that situation for two reasons, both of which this class
    fixes:

    * `doRollover` closes the stream *before* renaming, so a failed rename leaves
      the handler with no stream and logging silently stops.
    * It only advances `rolloverAt` after a successful rename, so every subsequent
      record retries the rename and raises again — one failed rollover turns into a
      traceback per log line, which is what buried real output.

    On failure this keeps appending to the current file and defers the next attempt
    to the following period, so at most one process rotates and the others carry on.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._rollover_warned = False

    def doRollover(self):  # noqa: N802 - name fixed by logging
        try:
            super().doRollover()
            self._rollover_warned = False
            return
        except OSError as exc:
            # Reopen first: the base class closed the stream before failing, and
            # without this the handler would drop every later record.
            if self.stream is None:
                try:
                    self.stream = self._open()
                except OSError:
                    pass
            # Defer instead of retrying on every record.
            self.rolloverAt = self.computeRollover(int(time.time()))
            if not self._rollover_warned:
                self._rollover_warned = True
                # Written straight to stderr, not through logging, which would
                # re-enter this handler.
                print(
                    f"[logging] could not rotate {self.baseFilename} "
                    f"({exc.__class__.__name__}: {exc}). Continuing to append to "
                    "the current file; will retry next period.",
                    file=sys.stderr,
                )


def configure_logging(level: int = logging.INFO, log_dir: Optional[str] = None,
                      log_filename: Optional[str] = None) -> None:
    """Configure application-wide logging.

    Console output is always configured. File output keeps seven days of logs and
    tolerates a rollover blocked by another process (see
    `SharedTimedRotatingFileHandler`). Runs once per Python process.

    Environment overrides:
      STREAMPAGE_LOG_DIR         directory for log files
      STREAMPAGE_LOG_FILENAME    log file name (default 'streampage.log')
      STREAMPAGE_LOG_TO_FILE=0   console only — useful for short-lived CLI runs
                                 that should not touch the app's log file at all
      STREAMPAGE_LOG_PER_PROCESS=1
                                 write to '<name>-<pid>.log', which sidesteps
                                 cross-process rotation entirely
    """
    global _LOGGER_CONFIGURED
    if _LOGGER_CONFIGURED:
        return

    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    handlers = []
    if _env_flag("STREAMPAGE_LOG_TO_FILE", True):
        name = log_filename or os.getenv("STREAMPAGE_LOG_FILENAME") or "streampage.log"
        if _env_flag("STREAMPAGE_LOG_PER_PROCESS", False):
            stem, dot, suffix = name.rpartition(".")
            name = f"{stem or suffix}-{os.getpid()}{dot}{suffix if stem else ''}"
        target_dir = _resolve_log_dir(log_dir)
        try:
            target_dir.mkdir(parents=True, exist_ok=True)
            file_handler = SharedTimedRotatingFileHandler(
                target_dir / name,
                when="midnight",
                backupCount=7,
                encoding="utf-8",
                utc=False,
                # Open on first record, so a process that never logs does not hold
                # the file open and block another process's rollover.
                delay=True,
            )
            file_handler.setLevel(level)
            file_handler.setFormatter(formatter)
            handlers.append(file_handler)
        except OSError as exc:
            print(f"[logging] file logging disabled ({exc}); using console only",
                  file=sys.stderr)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    handlers.append(console_handler)

    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    for handler in list(root_logger.handlers):
        root_logger.removeHandler(handler)
    for handler in handlers:
        root_logger.addHandler(handler)

    _LOGGER_CONFIGURED = True


__all__ = ["configure_logging", "SharedTimedRotatingFileHandler"]
