"""
Structured logger for the ETL pipeline.
Writes to both console (stdout) and a rotating daily log file.
"""
import logging
import os
import sys
from datetime import datetime
from logging.handlers import RotatingFileHandler

import yaml

# ── Resolve project root & config ────────────────────────────────────────────
_HERE = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.abspath(os.path.join(_HERE, "..", ".."))
_CONFIG_PATH = os.path.join(_PROJECT_ROOT, "config", "config.yaml")


def _load_logs_dir() -> str:
    try:
        with open(_CONFIG_PATH, "r") as f:
            cfg = yaml.safe_load(f)
        return os.path.join(_PROJECT_ROOT, cfg["paths"]["logs_dir"])
    except Exception:
        return os.path.join(_PROJECT_ROOT, "logs")


def get_logger(name: str = "etl") -> logging.Logger:
    """
    Return a configured logger with the given name.
    Safe to call multiple times — handlers are not duplicated.

    Usage::
        from src.utils.logger import get_logger
        log = get_logger(__name__)
        log.info("Starting extraction")
    """
    logger = logging.getLogger(name)

    if logger.handlers:
        # Already configured — return as-is
        return logger

    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # ── Console handler ───────────────────────────────────────────────────────
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.INFO)
    console.setFormatter(formatter)
    logger.addHandler(console)

    # ── File handler (rotating, max 10 MB, keep 5 backups) ───────────────────
    logs_dir = _load_logs_dir()
    os.makedirs(logs_dir, exist_ok=True)
    log_file = os.path.join(logs_dir, f"etl_{datetime.now():%Y%m%d}.log")

    file_handler = RotatingFileHandler(
        log_file, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    logger.propagate = False
    return logger
