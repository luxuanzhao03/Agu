from __future__ import annotations

import atexit
from datetime import datetime, timezone
import os
from pathlib import Path
import shutil
import time
from typing import Iterator
from uuid import uuid4

import pytest


_local_app_data = os.environ.get("LOCALAPPDATA")
if _local_app_data:
    _TMP_ROOT = Path(_local_app_data) / "Temp" / "codex-pytest-cases"
else:
    _TMP_ROOT = Path(".tmp") / "pytest-cases"
_TMP_ROOT.mkdir(parents=True, exist_ok=True)


def _cleanup_path(path: Path, attempts: int = 5) -> None:
    for idx in range(attempts):
        shutil.rmtree(path, ignore_errors=True)
        if not path.exists():
            return
        time.sleep(0.05 * (idx + 1))


@pytest.fixture
def tmp_path() -> Iterator[Path]:
    """
    Project-local tmp_path fixture that avoids system temp ACL issues.
    """
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
    case_dir = _TMP_ROOT / f"case-{ts}-{uuid4().hex[:8]}"
    case_dir.mkdir(parents=True, exist_ok=False)
    yield case_dir
    _cleanup_path(case_dir)


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    """Best-effort cleanup for the fixture root when tests are run directly."""
    _cleanup_path(_TMP_ROOT)


def pytest_unconfigure(config: pytest.Config) -> None:
    _cleanup_path(_TMP_ROOT)


atexit.register(lambda: _cleanup_path(_TMP_ROOT, attempts=10))
