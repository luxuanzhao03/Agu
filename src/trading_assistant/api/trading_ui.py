from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter
from fastapi.responses import FileResponse

router = APIRouter(prefix="/trading", tags=["trading-ui"])

_TRADING_WORKBENCH_HTML = Path(__file__).resolve().parents[1] / "web" / "trading-workbench" / "index.html"


@router.get("/workbench", include_in_schema=False)
def trading_workbench_ui() -> FileResponse:
    return FileResponse(_TRADING_WORKBENCH_HTML)
