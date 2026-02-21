from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter
from fastapi.responses import FileResponse

router = APIRouter(prefix="/ops", tags=["ops-ui"])

_OPS_DASHBOARD_HTML = Path(__file__).resolve().parents[1] / "web" / "ops-dashboard" / "index.html"


@router.get("/dashboard", include_in_schema=False)
def ops_dashboard_ui() -> FileResponse:
    return FileResponse(_OPS_DASHBOARD_HTML)
