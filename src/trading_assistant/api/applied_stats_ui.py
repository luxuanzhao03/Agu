from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter
from fastapi.responses import FileResponse

router = APIRouter(prefix="/applied-stats", tags=["applied-stats-ui"])

_APPLIED_STATS_SHOWCASE_HTML = (
    Path(__file__).resolve().parents[1] / "web" / "applied-stats-showcase" / "index.html"
)


@router.get("/showcase", include_in_schema=False)
def applied_stats_showcase_ui() -> FileResponse:
    return FileResponse(_APPLIED_STATS_SHOWCASE_HTML)

