import asyncio
from contextlib import asynccontextmanager, suppress
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from trading_assistant.api.audit import router as audit_router
from trading_assistant.api.alerts import router as alerts_router
from trading_assistant.api.backtest import router as backtest_router
from trading_assistant.api.compliance import router as compliance_router
from trading_assistant.api.data_governance import router as data_governance_router
from trading_assistant.api.data_license import router as data_license_router
from trading_assistant.api.events import router as events_router
from trading_assistant.api.factors import router as factors_router
from trading_assistant.api.health import router as health_router
from trading_assistant.api.jobs import router as jobs_router
from trading_assistant.api.market import router as market_router
from trading_assistant.api.metrics import router as metrics_router
from trading_assistant.api.model_risk import router as model_risk_router
from trading_assistant.api.ops_ui import router as ops_ui_router
from trading_assistant.api.pipeline import router as pipeline_router
from trading_assistant.api.portfolio import router as portfolio_router
from trading_assistant.api.replay import router as replay_router
from trading_assistant.api.reports import router as reports_router
from trading_assistant.api.research import router as research_router
from trading_assistant.api.risk import router as risk_router
from trading_assistant.api.signals import router as signals_router
from trading_assistant.api.strategies import router as strategies_router
from trading_assistant.api.strategy_governance import router as strategy_governance_router
from trading_assistant.api.system import router as system_router
from trading_assistant.api.trading_ui import router as trading_ui_router
from trading_assistant.core.config import get_settings
from trading_assistant.core.container import get_job_scheduler_worker
from trading_assistant.core.logging import setup_logging

settings = get_settings()
setup_logging(settings.log_level)


@asynccontextmanager
async def _lifespan(_: FastAPI):
    worker = None
    worker_task = None
    if settings.ops_scheduler_enabled:
        worker = get_job_scheduler_worker()
        worker_task = asyncio.create_task(worker.run_forever(), name="ops-job-scheduler")
    try:
        yield
    finally:
        if worker is not None:
            await worker.stop()
        if worker_task is not None:
            worker_task.cancel()
            with suppress(asyncio.CancelledError):
                await worker_task


app = FastAPI(
    title=settings.app_name,
    version="0.8.0",
    description="A-share semi-automated trading assistant foundation. Research and decision support only.",
    lifespan=_lifespan,
)

_web_root = Path(__file__).resolve().parent / "web"
if _web_root.exists():
    app.mount("/ui", StaticFiles(directory=str(_web_root), html=True), name="ui")

app.include_router(health_router)
app.include_router(market_router)
app.include_router(data_governance_router)
app.include_router(data_license_router)
app.include_router(events_router)
app.include_router(factors_router)
app.include_router(strategies_router)
app.include_router(strategy_governance_router)
app.include_router(signals_router)
app.include_router(risk_router)
app.include_router(portfolio_router)
app.include_router(backtest_router)
app.include_router(pipeline_router)
app.include_router(replay_router)
app.include_router(research_router)
app.include_router(reports_router)
app.include_router(compliance_router)
app.include_router(audit_router)
app.include_router(alerts_router)
app.include_router(metrics_router)
app.include_router(model_risk_router)
app.include_router(system_router)
app.include_router(jobs_router)
app.include_router(ops_ui_router)
app.include_router(trading_ui_router)


@app.get("/")
def root() -> dict[str, str]:
    return {"message": "A-share semi-automated trading assistant foundation is running."}
