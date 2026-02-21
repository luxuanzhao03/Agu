from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from trading_assistant.audit.service import AuditService
from trading_assistant.core.models import ReportGenerateRequest, ReportGenerateResult
from trading_assistant.replay.service import ReplayService


class ReportingService:
    def __init__(self, replay: ReplayService, audit: AuditService, output_dir: str = "reports") -> None:
        self.replay = replay
        self.audit = audit
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def generate(self, req: ReportGenerateRequest) -> ReportGenerateResult:
        if req.report_type == "replay":
            return self._build_replay_report(req)
        if req.report_type == "risk":
            return self._build_risk_report(req)
        return self._build_signal_report(req)

    def _build_signal_report(self, req: ReportGenerateRequest) -> ReportGenerateResult:
        events = self.audit.query(event_type="signal_generation", limit=req.limit)
        lines = [
            "# Signal Generation Report",
            "",
            f"- Generated at: {datetime.now(timezone.utc).isoformat()}",
            f"- Watermark: {req.watermark}",
            "",
            "## Recent Runs",
        ]
        for e in events:
            if req.symbol and str(e.payload.get("symbol")) != req.symbol:
                continue
            lines.append(
                f"- {e.event_time.isoformat()} | symbol={e.payload.get('symbol')} "
                f"strategy={e.payload.get('strategy')} signals={e.payload.get('signals')}"
            )
        content = "\n".join(lines)
        path = self._save_if_needed("signal", content, req.save_to_file)
        return ReportGenerateResult(title="Signal Generation Report", content=content, saved_path=path)

    def _build_replay_report(self, req: ReportGenerateRequest) -> ReportGenerateResult:
        report = self.replay.report(
            symbol=req.symbol,
            start_date=req.start_date,
            end_date=req.end_date,
            limit=req.limit,
        )
        lines = [
            "# Execution Replay Report",
            "",
            f"- Generated at: {datetime.now(timezone.utc).isoformat()}",
            f"- Watermark: {req.watermark}",
            f"- Follow Rate: {report.follow_rate:.2%}",
            f"- Avg Slippage (bps): {report.avg_slippage_bps:.2f}",
            f"- Avg Delay (days): {report.avg_delay_days:.2f}",
            "",
            "## Items",
        ]
        for item in report.items:
            lines.append(
                f"- {item.signal_id} | {item.symbol} | signal={item.signal_action.value} "
                f"exec={item.executed_action.value if item.executed_action else 'NONE'} "
                f"qty={item.executed_quantity} delay={item.delay_days}"
            )
        content = "\n".join(lines)
        path = self._save_if_needed("replay", content, req.save_to_file)
        return ReportGenerateResult(title="Execution Replay Report", content=content, saved_path=path)

    def _build_risk_report(self, req: ReportGenerateRequest) -> ReportGenerateResult:
        events = self.audit.query(event_type="risk_check", limit=req.limit)
        lines = [
            "# Risk Check Report",
            "",
            f"- Generated at: {datetime.now(timezone.utc).isoformat()}",
            f"- Watermark: {req.watermark}",
            "",
            "## Recent Risk Checks",
        ]
        for e in events:
            if req.symbol and str(e.payload.get("symbol")) != req.symbol:
                continue
            lines.append(
                f"- {e.event_time.isoformat()} | symbol={e.payload.get('symbol')} "
                f"action={e.payload.get('action')} blocked={e.payload.get('blocked')}"
            )
        content = "\n".join(lines)
        path = self._save_if_needed("risk", content, req.save_to_file)
        return ReportGenerateResult(title="Risk Check Report", content=content, saved_path=path)

    def _save_if_needed(self, prefix: str, content: str, save: bool) -> str | None:
        if not save:
            return None
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        path = self.output_dir / f"{prefix}_report_{ts}.md"
        path.write_text(content, encoding="utf-8")
        return str(path)

