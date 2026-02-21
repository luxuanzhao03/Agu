from __future__ import annotations

import argparse
import json
from datetime import date, timedelta

from trading_assistant.core.container import get_pipeline_runner
from trading_assistant.core.models import PipelineRunRequest


def _date_arg(value: str) -> date:
    return date.fromisoformat(value)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Trading assistant CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    daily = sub.add_parser("daily-run", help="Run daily batch pipeline")
    daily.add_argument("--symbols", required=True, help="Comma-separated symbols, e.g. 000001,000002")
    daily.add_argument("--start-date", type=_date_arg, default=(date.today() - timedelta(days=120)).isoformat())
    daily.add_argument("--end-date", type=_date_arg, default=date.today().isoformat())
    daily.add_argument("--strategy-name", default="trend_following")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    if args.command == "daily-run":
        symbols = [x.strip() for x in args.symbols.split(",") if x.strip()]
        req = PipelineRunRequest(
            symbols=symbols,
            start_date=args.start_date if isinstance(args.start_date, date) else date.fromisoformat(args.start_date),
            end_date=args.end_date if isinstance(args.end_date, date) else date.fromisoformat(args.end_date),
            strategy_name=args.strategy_name,
        )
        result = get_pipeline_runner().run(req)
        print(json.dumps(result.model_dump(), ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()

