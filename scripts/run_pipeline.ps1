param(
  [string]$Symbols = "000001,000002",
  [string]$StartDate = "2025-01-01",
  [string]$EndDate = "2025-12-31",
  [string]$Strategy = "trend_following"
)

$ErrorActionPreference = "Stop"

. .\.venv\Scripts\Activate.ps1
python -m trading_assistant.cli daily-run --symbols $Symbols --start-date $StartDate --end-date $EndDate --strategy-name $Strategy
