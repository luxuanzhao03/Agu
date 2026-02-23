param(
  [string]$StartDate = "",
  [string]$EndDate = "",
  [double]$Principal = 2000,
  [int]$LotSize = 100,
  [double]$CashBufferRatio = 0.10,
  [double]$MaxSinglePosition = 0.60,
  [double]$MinEdgeBps = 140,
  [int]$MaxSymbols = 0,
  [int]$SleepMs = 0,
  [int]$TopN = 30
)

$ErrorActionPreference = "Stop"

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Push-Location $repoRoot
try {
  if (-not $StartDate) {
    $StartDate = (Get-Date).AddDays(-240).ToString("yyyy-MM-dd")
  }
  if (-not $EndDate) {
    $EndDate = (Get-Date).AddDays(-1).ToString("yyyy-MM-dd")
  }

  $pythonExe = ".\.venv\Scripts\python.exe"
  if (-not (Test-Path $pythonExe)) {
    throw "Python venv not found at $pythonExe. Run scripts/bootstrap.ps1 first."
  }

  # Disable broken local proxy for this run.
  $env:ALL_PROXY = ""
  $env:HTTP_PROXY = ""
  $env:HTTPS_PROXY = ""
  $env:GIT_HTTP_PROXY = ""
  $env:GIT_HTTPS_PROXY = ""

  & $pythonExe .\scripts\full_market_pick_2000.py `
    --start-date $StartDate `
    --end-date $EndDate `
    --principal $Principal `
    --lot-size $LotSize `
    --cash-buffer-ratio $CashBufferRatio `
    --max-single-position $MaxSinglePosition `
    --min-edge-bps $MinEdgeBps `
    --max-symbols $MaxSymbols `
    --sleep-ms $SleepMs `
    --top-n $TopN `
    --output-jsonl reports/full_market_signals_2000.jsonl `
    --output-summary reports/full_market_summary_2000.json `
    --output-csv reports/buy_candidates_2000.csv
}
finally {
  Pop-Location
}

