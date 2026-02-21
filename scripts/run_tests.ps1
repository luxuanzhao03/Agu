param(
  [Parameter(ValueFromRemainingArguments = $true)]
  [string[]]$PytestArgs
)

$ErrorActionPreference = "Stop"

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$localAppData = [Environment]::GetFolderPath("LocalApplicationData")
$pytestTemp = Join-Path $localAppData "Temp\\codex-pytest-temp"
$caseTemp = Join-Path $localAppData "Temp\\codex-pytest-cases"
$pycacheTemp = Join-Path $pytestTemp "pycache"
$cacheDir = Join-Path $repoRoot ".pytest_cache"
$legacyTmpRoot = Join-Path $repoRoot ".tmp"
$legacyPytestTemp = Join-Path $legacyTmpRoot "pytest-temp"
$legacyPytestCases = Join-Path $legacyTmpRoot "pytest-cases"

New-Item -ItemType Directory -Path $pytestTemp -Force | Out-Null
New-Item -ItemType Directory -Path $caseTemp -Force | Out-Null
New-Item -ItemType Directory -Path $pycacheTemp -Force | Out-Null

# Use user-writable temp paths.
$env:TMP = $pytestTemp
$env:TEMP = $pytestTemp
$env:PYTHONDONTWRITEBYTECODE = "1"
$env:PYTHONPYCACHEPREFIX = $pycacheTemp

Write-Host "TMP/TEMP -> $pytestTemp"
Write-Host "Case TMP -> $caseTemp"
Write-Host "PYCACHE prefix -> $pycacheTemp"
Write-Host "PYTEST cache provider -> disabled"
Write-Host "PYTHONDONTWRITEBYTECODE -> 1"

$pytestOpts = @(
  "--basetemp", $caseTemp,
  "-p", "no:cacheprovider"
)

function Set-PathWritable([string]$path) {
  if (-not (Test-Path -LiteralPath $path)) {
    return
  }
  try {
    Get-ChildItem -LiteralPath $path -Recurse -Force -ErrorAction SilentlyContinue |
      ForEach-Object {
        $_.Attributes = $_.Attributes -band (-bnot [System.IO.FileAttributes]::ReadOnly)
      }
    $item = Get-Item -LiteralPath $path -Force -ErrorAction SilentlyContinue
    if ($null -ne $item) {
      $item.Attributes = $item.Attributes -band (-bnot [System.IO.FileAttributes]::ReadOnly)
    }
  }
  catch {
  }
}

function Remove-PathSafe([string]$path, [string]$label = "Cleaned") {
  if (-not (Test-Path -LiteralPath $path)) {
    return
  }
  Set-PathWritable $path
  $item = Get-Item -LiteralPath $path -Force -ErrorAction SilentlyContinue
  try {
    Remove-Item -LiteralPath $path -Recurse -Force -ErrorAction Stop
    Write-Host "$label -> $path"
  }
  catch {
    Start-Sleep -Milliseconds 200
    Set-PathWritable $path
    try {
      Remove-Item -LiteralPath $path -Recurse -Force -ErrorAction Stop
      Write-Host "$label -> $path"
    }
    catch {
      # Fallback to cmd tools for stubborn Windows ACL/read-only cases.
      try {
        if ($null -ne $item -and $item.PSIsContainer) {
          cmd /c "attrib -R `"$path`" /S /D" 2>$null | Out-Null
          cmd /c "rmdir /S /Q `"$path`"" 2>$null | Out-Null
        }
        else {
          cmd /c "attrib -R `"$path`"" 2>$null | Out-Null
          cmd /c "del /F /Q `"$path`"" 2>$null | Out-Null
        }
      }
      catch {
      }

      if (Test-Path -LiteralPath $path) {
        Write-Warning "Cleanup failed for '$path': $($_.Exception.Message)"
      }
      else {
        Write-Host "$label -> $path"
      }
    }
  }
}

$exitCode = 0
try {
  if ($PytestArgs.Count -eq 0) {
    python -m pytest @pytestOpts
  }
  else {
    python -m pytest @pytestOpts @PytestArgs
  }
  if ($null -ne $LASTEXITCODE) {
    $exitCode = [int]$LASTEXITCODE
  }
}
finally {
  # Remove pytest cache and temporary artifacts.
  Remove-PathSafe $cacheDir
  Remove-PathSafe $pytestTemp
  Remove-PathSafe $caseTemp
  Remove-PathSafe $legacyPytestTemp
  Remove-PathSafe $legacyPytestCases

  # Remove generated pytest scratch folders if any.
  Get-ChildItem -Path $repoRoot -Directory -Filter "pytest-cache-files-*" -ErrorAction SilentlyContinue |
    ForEach-Object { Remove-PathSafe $_.FullName }

  # Remove common test coverage cache artifacts.
  Remove-PathSafe (Join-Path $repoRoot ".coverage")
  Get-ChildItem -Path $repoRoot -File -Filter ".coverage.*" -ErrorAction SilentlyContinue |
    ForEach-Object { Remove-PathSafe $_.FullName }
  Remove-PathSafe (Join-Path $repoRoot "htmlcov")

  Remove-PathSafe (Join-Path $repoRoot ".ruff_cache")
  Remove-PathSafe (Join-Path $repoRoot ".mypy_cache")

  # Remove empty legacy tmp root.
  if (Test-Path -LiteralPath $legacyTmpRoot) {
    try {
      if ((Get-ChildItem -LiteralPath $legacyTmpRoot -Force | Measure-Object).Count -eq 0) {
        Remove-Item -LiteralPath $legacyTmpRoot -Force -ErrorAction Stop
        Write-Host "Cleaned -> $legacyTmpRoot"
      }
    }
    catch {
      Write-Warning "Cleanup failed for '$legacyTmpRoot': $($_.Exception.Message)"
    }
  }
}

exit $exitCode
