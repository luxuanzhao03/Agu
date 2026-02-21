param(
  [switch]$IncludeSourceBytecode
)

$ErrorActionPreference = "Continue"

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$localAppData = [Environment]::GetFolderPath("LocalApplicationData")
$currentUser = if ($env:USERDOMAIN) {
  "$($env:USERDOMAIN)\$($env:USERNAME)"
}
else {
  $env:USERNAME
}

function Reset-AclAndDelete([string]$path) {
  if (-not (Test-Path -LiteralPath $path)) {
    return
  }

  Write-Host "Fixing ACL -> $path"
  cmd /c "takeown /F `"$path`" /R /D Y" 2>$null | Out-Null
  cmd /c "icacls `"$path`" /grant `"${currentUser}:(OI)(CI)F`" /T /C" 2>$null | Out-Null
  cmd /c "attrib -R `"$path`" /S /D" 2>$null | Out-Null

  try {
    Remove-Item -LiteralPath $path -Recurse -Force -ErrorAction Stop
    Write-Host "Removed -> $path"
  }
  catch {
    Write-Warning "Still cannot remove '$path': $($_.Exception.Message)"
  }
}

$targets = @(
  (Join-Path $repoRoot ".pytest_cache"),
  (Join-Path $repoRoot ".tmp\pytest-temp"),
  (Join-Path $repoRoot ".tmp\pytest-cases"),
  (Join-Path $localAppData "Temp\codex-pytest-temp"),
  (Join-Path $localAppData "Temp\codex-pytest-cases")
)

Get-ChildItem -Path $repoRoot -Directory -Filter "pytest-cache-files-*" -ErrorAction SilentlyContinue |
  ForEach-Object { $targets += $_.FullName }

if ($IncludeSourceBytecode) {
  Get-ChildItem -Path (Join-Path $repoRoot "src"), (Join-Path $repoRoot "tests") -Directory -Recurse -Filter "__pycache__" -ErrorAction SilentlyContinue |
    ForEach-Object { $targets += $_.FullName }
}

$targets |
  Where-Object { $_ -and (Test-Path -LiteralPath $_) } |
  Sort-Object -Unique |
  ForEach-Object { Reset-AclAndDelete $_ }
