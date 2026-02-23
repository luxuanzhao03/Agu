@echo off
setlocal

for %%I in ("%~dp0.") do set "SCRIPT_DIR=%%~fI"
cd /d "%SCRIPT_DIR%" || (
  echo [ERROR] Failed to switch directory: %SCRIPT_DIR%
  exit /b 1
)

set "DRY_RUN=0"
set "NO_BROWSER=0"
for %%A in (%*) do (
  if /I "%%~A"=="--dry-run" set "DRY_RUN=1"
  if /I "%%~A"=="--no-browser" set "NO_BROWSER=1"
)

echo [INFO] Project root: %CD%

where python >nul 2>nul
if errorlevel 1 (
  echo [ERROR] Python is not found in PATH. Please install Python 3.11+ first.
  exit /b 1
)

if not exist ".venv\Scripts\activate.bat" (
  echo [INFO] Virtual environment not found, creating .venv ...
  if "%DRY_RUN%"=="1" (
    echo python -m venv .venv
  ) else (
    python -m venv .venv || exit /b 1
  )
)

if not exist ".env" (
  echo [INFO] .env not found, copying from .env.example ...
  if "%DRY_RUN%"=="1" (
    echo copy /Y .env.example .env
  ) else (
    copy /Y ".env.example" ".env" >nul || exit /b 1
  )
)

if "%DRY_RUN%"=="1" (
  echo call .venv\Scripts\activate.bat
) else (
  call ".venv\Scripts\activate.bat" || exit /b 1
)

if "%DRY_RUN%"=="1" (
  echo python -c "import trading_assistant"
) else (
  python -c "import trading_assistant" >nul 2>nul
  if errorlevel 1 (
    echo [INFO] Installing project dependencies ...
    python -m pip install --upgrade pip || exit /b 1
    pip install -e .[dev] || exit /b 1
  )
)

call :CHECK_API
if "%API_READY%"=="1" (
  echo [INFO] API already running: http://127.0.0.1:8000
) else (
  echo [INFO] Starting API in a new terminal window ...
  if "%DRY_RUN%"=="1" (
    echo start "Trading Assistant API" cmd /k "cd /d ""%SCRIPT_DIR%"" ^&^& call .venv\Scripts\activate.bat ^&^& set ""OPS_SCHEDULER_ENABLED=true"" ^&^& uvicorn trading_assistant.main:app --host 127.0.0.1 --port 8000 --reload"
  ) else (
    start "Trading Assistant API" cmd /k "cd /d ""%SCRIPT_DIR%"" && call .venv\Scripts\activate.bat && set ""OPS_SCHEDULER_ENABLED=true"" && uvicorn trading_assistant.main:app --host 127.0.0.1 --port 8000 --reload"
    call :WAIT_API 90
    if not "%API_READY%"=="1" (
      echo [WARN] API did not become ready within 90 seconds.
      echo [WARN] You can check the "Trading Assistant API" window for details.
    )
  )
)

if "%NO_BROWSER%"=="1" goto :DONE

if "%DRY_RUN%"=="1" (
  echo start "" "http://127.0.0.1:8000/ui/"
  echo start "" "http://127.0.0.1:8000/trading/workbench"
  echo start "" "http://127.0.0.1:8000/ops/dashboard"
) else (
  start "" "http://127.0.0.1:8000/ui/"
  start "" "http://127.0.0.1:8000/trading/workbench"
  start "" "http://127.0.0.1:8000/ops/dashboard"
)

:DONE
echo [INFO] Launcher finished.
exit /b 0

:CHECK_API
set "API_READY=0"
if "%DRY_RUN%"=="1" goto :eof
powershell -NoProfile -ExecutionPolicy Bypass -Command "$ErrorActionPreference='Stop';try{$r=Invoke-WebRequest -UseBasicParsing -Uri 'http://127.0.0.1:8000/health' -TimeoutSec 2;if($r.StatusCode -eq 200){exit 0}else{exit 1}}catch{exit 1}" >nul 2>nul
if not errorlevel 1 set "API_READY=1"
goto :eof

:WAIT_API
set "MAX_WAIT=%~1"
if "%MAX_WAIT%"=="" set "MAX_WAIT=90"
set /a ELAPSED=0
set "API_READY=0"
:WAIT_LOOP
call :CHECK_API
if "%API_READY%"=="1" goto :eof
if %ELAPSED% GEQ %MAX_WAIT% goto :eof
timeout /t 1 /nobreak >nul
set /a ELAPSED+=1
goto WAIT_LOOP
