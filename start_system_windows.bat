@echo off
setlocal

for %%I in ("%~dp0.") do set "SCRIPT_DIR=%%~fI"
cd /d "%SCRIPT_DIR%" || (
  echo [ERROR] Failed to switch directory: %SCRIPT_DIR%
  exit /b 1
)

set "VENV_DIR=%SCRIPT_DIR%\.venv"
set "VENV_PY=%VENV_DIR%\Scripts\python.exe"

set "DRY_RUN=0"
set "NO_BROWSER=0"
set "SHOW_API_WINDOW=1"
for %%A in (%*) do (
  if /I "%%~A"=="--dry-run" set "DRY_RUN=1"
  if /I "%%~A"=="--no-browser" set "NO_BROWSER=1"
  if /I "%%~A"=="--hide-api-window" set "SHOW_API_WINDOW=0"
)

echo [INFO] Project root: %CD%

where python >nul 2>nul
if errorlevel 1 (
  echo [ERROR] Python is not found in PATH. Please install Python 3.11+ first.
  exit /b 1
)

if not exist "%VENV_PY%" (
  echo [INFO] Virtual environment not found, creating .venv ...
  if "%DRY_RUN%"=="1" (
    echo python -m venv .venv
  ) else (
    python -m venv .venv || exit /b 1
  )
)

if not exist "%VENV_PY%" (
  echo [ERROR] Virtual environment Python not found: %VENV_PY%
  exit /b 1
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
  echo "%VENV_PY%" -c "import trading_assistant"
) else (
  "%VENV_PY%" -c "import trading_assistant" >nul 2>nul
  if errorlevel 1 (
    echo [INFO] Installing project dependencies ...
    "%VENV_PY%" -m pip install --upgrade pip || exit /b 1
    "%VENV_PY%" -m pip install -e .[dev] || exit /b 1
  )
)

call :SELF_CHECK_RUNTIME
if not "%SELF_CHECK_OK%"=="1" (
  echo [INFO] Runtime self-check failed, reinstalling dependencies in project venv ...
  if "%DRY_RUN%"=="1" (
    echo "%VENV_PY%" -m pip install --upgrade pip
    echo "%VENV_PY%" -m pip install -e .[dev]
  ) else (
    "%VENV_PY%" -m pip install --upgrade pip || exit /b 1
    "%VENV_PY%" -m pip install -e .[dev] || exit /b 1
  )
  call :SELF_CHECK_RUNTIME
  if not "%SELF_CHECK_OK%"=="1" (
    echo [ERROR] Runtime self-check still failed after reinstall.
    exit /b 1
  )
)

call :CHECK_API
if "%API_READY%"=="1" (
  set "API_OWNER_MATCH=0"
  call :GET_API_PROCESS_INFO
  if defined API_PID (
    if not defined API_PROC_PATH (
      echo [WARN] Unable to read running API python path, keeping existing process.
      set "API_OWNER_MATCH=1"
    ) else (
      if /I "%API_PROC_PATH%"=="%VENV_PY%" set "API_OWNER_MATCH=1"
    )
  )

  if "%API_OWNER_MATCH%"=="1" (
    if "%SHOW_API_WINDOW%"=="1" (
      if defined API_PID (
        echo [INFO] API already running with project venv Python: %API_PROC_PATH%
        echo [INFO] Restarting API so you get a visible console window ...
        if "%DRY_RUN%"=="1" (
          echo taskkill /PID %API_PID% /F
        ) else (
          taskkill /PID %API_PID% /F >nul 2>nul
          timeout /t 1 /nobreak >nul
        )
        set "API_READY=0"
      ) else (
        echo [WARN] API is running but PID lookup failed, keeping existing process.
      )
    ) else (
      echo [INFO] API already running with project venv Python: %API_PROC_PATH%
    )
  ) else (
    echo [WARN] API already running on 127.0.0.1:8000 but not from project venv.
    if defined API_PROC_PATH (
      echo [WARN] Running Python: %API_PROC_PATH%
    ) else (
      echo [WARN] Running process PID: %API_PID%
    )
    echo [INFO] Restarting API with project venv Python: %VENV_PY%
    if "%DRY_RUN%"=="1" (
      if defined API_PID echo taskkill /PID %API_PID% /F
    ) else (
      if defined API_PID taskkill /PID %API_PID% /F >nul 2>nul
      timeout /t 1 /nobreak >nul
    )
    set "API_READY=0"
  )
)

if not "%API_READY%"=="1" (
  echo [INFO] Starting API in a new terminal window ...
  if "%DRY_RUN%"=="1" (
    if "%SHOW_API_WINDOW%"=="1" (
      echo start "Trading Assistant API" cmd /k "cd /d ""%SCRIPT_DIR%"" ^&^& set ""OPS_SCHEDULER_ENABLED=true"" ^&^& ""%VENV_PY%"" -m uvicorn trading_assistant.main:app --host 127.0.0.1 --port 8000"
    ) else (
      echo start "Trading Assistant API" /MIN cmd /c "cd /d ""%SCRIPT_DIR%"" ^&^& set ""OPS_SCHEDULER_ENABLED=true"" ^&^& ""%VENV_PY%"" -m uvicorn trading_assistant.main:app --host 127.0.0.1 --port 8000"
    )
  ) else (
    if "%SHOW_API_WINDOW%"=="1" (
      start "Trading Assistant API" cmd /k "cd /d ""%SCRIPT_DIR%"" && set ""OPS_SCHEDULER_ENABLED=true"" && ""%VENV_PY%"" -m uvicorn trading_assistant.main:app --host 127.0.0.1 --port 8000"
    ) else (
      start "Trading Assistant API" /MIN cmd /c "cd /d ""%SCRIPT_DIR%"" && set ""OPS_SCHEDULER_ENABLED=true"" && ""%VENV_PY%"" -m uvicorn trading_assistant.main:app --host 127.0.0.1 --port 8000"
    )
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

:SELF_CHECK_RUNTIME
set "SELF_CHECK_OK=0"
set "SELF_CHECK_CMD=import importlib.util as u,sys;mods=['trading_assistant','fastapi','uvicorn','pandas','tushare','akshare'];missing=[m for m in mods if u.find_spec(m) is None];print('[CHECK] python='+sys.executable);print('[CHECK] missing=' + (','.join(missing) if missing else 'none'));raise SystemExit(0 if not missing else 2)"
if "%DRY_RUN%"=="1" (
  echo "%VENV_PY%" -c "%SELF_CHECK_CMD%"
  set "SELF_CHECK_OK=1"
  goto :eof
)
"%VENV_PY%" -c "%SELF_CHECK_CMD%"
if not errorlevel 1 set "SELF_CHECK_OK=1"
goto :eof

:CHECK_API
set "API_READY=0"
if "%DRY_RUN%"=="1" goto :eof
powershell -NoProfile -ExecutionPolicy Bypass -Command "$ErrorActionPreference='Stop';try{$r=Invoke-WebRequest -UseBasicParsing -Uri 'http://127.0.0.1:8000/health' -TimeoutSec 2;if($r.StatusCode -eq 200){exit 0}else{exit 1}}catch{exit 1}" >nul 2>nul
if not errorlevel 1 set "API_READY=1"
goto :eof

:GET_API_PROCESS_INFO
set "API_PID="
set "API_PROC_PATH="
for /f "tokens=5" %%a in ('netstat -ano ^| findstr ":8000" ^| findstr "LISTENING"') do (
  set "API_PID=%%a"
  goto :GET_API_PROCESS_INFO_PID_FOUND
)
for /f "usebackq delims=" %%P in (`powershell -NoProfile -ExecutionPolicy Bypass -Command "$c=Get-NetTCPConnection -LocalPort 8000 -State Listen -ErrorAction SilentlyContinue | Select-Object -First 1; if($c){$c.OwningProcess}"`) do (
  set "API_PID=%%P"
)
:GET_API_PROCESS_INFO_PID_FOUND
if not defined API_PID goto :eof
if "%DRY_RUN%"=="1" goto :eof
for /f "usebackq delims=" %%P in (`powershell -NoProfile -ExecutionPolicy Bypass -Command "$p=Get-Process -Id %API_PID% -ErrorAction SilentlyContinue; if($p -and $p.Path){$p.Path}"`) do (
  set "API_PROC_PATH=%%P"
)
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
