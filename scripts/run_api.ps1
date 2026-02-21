$ErrorActionPreference = "Stop"

. .\.venv\Scripts\Activate.ps1
uvicorn trading_assistant.main:app --reload
