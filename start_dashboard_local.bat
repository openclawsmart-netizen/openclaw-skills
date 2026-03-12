@echo off
setlocal

echo [1/4] Checking Python on Windows...
where python >nul 2>nul
if %errorlevel%==0 (
  set "PY_CMD=python"
) else (
  where py >nul 2>nul
  if %errorlevel%==0 (
    set "PY_CMD=py -3"
  ) else (
    echo [ERROR] Python is not installed on Windows.
    echo Please install Python 3 from https://www.python.org/downloads/windows/
    echo Make sure "Add python.exe to PATH" is enabled.
    pause
    exit /b 1
  )
)

echo [2/4] Starting WSL web API service (127.0.0.1:8787)...
wsl -e bash -lc "cd /root/openclaw-skills && (pkill -f 'scripts/live_monitor_web.py.*8787' || true) && nohup python3 scripts/live_monitor_web.py --host 0.0.0.0 --port 8787 >/tmp/live_monitor_web.out 2>&1 &"
if not %errorlevel%==0 (
  echo [WARN] Failed to start WSL service automatically. Please verify WSL is installed and running.
)

echo [3/4] Waiting for service startup...
timeout /t 2 /nobreak >nul

echo [4/4] Starting local desktop GUI...
start "Live Monitor Desktop" cmd /c "%PY_CMD% scripts\live_monitor_desktop.py"

echo Done. If GUI shows API errors, check WSL log: /tmp/live_monitor_web.out
endlocal
