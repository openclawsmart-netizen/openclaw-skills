@echo off
setlocal

echo [1/2] Stopping Windows desktop GUI...
taskkill /FI "WINDOWTITLE eq Live Monitor Desktop*" /T /F >nul 2>nul
taskkill /IM python.exe /FI "WINDOWTITLE eq Live Monitor Desktop*" /T /F >nul 2>nul

echo [2/2] Stopping WSL web API service...
wsl -e bash -lc "pkill -f 'scripts/live_monitor_web.py.*8787' || true"

echo Done.
endlocal
