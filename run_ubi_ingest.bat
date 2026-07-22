@echo off
REM Run ubi_ingest minimized and redirect stdout/stderr to logs
set SCRIPT_DIR=%~dp0
if not exist "%SCRIPT_DIR%logs" mkdir "%SCRIPT_DIR%logs"
pushd "%SCRIPT_DIR%"
start "" /min python "%SCRIPT_DIR%main.py" > "%SCRIPT_DIR%logs\out.log" 2> "%SCRIPT_DIR%logs\err.log"
popd
exit /b 0
