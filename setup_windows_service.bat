@echo off
REM Batch shim to run the PowerShell service setup script with elevated policy
pushd %~dp0
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0setup_windows_service.ps1" %*
popd
