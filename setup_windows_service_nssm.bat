@echo off
REM Batch shim to run the NSSM-based PowerShell service setup script
pushd %~dp0
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0setup_windows_service_nssm.ps1" %*
popd
