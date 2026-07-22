<#
setup_windows_service.ps1
Creates or removes a Windows service for this project using the local Python interpreter.

Usage examples:
  .\setup_windows_service.ps1                # create service using defaults
  .\setup_windows_service.ps1 -ServiceName MySvc -PythonPath "C:\Python39\python.exe"
  .\setup_windows_service.ps1 -Remove       # stop and delete service

This script uses `sc.exe` to register the service. For a more robust production deploy
consider installing NSSM (https://nssm.cc) or implementing a pywin32-based service.
#>

param(
    [string]$ServiceName = "ubi_ingest",
    [string]$DisplayName = "ubi_ingest",
    [string]$PythonPath = "",
    [string]$ScriptPath = "",
    [ValidateSet("auto","demand")] [string]$StartMode = "auto",
    [switch]$Remove
)

# Elevate if not running as admin
if (-not ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    Write-Host "Restarting elevated..."
    Start-Process powershell -Verb runAs -ArgumentList "-NoProfile -ExecutionPolicy Bypass -File `"$PSCommandPath`""
    exit
}

if ($PythonPath -eq "") {
    $pyCmd = Get-Command python -ErrorAction SilentlyContinue
    if ($pyCmd) { $PythonPath = $pyCmd.Source }
    else {
        $pyLauncher = Get-Command py -ErrorAction SilentlyContinue
        if ($pyLauncher) { $PythonPath = $pyLauncher.Source + " -3" }
    }
}

if ($ScriptPath -eq "") { $ScriptPath = Join-Path $PSScriptRoot "main.py" }

if ($Remove) {
    Write-Host "Stopping and removing service '$ServiceName'..."
    sc.exe stop $ServiceName | Out-Null
    sc.exe delete $ServiceName | Out-Null
    Write-Host "Service removed."
    exit 0
}

if (-not (Test-Path $ScriptPath)) {
    Write-Error "Script not found: $ScriptPath`nSpecify -ScriptPath or place main.py next to this script."
    exit 1
}

if ($PythonPath -eq "") {
    Write-Error "Python executable not found in PATH. Specify -PythonPath parameter."
    exit 1
}

# Build the binPath argument including quoting so sc.exe receives executable + args
$binPath = '"' + $PythonPath + '" "' + $ScriptPath + '" -u'

Write-Host "Creating service '$ServiceName' -> $binPath"

Start-Process sc.exe -ArgumentList @("create", $ServiceName, "binPath= $binPath", "DisplayName= $DisplayName", "start= $StartMode") -NoNewWindow -Wait
Start-Sleep -Milliseconds 500
Start-Process sc.exe -ArgumentList @("description", $ServiceName, "Runs ubi_ingest main.py") -NoNewWindow -Wait
Start-Sleep -Milliseconds 200

Write-Host "Starting service '$ServiceName'..."
Start-Process sc.exe -ArgumentList @("start", $ServiceName) -NoNewWindow -Wait

Write-Host "Done. Use '-Remove' to uninstall the service.`nIf the service fails to behave like a Windows service, consider using NSSM or a pywin32 service wrapper."
