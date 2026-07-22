<#
setup_windows_service_nssm.ps1
Installs or removes a Windows service for this project using NSSM (Non-Sucking Service Manager).

Usage:
  .\setup_windows_service_nssm.ps1                # create service using defaults
  .\setup_windows_service_nssm.ps1 -ServiceName MySvc -PythonPath "C:\Python39\python.exe"
  .\setup_windows_service_nssm.ps1 -Remove

The script will try to find `nssm.exe` in PATH or next to this script. If missing,
it will download the official nssm zip, extract the win64 binary and use it.
#>

param(
    [string]$ServiceName = "ubi_ingest",
    [string]$DisplayName = "ubi_ingest",
    [string]$PythonPath = "",
    [string]$ScriptPath = "",
    [switch]$Remove,
    [string]$NssmUrl = "https://nssm.cc/release/nssm-2.24.zip"
)

function Ensure-Admin {
    if (-not ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
        Write-Host "Restarting elevated..."
        Start-Process -FilePath powershell -Verb runAs -ArgumentList "-NoProfile","-ExecutionPolicy","Bypass","-File",$PSCommandPath -Wait
        exit
    }
}

function Get-NssmExe {
    # Check same folder
    $candidates = @(
        [IO.Path]::Combine($PSScriptRoot, 'nssm.exe'),
        [IO.Path]::Combine($PSScriptRoot, 'nssm-2.24\win64\nssm.exe')
    )

    foreach ($c in $candidates) { if (Test-Path $c) { return (Get-Item $c).FullName } }

    # Check PATH
    $cmd = Get-Command nssm -ErrorAction SilentlyContinue
    if ($cmd) { return $cmd.Source }

    # Download and extract
    Write-Host "nssm.exe not found - downloading from $NssmUrl"
    $tmp = Join-Path $env:TEMP ([IO.Path]::GetRandomFileName() + '.zip')
    try {
        Invoke-WebRequest -Uri $NssmUrl -OutFile $tmp -UseBasicParsing -ErrorAction Stop
        $extractDir = Join-Path $PSScriptRoot 'nssm-extract'
        if (Test-Path $extractDir) { Remove-Item $extractDir -Recurse -Force }
        Expand-Archive -Path $tmp -DestinationPath $extractDir
        $found = Get-ChildItem -Path $extractDir -Filter nssm.exe -Recurse | Where-Object { $_.FullName -like '*win64*' } | Select-Object -First 1
        if ($found) {
            $dest = Join-Path $PSScriptRoot 'nssm.exe'
            Copy-Item $found.FullName $dest -Force
            return $dest
        }
    } catch {
        Write-Warning "Failed to download/extract nssm: $_"
    } finally { if (Test-Path $tmp) { Remove-Item $tmp -Force } }

    throw "nssm.exe not available. Please install NSSM manually and add to PATH."
}

Ensure-Admin

# Detect Python executable. Keep launcher args separate to avoid embedding them into the program path.
# Default to unbuffered output (-u). If using the py launcher, include -3.
$extraInterpreterArgs = '-u'
$pyCmd = Get-Command python -ErrorAction SilentlyContinue
$pyLauncher = Get-Command py -ErrorAction SilentlyContinue
if ($pyCmd) { $PythonPath = $pyCmd.Source }
elseif ($pyLauncher) { $PythonPath = $pyLauncher.Source; $extraInterpreterArgs = '-3 -u' }


if ($ScriptPath -eq "") { $ScriptPath = Join-Path $PSScriptRoot "main.py" }

if ($Remove) {
    $nssm = Get-NssmExe
    Write-Host "Removing service '$ServiceName' via NSSM..."
    Start-Process -FilePath $nssm -ArgumentList @('remove', $ServiceName, 'confirm') -NoNewWindow -Wait
    Write-Host "Removed."
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

$nssm = Get-NssmExe

Write-Host "Installing service '$ServiceName' using NSSM ($nssm)"

try {
    $scriptFull = (Resolve-Path $ScriptPath).Path
} catch {
    $scriptFull = $ScriptPath
}

# Build properly quoted parameter string for NSSM
# Build parameter string: interpreter flags (eg -u -3) then quoted script path then script args
$paramParts = @()
if ($extraInterpreterArgs -ne '') { $paramParts += $extraInterpreterArgs }
$paramParts += '"' + $scriptFull + '"'
$paramStr = $paramParts -join ' '

# If a previous service exists, remove it so install doesn't fail
if (Get-Service -Name $ServiceName -ErrorAction SilentlyContinue) {
    Write-Host "Service '$ServiceName' already exists - removing old service before install..."
    Start-Process -FilePath $nssm -ArgumentList @('remove', $ServiceName, 'confirm') -NoNewWindow -Wait
    Start-Sleep -Milliseconds 200
}

Start-Process -FilePath $nssm -ArgumentList @('install', $ServiceName, $PythonPath, $paramStr) -NoNewWindow -Wait
Start-Sleep -Milliseconds 200

# Set display name and working directory and logs
Start-Process -FilePath $nssm -ArgumentList @('set', $ServiceName, 'DisplayName', $DisplayName) -NoNewWindow -Wait
Start-Process -FilePath $nssm -ArgumentList @('set', $ServiceName, 'AppDirectory', $PSScriptRoot) -NoNewWindow -Wait

$logDir = Join-Path $PSScriptRoot 'logs'
if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Path $logDir | Out-Null }
Start-Process -FilePath $nssm -ArgumentList @('set', $ServiceName, 'AppStdout', (Join-Path $logDir 'stdout.log')) -NoNewWindow -Wait
Start-Process -FilePath $nssm -ArgumentList @('set', $ServiceName, 'AppStderr', (Join-Path $logDir 'stderr.log')) -NoNewWindow -Wait
Start-Process -FilePath $nssm -ArgumentList @('set', $ServiceName, 'AppRotateFiles', '1') -NoNewWindow -Wait

# Ensure NSSM knows the arguments to pass to the app
Start-Process -FilePath $nssm -ArgumentList @('set', $ServiceName, 'AppParameters', $paramStr) -NoNewWindow -Wait

# Make service more tolerant: restart on unexpected exit
Start-Process -FilePath $nssm -ArgumentList @('set', $ServiceName, 'AppRestartDelay', '5000') -NoNewWindow -Wait

Write-Host "Starting service '$ServiceName'..."
Start-Process -FilePath $nssm -ArgumentList @('start', $ServiceName) -NoNewWindow -Wait

Write-Host "Done. Logs are in: $logDir"

