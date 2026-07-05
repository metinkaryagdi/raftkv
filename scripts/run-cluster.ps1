# Launches a local 5-node raftkv cluster as separate processes.
# Raft RPCs use gRPC on ports 9001-9005; the client HTTP API is on 8001-8005.
# Logs are written to .\clusterlogs\nN.log. Press Ctrl+C or run stop-cluster.ps1
# (or Get-Process raftkv | Stop-Process) to shut the cluster down.
param(
    [int]$Nodes = 5,
    [string]$BinDir = "$PSScriptRoot\..\bin"
)

$ErrorActionPreference = "Stop"
$exe = Join-Path $BinDir "raftkv.exe"
if (-not (Test-Path $exe)) {
    Write-Host "Building raftkv..." -ForegroundColor Cyan
    & go build -o $exe "$PSScriptRoot\..\cmd\raftkv"
}

# Build the shared peers list (id=host:port for every node).
$peerParts = @()
for ($i = 1; $i -le $Nodes; $i++) { $peerParts += "n$i=127.0.0.1:$(9000 + $i)" }
$peers = $peerParts -join ","

$logDir = Join-Path $PSScriptRoot "..\clusterlogs"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null

Write-Host "Starting $Nodes-node cluster (peers: $peers)" -ForegroundColor Green
for ($i = 1; $i -le $Nodes; $i++) {
    $httpPort = 8000 + $i
    $log = Join-Path $logDir "n$i.log"
    Start-Process -FilePath $exe `
        -ArgumentList @("--id", "n$i", "--peers", $peers, "--http-addr", "127.0.0.1:$httpPort") `
        -RedirectStandardError $log -RedirectStandardOutput "$log.out" -WindowStyle Hidden
    Write-Host "  n$i  ->  raft 127.0.0.1:$(9000 + $i)   http 127.0.0.1:$httpPort   log $log"
}

Write-Host ""
Write-Host "Cluster up. Try:" -ForegroundColor Yellow
Write-Host '  curl.exe http://127.0.0.1:8001/status'
Write-Host '  curl.exe -X PUT http://127.0.0.1:8001/kv/city -d istanbul   # redirects if not leader'
Write-Host "Stop with:  Get-Process raftkv | Stop-Process"
