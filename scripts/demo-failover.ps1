# Live failover demo: starts a real 5-process gRPC cluster, writes a value,
# kills the leader process, and shows the cluster elect a new leader and keep
# serving — with the pre-crash write still intact. Narrated for screen recording.
#
#   pwsh scripts/demo-failover.ps1
#
$ErrorActionPreference = "Stop"
$root = Join-Path $PSScriptRoot ".."
$exe = Join-Path $root "bin\raftkv.exe"
Write-Host "Building raftkv..." -ForegroundColor Cyan
& go build -o $exe (Join-Path $root "cmd\raftkv")

$nodes = 1..5
$peers = ($nodes | ForEach-Object { "n$_=127.0.0.1:$(9000 + $_)" }) -join ","
$logDir = Join-Path $root "clusterlogs"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null
$procs = @{}

function Write-Step($msg) { Write-Host "`n=== $msg ===" -ForegroundColor Green }
function Get-NodeStatus($n) {
    try { Invoke-RestMethod -TimeoutSec 2 "http://127.0.0.1:$(8000 + $n)/status" } catch { $null }
}
function Find-Leader {
    for ($try = 0; $try -lt 60; $try++) {
        foreach ($n in $nodes) {
            if (-not $procs[$n] -or $procs[$n].HasExited) { continue }
            $s = Get-NodeStatus $n
            if ($s -and $s.role -eq "Leader") { return @{ n = $n; term = $s.term } }
        }
        Start-Sleep -Milliseconds 250
    }
    throw "no leader found"
}

try {
    Write-Step "Starting 5-node cluster"
    foreach ($n in $nodes) {
        $log = Join-Path $logDir "n$n.log"
        $p = Start-Process -FilePath $exe -PassThru -WindowStyle Hidden `
            -ArgumentList @("--id", "n$n", "--peers", $peers, "--http-addr", "127.0.0.1:$(8000 + $n)") `
            -RedirectStandardError $log -RedirectStandardOutput "$log.out"
        $procs[$n] = $p
        Write-Host "  started n$n (pid $($p.Id))  http=127.0.0.1:$(8000 + $n)"
    }

    Write-Step "Waiting for leader election"
    $leader = Find-Leader
    Write-Host "  leader = n$($leader.n)  (term $($leader.term))" -ForegroundColor Yellow

    Write-Step "Writing city=istanbul via leader n$($leader.n)"
    Invoke-RestMethod -Method Put -TimeoutSec 3 "http://127.0.0.1:$(8000 + $leader.n)/kv/city" -Body "istanbul" | Out-Null
    $read = Invoke-RestMethod -TimeoutSec 3 "http://127.0.0.1:$(8000 + $leader.n)/kv/city"
    Write-Host "  read back: city = $($read.value)"

    Write-Step "KILLING leader n$($leader.n) (pid $($procs[$leader.n].Id))"
    Stop-Process -Id $procs[$leader.n].Id -Force
    $procs[$leader.n].WaitForExit()
    Write-Host "  leader process terminated" -ForegroundColor Red

    Write-Step "Waiting for the cluster to elect a NEW leader"
    $newLeader = Find-Leader
    Write-Host "  new leader = n$($newLeader.n)  (term $($newLeader.term))" -ForegroundColor Yellow

    Write-Step "Cluster still serving: pre-crash write survived, and a new write commits"
    $stillThere = Invoke-RestMethod -TimeoutSec 3 "http://127.0.0.1:$(8000 + $newLeader.n)/kv/city"
    Write-Host "  city (written before crash) = $($stillThere.value)"
    Invoke-RestMethod -Method Put -TimeoutSec 3 "http://127.0.0.1:$(8000 + $newLeader.n)/kv/lang" -Body "go" | Out-Null
    $lang = Invoke-RestMethod -TimeoutSec 3 "http://127.0.0.1:$(8000 + $newLeader.n)/kv/lang"
    Write-Host "  lang (written after crash)  = $($lang.value)"

    Write-Step "Final status of all live nodes"
    foreach ($n in $nodes) {
        $s = Get-NodeStatus $n
        if ($s) { Write-Host ("  n{0}: role={1,-9} term={2} leader={3} commit={4}" -f $n, $s.role, $s.term, $s.leader, $s.commitIndex) }
        else { Write-Host "  n${n}: (down)" -ForegroundColor DarkGray }
    }
    Write-Host "`nDemo complete: the cluster survived a leader crash with no data loss." -ForegroundColor Green
}
finally {
    Write-Host "`nStopping cluster..." -ForegroundColor Cyan
    foreach ($n in $nodes) {
        if ($procs[$n] -and -not $procs[$n].HasExited) { Stop-Process -Id $procs[$n].Id -Force -ErrorAction SilentlyContinue }
    }
}
