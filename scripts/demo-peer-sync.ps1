[CmdletBinding()]
param(
    [string]$DemoRoot = (Join-Path (Split-Path -Parent $PSScriptRoot) ("mesh0-demo-" + (Get-Date -Format 'yyyyMMdd-HHmmss')))
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$projectRoot = Split-Path -Parent $PSScriptRoot
$mesh = Join-Path $projectRoot 'mesh0.exe'
if (-not (Test-Path -LiteralPath $mesh)) {
    Push-Location $projectRoot
    try {
        go build -o mesh0.exe ./cmd/mesh0
    } finally {
        Pop-Location
    }
}

if (Test-Path -LiteralPath $DemoRoot) {
    throw "Demo destination already exists: $DemoRoot. Choose a new -DemoRoot path."
}

$a = Join-Path $DemoRoot 'device-a'
$b = Join-Path $DemoRoot 'device-b'
New-Item -ItemType Directory -Path $DemoRoot | Out-Null

function Get-Actor([string]$Path) {
    $line = (& $mesh status $Path | Select-String '^  actor\s+').ToString()
    return ($line -replace '^\s*actor\s+', '').Trim()
}

function Get-PublicKey([string]$Path) {
    return (& $mesh peer identity $Path | Select-Object -Last 1).Trim()
}

& $mesh init $a | Out-Null
& $mesh put $a 'tasks/1' 'title="Prepared offline on device A"' 'done=false' | Out-Null
& $mesh replica create $a $b | Out-Null

$actorA = Get-Actor $a
$actorB = Get-Actor $b
$keyA = Get-PublicKey $a
$keyB = Get-PublicKey $b

# Pair and grant each logical device permission to write only the tasks collection.
& $mesh peer add $a 'device-b' $actorB $keyB | Out-Null
& $mesh peer grant $a $actorB 'tasks' | Out-Null
& $mesh peer add $b 'device-a' $actorA $keyA | Out-Null
& $mesh peer grant $b $actorA 'tasks' | Out-Null

# Device B makes an offline update. Device A does not have this document yet.
& $mesh put $b 'tasks/2' 'title="Reported offline on device B"' 'done=true' | Out-Null

$server = Start-Process -FilePath $mesh -ArgumentList @('serve', $b, '--listen', '127.0.0.1:17340') -PassThru -WindowStyle Hidden
Start-Sleep -Milliseconds 750
try {
    & $mesh sync $a '127.0.0.1:17340' $keyB 'device-b' | Out-Null
} finally {
    Stop-Process -Id $server.Id -ErrorAction SilentlyContinue
    Wait-Process -Id $server.Id -Timeout 5 -ErrorAction SilentlyContinue
}

$received = (& $mesh get $a 'tasks/2') -join "`n"
if ($received -notmatch 'Reported offline on device B') {
    throw 'Sync finished without the expected device-B document on device A.'
}

Write-Host ''
Write-Host 'MESH0 PEER-SYNC DEMO PASSED' -ForegroundColor Green
Write-Host "Device A received device B's offline update over authenticated localhost TLS."
Write-Host "Inspect the two replicas at: $DemoRoot"
Write-Host 'Try: '
Write-Host "  & '$mesh' get '$a' tasks/2"
