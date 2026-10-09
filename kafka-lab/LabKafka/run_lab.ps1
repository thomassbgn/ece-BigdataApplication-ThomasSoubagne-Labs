$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot
function Check-Exit { if ($LASTEXITCODE -ne 0) { throw "Command failed ($LASTEXITCODE)" } }
if (-not (Test-Path -LiteralPath '.venv/Scripts/python.exe')) {
    python -m venv .venv
    Check-Exit
}
$taskPython = Join-Path $PSScriptRoot '.venv/Scripts/python.exe'
& $taskPython -m pip install -r requirements.txt
Check-Exit
New-Item -ItemType Directory -Force data,output | Out-Null
if (-not (Test-Path -LiteralPath 'data/around_the_world_in_80_days.txt')) {
    Invoke-WebRequest -UseBasicParsing 'https://www.gutenberg.org/cache/epub/103/pg103.txt' -OutFile 'data/around_the_world_in_80_days.txt'
}
docker compose up -d
Check-Exit
& $taskPython admin.py
Check-Exit
$taskRunId = [guid]::NewGuid().ToString()
& $taskPython producer.py --run-id $taskRunId
Check-Exit
& $taskPython consumer.py --run-id $taskRunId
Check-Exit
& $taskPython verify_output.py
Check-Exit

