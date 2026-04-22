$base = "c:\Users\gandh\Downloads\DC-Project\payflow\payflow"
$src = Join-Path $base "placeholder"

$dirs = @("coordinator", "worker", "payment-log")
foreach ($dir in $dirs) {
    $target = Join-Path $base $dir
    New-Item -Path $target -ItemType Directory -Force | Out-Null
    Copy-Item (Join-Path $src "main.go") (Join-Path $target "main.go") -Force
    Copy-Item (Join-Path $src "Dockerfile") (Join-Path $target "Dockerfile") -Force
    Write-Host "Copied placeholder to $dir"
}

# Also need to replace gateway with placeholder since existing gateway won't build standalone
$gwTarget = Join-Path $base "gateway"
Copy-Item (Join-Path $src "main.go") (Join-Path $gwTarget "main.go") -Force
Copy-Item (Join-Path $src "Dockerfile") (Join-Path $gwTarget "Dockerfile") -Force
Write-Host "Replaced gateway with placeholder for Week 1"
