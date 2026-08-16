$ErrorActionPreference = "Continue"
Set-Location D:\GIT\master-thesis\src
$pools = @(
  "pool__market_breadth",
  "pool__news_daily",
  "pool__bonds",
  "pool__stock_market",
  "pool__fa",
  "pool__ta"
)
foreach ($p in $pools) {
  Write-Output "=== LAYER1 $p  $(Get-Date -Format 'HH:mm:ss') ==="
  python -m feature_selection.run --ticker VCB --pools "pool__basic,$p" `
    --target return_5day --lookback 20 --horizon 5 `
    --null-draws 10 --device cuda `
    --notes "LAYER 1, 2026-08-16: return_5day sweep. pool__basic + $p. 10-draw null."
  Write-Output "=== DONE $p  exit=$LASTEXITCODE  $(Get-Date -Format 'HH:mm:ss') ==="
}
Write-Output "=== SWEEP COMPLETE $(Get-Date -Format 'HH:mm:ss') ==="
