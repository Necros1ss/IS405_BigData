param(
  [Parameter(Mandatory=$true)] [string]$LocalPath,
  [Parameter(Mandatory=$true)] [string]$VmUser,
  [Parameter(Mandatory=$true)] [string]$VmHost,
  [string]$RemotePath = "/home/hadoop/data/",
  [int]$Port = 22
)

# clean_and_transfer.ps1
# PowerShell script: normalize CSV to UTF-8 (no BOM), compute MD5, compress (zip), scp to VM

if (-not (Test-Path $LocalPath)) { Write-Error "Local file not found: $LocalPath"; exit 1 }

$filename = Split-Path $LocalPath -Leaf
$out = Join-Path (Split-Path $LocalPath -Parent) ("clean_$filename")

# Convert to UTF8 without BOM
Get-Content -Raw -Encoding Default $LocalPath | Out-File -FilePath $out -Encoding utf8
Write-Host "Wrote cleaned file: $out"

# Compute MD5
$hash = Get-FileHash -Algorithm MD5 $out
Write-Host "MD5: $($hash.Hash)"

# Create zip archive (works on Windows)
$zipPath = "$out.zip"
if (Test-Path $zipPath) { Remove-Item $zipPath }
Compress-Archive -LiteralPath $out -DestinationPath $zipPath
Write-Host "Compressed to: $zipPath"

# Transfer via scp (requires OpenSSH client on Windows) - fallback instructions provided if scp missing
$scpCmd = "scp -P $Port $zipPath $VmUser@$VmHost:$RemotePath"
Write-Host "Running: $scpCmd"
try {
  iex $scpCmd
  Write-Host "Transfer complete."
} catch {
  Write-Error "scp failed. Ensure OpenSSH client installed on Windows, or use WinSCP/rsync/WSL."
}
