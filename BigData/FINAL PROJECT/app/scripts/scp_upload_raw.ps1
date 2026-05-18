param(
  [Parameter(Mandatory=$true)] [string]$LocalCsv,
  [Parameter(Mandatory=$true)] [string]$VmUser,
  [Parameter(Mandatory=$true)] [string]$VmHost,
  [string]$RemoteDir = "/home/thinh/data/",
  [int]$Port = 22
)

if (-not (Test-Path $LocalCsv)) { Write-Error "Local CSV not found: $LocalCsv"; exit 1 }

Write-Host "Uploading $LocalCsv -> $VmUser@$VmHost:$RemoteDir"
$scp = "scp -P $Port `"$LocalCsv`" $VmUser@$VmHost:`"$RemoteDir`""
Write-Host $scp
try {
  iex $scp
  Write-Host "Upload complete."
} catch {
  Write-Error "Upload failed. Ensure OpenSSH client (scp) available on Windows or use WinSCP/WSL rsync."
}
