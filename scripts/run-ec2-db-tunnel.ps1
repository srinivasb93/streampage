<#
.SYNOPSIS
  Forwards a local port to PostgreSQL on the AWS EC2 instance over AWS Systems Manager.

.DESCRIPTION
  Streampage resolves its database endpoint from DB_TARGET, so pointing it at the EC2
  database is a matter of having this forward up and running the app with
  DB_TARGET=ec2 (which resolves to 127.0.0.1:EC2_DB_TUNNEL_PORT, default 15432).

  SSM is used rather than opening 5432 in the security group: the database stays
  unreachable from the internet and no inbound rule is needed.

  Unlike falcon_app's legacy C:\Users\sba400\start-tunnels.ps1, this script never
  performs a broad stop of `aws ssm start-session` processes. It refuses to start
  when the local port is already taken, so an existing DB / API / pgAdmin forward is
  left running. If the legacy script already owns 15432, that forward works and this
  one is unnecessary.

.EXAMPLE
  .\scripts\run-ec2-db-tunnel.ps1
  $env:DB_TARGET = 'ec2'             # in another terminal
  streamlit run streamlit_app.py
#>
[CmdletBinding()]
param(
  [string]$InstanceId = 'i-0ac6de1a71abf4794',
  [string]$AwsProfile = 'dev',
  [ValidateRange(1, 65535)][int]$LocalPort = 15432,
  [ValidateRange(1, 65535)][int]$RemotePort = 5432,
  [string]$AwsExecutable = 'aws',
  [ValidateRange(1, 300)][int]$ReadyTimeoutSeconds = 45
)

$ErrorActionPreference = 'Stop'

function Test-TcpEndpoint {
  param([string]$HostName, [int]$Port, [int]$TimeoutMs = 1000)
  $client = [System.Net.Sockets.TcpClient]::new()
  try {
    $pending = $client.BeginConnect($HostName, $Port, $null, $null)
    if (-not $pending.AsyncWaitHandle.WaitOne($TimeoutMs)) { return $false }
    $client.EndConnect($pending)
    return $client.Connected
  }
  catch { return $false }
  finally { $client.Dispose() }
}

# Refuse to fight over the port. A listener here is almost always a forward that
# already works (this script, or the legacy start-tunnels.ps1), and stopping it
# would break whatever else is using it.
$existing = @(
  Get-NetTCPConnection -State Listen -LocalPort $LocalPort -ErrorAction SilentlyContinue |
    Where-Object { $_.LocalAddress -in @('127.0.0.1', '::1') }
)
if ($existing.Count -gt 0) {
  $owner = Get-CimInstance Win32_Process -Filter "ProcessId = $($existing[0].OwningProcess)" -ErrorAction SilentlyContinue
  Write-Host "Local port $LocalPort is already forwarded by PID $($existing[0].OwningProcess) ($($owner.Name))."
  Write-Host 'Nothing was started and nothing was stopped. Reuse that forward, or stop it yourself first.'
  exit 0
}

foreach ($tool in @($AwsExecutable, 'session-manager-plugin')) {
  if (-not (Get-Command $tool -ErrorAction SilentlyContinue)) {
    throw "$tool was not found on PATH. Install the AWS CLI and the AWS Session Manager plugin."
  }
}

& $AwsExecutable sts get-caller-identity --profile $AwsProfile --output text > $null
if ($LASTEXITCODE -ne 0) {
  throw "AWS profile '$AwsProfile' is not authenticated. Refresh the login, then retry."
}

$parameters = "{`\`"portNumber`\`":[`\`"$RemotePort`\`"],`\`"localPortNumber`\`":[`\`"$LocalPort`\`"]}"
Write-Host "Forwarding 127.0.0.1:$LocalPort -> $InstanceId`:$RemotePort (profile $AwsProfile)..."

$session = Start-Process -FilePath $AwsExecutable -PassThru -WindowStyle Hidden -ArgumentList @(
  'ssm', 'start-session',
  '--target', $InstanceId,
  '--profile', $AwsProfile,
  '--document-name', 'AWS-StartPortForwardingSession',
  '--parameters', $parameters
) -RedirectStandardOutput "$HOME\streampage_tunnel_$LocalPort.log" `
  -RedirectStandardError "$HOME\streampage_tunnel_$LocalPort.err"

try {
  $deadline = (Get-Date).AddSeconds($ReadyTimeoutSeconds)
  while ((Get-Date) -lt $deadline) {
    if ($session.HasExited) {
      throw "The SSM session exited with code $($session.ExitCode). See $HOME\streampage_tunnel_$LocalPort.err"
    }
    if (Test-TcpEndpoint -HostName '127.0.0.1' -Port $LocalPort) {
      Write-Host "Tunnel is up on 127.0.0.1:$LocalPort. Run the app with `$env:DB_TARGET = 'ec2'."
      Write-Host 'Press Ctrl+C to stop this tunnel.'
      break
    }
    Start-Sleep -Milliseconds 500
  }
  if (-not (Test-TcpEndpoint -HostName '127.0.0.1' -Port $LocalPort)) {
    throw "Port $LocalPort did not start accepting connections within $ReadyTimeoutSeconds seconds."
  }

  # Hold the terminal so Ctrl+C reaches the finally block and stops only this child.
  while (-not $session.HasExited) { Start-Sleep -Seconds 2 }
  Write-Host "The SSM session ended with code $($session.ExitCode)."
}
finally {
  if (-not $session.HasExited) {
    Write-Host "Stopping this tunnel's SSM session (PID $($session.Id)) only."
    # /T also stops the session-manager-plugin child that actually holds the socket.
    & taskkill.exe /PID $session.Id /T /F > $null 2>&1
  }
}
