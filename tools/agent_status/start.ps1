# Starts the agent status panel (port 8977) in a hidden window.
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
Start-Process -WindowStyle Hidden -WorkingDirectory $here -FilePath python -ArgumentList "server.py"
Write-Host "agent_status: http://localhost:8977/"
