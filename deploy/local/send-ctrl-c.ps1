<#
.SYNOPSIS
  Helper for stop-xtnl.ps1: sends Ctrl+C to every process on the (hidden) console of -TargetPid.

.DESCRIPTION
  Run it in its own hidden PowerShell (stop-xtnl.ps1 does): it leaves its own console, attaches to the target's
  console, ignores Ctrl+C itself and raises CTRL_C_EVENT for the whole console. start-xtnl.ps1 then stops,
  run-local.ps1 stops, and the engine receives SIGINT and drains (end_reason SIGINT in engine_runs).
  Exit codes: 0 sent, 2 could not attach to the target's console, 3 the event could not be raised.
  Phase 1 never sends orders; this touches only local processes.
#>
param([Parameter(Mandatory = $true)][int]$TargetPid)
$ErrorActionPreference = 'Stop'
if (-not ('XtnlCtrlC' -as [type])) {
    Add-Type -TypeDefinition @'
using System;
using System.Runtime.InteropServices;
public static class XtnlCtrlC {
    [DllImport("kernel32.dll")] public static extern bool FreeConsole();
    [DllImport("kernel32.dll", SetLastError = true)] public static extern bool AttachConsole(uint processId);
    [DllImport("kernel32.dll", SetLastError = true)] public static extern bool SetConsoleCtrlHandler(IntPtr handler, bool add);
    [DllImport("kernel32.dll", SetLastError = true)] public static extern bool GenerateConsoleCtrlEvent(uint ctrlEvent, uint processGroupId);
}
'@
}
[void][XtnlCtrlC]::FreeConsole()
if (-not [XtnlCtrlC]::AttachConsole([uint32]$TargetPid)) { [Environment]::Exit(2) }
[void][XtnlCtrlC]::SetConsoleCtrlHandler([IntPtr]::Zero, $true)   # this helper ignores the event itself
if (-not [XtnlCtrlC]::GenerateConsoleCtrlEvent(0, 0)) { [Environment]::Exit(3) }   # CTRL_C_EVENT, whole console
Start-Sleep -Seconds 2
[void][XtnlCtrlC]::FreeConsole()
[Environment]::Exit(0)
