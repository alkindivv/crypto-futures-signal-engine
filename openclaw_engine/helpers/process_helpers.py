"""
Process helpers — PID management utilities, no trading logic.
"""
import os
import signal
from pathlib import Path
from typing import Optional, List


def process_is_alive(pid: Optional[int]) -> bool:
    """Check if a process with given PID is running."""
    if pid is None:
        return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def read_pid_file(path: Path) -> Optional[int]:
    """Read a PID from a file (single integer)."""
    p = Path(path)
    if not p.exists():
        return None
    try:
        content = p.read_text().strip()
        if content:
            return int(content)
    except (ValueError, OSError):
        pass
    return None


def find_process_pids(pattern: str) -> List[int]:
    """Find PIDs matching a process name pattern via /proc scan."""
    pids: List[int] = []
    try:
        for entry in os.listdir("/proc"):
            if entry.isdigit():
                try:
                    cmdline_path = Path(f"/proc/{entry}/cmdline")
                    if cmdline_path.exists():
                        cmdline = cmdline_path.read_text()
                        if pattern in cmdline:
                            pids.append(int(entry))
                except OSError:
                    continue
    except OSError:
        pass
    return pids


def live_trigger_running_pids() -> List[int]:
    """Find all live_trigger_engine process PIDs."""
    return find_process_pids("live_trigger_engine")