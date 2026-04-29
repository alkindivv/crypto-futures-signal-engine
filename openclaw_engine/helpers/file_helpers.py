"""
File helpers — pure I/O utilities, no trading logic.
"""
import json
from pathlib import Path
from typing import Any


def load_json_file(path: Path, default: Any) -> Any:
    """Load JSON file, return default if missing or invalid."""
    p = Path(path)
    if not p.exists():
        return default
    try:
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return default


def save_json_file(path: Path, payload: Any) -> None:
    """Atomically save JSON file (write to temp, then rename)."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    tmp.rename(p)


def append_jsonl(path: Path, payload: Any) -> None:
    """Append a JSON-serializable record to a .jsonl file."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "a", encoding="utf-8") as f:
        line = json.dumps(payload, ensure_ascii=False)
        f.write(line + "\n")