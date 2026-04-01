#!/usr/bin/env python3
"""~/.claude/settings.json に ai-analytics の hooks を登録するセットアップスクリプト."""

import json
from pathlib import Path

CLAUDE_SETTINGS = Path.home() / ".claude" / "settings.json"
PROJECT_DIR = Path(__file__).resolve().parent
HOOK_COMMAND = f"uv run --project {PROJECT_DIR} python {PROJECT_DIR / 'main.py'}"

HOOK_EVENTS = [
    "SessionStart",
    "SessionEnd",
    "UserPromptSubmit",
    "PreToolUse",
    "PostToolUse",
    "Stop",
]


def build_hook_entry() -> list:
    return [{"matcher": "", "hooks": [{"type": "command", "command": HOOK_COMMAND}]}]


def setup():
    if CLAUDE_SETTINGS.exists():
        settings = json.loads(CLAUDE_SETTINGS.read_text())
    else:
        CLAUDE_SETTINGS.parent.mkdir(parents=True, exist_ok=True)
        settings = {}

    hooks = settings.setdefault("hooks", {})

    for event in HOOK_EVENTS:
        hooks[event] = build_hook_entry()

    CLAUDE_SETTINGS.write_text(json.dumps(settings, indent=2, ensure_ascii=False) + "\n")
    print(f"hooks を登録しました: {CLAUDE_SETTINGS}")
    print(f"コマンド: {HOOK_COMMAND}")


if __name__ == "__main__":
    setup()
