#!/usr/bin/env python3
"""Claude Code hooks のイベントデータを DuckDB と Langfuse に蓄積する収集スクリプト."""

import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
from dotenv import load_dotenv
from langfuse import Langfuse

load_dotenv(Path(__file__).parent / ".env")

DB_PATH = Path(__file__).parent / "claude_usage.duckdb"

EVENT_TYPE_MAP = {
    "PreToolUse": "tool",
    "PostToolUse": "tool",
    "UserPromptSubmit": "generation",
}

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS events (
    id INTEGER DEFAULT nextval('events_seq'),
    timestamp TIMESTAMPTZ NOT NULL,
    session_id VARCHAR,
    hook_event_name VARCHAR NOT NULL,
    tool_name VARCHAR,
    tool_input JSON,
    tool_output JSON,
    prompt VARCHAR,
    cwd VARCHAR,
    permission_mode VARCHAR,
    agent_id VARCHAR,
    agent_type VARCHAR,
    raw JSON NOT NULL
)
"""


def init_db() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(str(DB_PATH))
    con.execute("CREATE SEQUENCE IF NOT EXISTS events_seq START 1")
    con.execute(CREATE_TABLE)
    return con


def collect(data: dict) -> None:
    con = init_db()
    con.execute(
        """
        INSERT INTO events (
            timestamp, session_id, hook_event_name,
            tool_name, tool_input, tool_output, prompt,
            cwd, permission_mode, agent_id, agent_type, raw
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            datetime.now(timezone.utc),
            data.get("session_id"),
            data.get("hook_event_name", "unknown"),
            data.get("tool_name"),
            json.dumps(data.get("tool_input")) if data.get("tool_input") else None,
            json.dumps(data.get("tool_output")) if data.get("tool_output") else None,
            data.get("prompt"),
            data.get("cwd"),
            data.get("permission_mode"),
            data.get("agent_id"),
            data.get("agent_type"),
            json.dumps(data),
        ],
    )
    con.close()


def send_to_langfuse(data: dict) -> None:
    """イベントデータを Langfuse にトレースとして送信する."""
    if not os.environ.get("LANGFUSE_SECRET_KEY"):
        return

    langfuse = Langfuse()
    session_id = data.get("session_id")
    event_name = data.get("hook_event_name", "unknown")
    tool_name = data.get("tool_name")
    as_type = EVENT_TYPE_MAP.get(event_name, "span")

    span_name = f"{event_name}:{tool_name}" if tool_name else event_name
    if session_id:
        trace_id = hashlib.md5(session_id.encode()).hexdigest()
        trace_context = {"trace_id": trace_id}
    else:
        trace_context = None

    input_data = data.get("tool_input") or data.get("prompt")
    output_data = data.get("tool_output")
    metadata = {
        k: v
        for k, v in {
            "cwd": data.get("cwd"),
            "permission_mode": data.get("permission_mode"),
            "agent_id": data.get("agent_id"),
            "agent_type": data.get("agent_type"),
        }.items()
        if v is not None
    }

    with langfuse.start_as_current_observation(
        trace_context=trace_context,
        name=span_name,
        as_type=as_type,
        input=input_data,
        output=output_data,
        metadata=metadata or None,
    ):
        pass

    langfuse.flush()
    langfuse.shutdown()


def main():
    raw = sys.stdin.read()
    if not raw.strip():
        return
    data = json.loads(raw)
    collect(data)
    send_to_langfuse(data)


if __name__ == "__main__":
    main()
