"""対話的 SQL シェル"""

import readline
import shutil
import subprocess
import sys
import uuid
from pathlib import Path

import duckdb


def _build_claude_context(db_path: str, summary: str, provider) -> str:
    """claude に渡す初回コンテキストを構築する"""
    venv_hint = ""
    if sys.prefix != sys.base_prefix:
        activate = Path(sys.prefix) / "bin" / "activate"
        venv_hint = (
            "venv 環境で実行しています。duckdb コマンドを使う前に "
            f"activate してください:\n  source {activate}\n"
        )

    return f"""{provider.name} ログが DuckDB に取り込まれています。分析を手伝ってください。

DB ファイル: {db_path}
{venv_hint}
{summary}

{provider.get_help_text()}

duckdb コマンドで SQL を実行して分析してください。"""


def _launch_claude(
    message: str,
    db_path: str,
    summary: str,
    provider,
    session_id: str,
    is_first: bool,
) -> None:
    """claude を対話的に起動する"""
    if not shutil.which("claude"):
        print("エラー: claude コマンドが見つかりません", file=sys.stderr)
        return

    if is_first:
        context = _build_claude_context(db_path, summary, provider)
        prompt = f"{context}\n\n---\n\n{message}"
        cmd = ["claude", "--session-id", session_id, prompt]
    else:
        cmd = ["claude", "-r", session_id, message]

    subprocess.run(cmd)
    print("\n-- SQL シェルに戻りました (.help でヘルプ表示)")


def sql_shell(
    db: duckdb.DuckDBPyConnection,
    db_path: str,
    summary: str,
    provider,
) -> None:
    """対話的 SQL シェルを起動する"""
    help_text = provider.get_help_text()
    print(help_text)

    claude_session_id = str(uuid.uuid4())
    claude_first = True

    while True:
        try:
            query = input(provider.shell_prompt).strip()
        except (EOFError, KeyboardInterrupt):
            print("\n終了します。")
            break

        if not query:
            continue
        if query.lower() in (".quit", ".exit", "quit", "exit"):
            break
        if query.lower() in (".help", "help"):
            print(help_text)
            continue
        if query.lower() == ".tables":
            db.sql(
                "SELECT table_name, table_type FROM information_schema.tables "
                "WHERE table_schema = 'main' ORDER BY table_type, table_name"
            ).show()
            continue
        if query.lower() == ".schema":
            db.sql(f"DESCRIBE {provider.table_name}").show()
            continue

        # .claude コマンド
        if query.lower().startswith(".claude"):
            message = query[len(".claude"):].strip()
            if not message:
                print("使い方: .claude <質問>")
                continue
            db.close()
            _launch_claude(
                message, db_path, summary, provider,
                claude_session_id, claude_first,
            )
            claude_first = False
            db = duckdb.connect(db_path)
            provider.create_views(db)
            continue

        # セミコロンで終わるまで複数行入力
        while not query.endswith(";"):
            try:
                line = input("  -> ").strip()
                query += " " + line
            except (EOFError, KeyboardInterrupt):
                query = ""
                break

        if not query:
            continue

        try:
            result = db.sql(query)
            result.show()
        except Exception as e:
            print(f"エラー: {e}", file=sys.stderr)
