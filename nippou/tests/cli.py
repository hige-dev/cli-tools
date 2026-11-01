#!/usr/bin/env python3
"""実際の CLI を起動し、終了状態・並列処理・端末による下書き制御を検証する。"""

import datetime
import json
import os
from pathlib import Path
import pty
import re
import shlex
import subprocess
import tempfile
from zoneinfo import ZoneInfo


def check(condition, message):
    if not condition:
        raise AssertionError(message)


def main():
    script = Path(__file__).resolve().parents[1] / "nippou.sh"
    today = datetime.datetime.now(ZoneInfo("Asia/Tokyo")).date()
    yesterday = today - datetime.timedelta(days=1)
    with tempfile.TemporaryDirectory(prefix="nippou-cli-") as directory:
        root = Path(directory)
        binary = root / "bin"
        binary.mkdir()
        calls_file = root / "calls.jsonl"
        fake_claude = binary / "claude"
        fake_claude.write_text(
            '''#!/usr/bin/env python3
import json
import os
import sys

args = sys.argv[1:]
prompt = sys.stdin.read()
record = json.dumps({"args": args, "prompt": prompt}, ensure_ascii=False) + "\\n"
fd = os.open(os.environ["NIPPOU_TEST_CALLS"], os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
os.write(fd, record.encode())
os.close(fd)
if "mcp__claude_ai_Slack__slack_send_message_draft" in args:
    if os.environ.get("NIPPOU_TEST_DRAFT_FAIL"):
        print("下書き接続エラー", file=sys.stderr)
        sys.exit(7)
    print("OK")
    sys.exit(0)
day = os.environ.get("NIPPOU_TEST_FAIL_DAY")
if day and ("対象日は " + day) in prompt:
    print("529 Overloaded: 要約の一時エラー", file=sys.stderr)
    sys.exit(7)
print("## 検証施策\\n本人の判断を記録\\n[未確認のリンク](https://example.invalid/made-up)")
if not os.environ.get("NIPPOU_TEST_NO_LISTS"):
    print("やったこと\\n■ 検証\\n  - 判断を記録\\n\\n明日やること\\n■ 検証\\n  - 未確認事項を確認")
''',
            encoding="utf-8",
        )
        fake_claude.chmod(0o700)
        fake_gh = binary / "gh"
        fake_gh.write_text("#!/bin/sh\necho '想定外の GitHub 呼び出し' >&2\nexit 99\n", encoding="utf-8")
        fake_gh.chmod(0o700)
        reports = root / "reports"
        config = root / "config"
        values = {
            "GH_AUTHOR": "",
            "REPORT_DIR": str(reports),
            "HIST_FILE": str(root / "no-history"),
            "CLAUDE_HISTORY": str(root / "no-claude-history"),
            "CLAUDE_PROJECTS_DIR": str(root / "no-projects"),
            "DOC_ROOTS": str(root / "no-docs"),
            "SLACK_TOKEN": "",
            "SLACK_DRAFT_CHANNEL": "C_TEST",
            "GOAL_SHEET_URL": "",
            "GOAL_SHEET_PERSON": "",
            "NOTION_INITIATIVES_URL": "",
            "CLAUDE_MAX_ATTEMPTS": "3",
            "CLAUDE_RETRY_WAIT": "0",
        }
        config.write_text("".join(f"{key}={shlex.quote(value)}\n" for key, value in values.items()), encoding="utf-8")
        config.chmod(0o600)
        environment = dict(os.environ, PATH=str(binary) + os.pathsep + os.environ["PATH"], NIPPOU_CONF=str(config), TZ="Asia/Tokyo", NIPPOU_TEST_CALLS=str(calls_file))

        def note_path(day):
            return reports / "notes" / day.strftime("%Y/%m/%Y-%m-%d.md")

        def report_path(day):
            return reports / day.strftime("%Y/%m/%Y-%m-%d.md")

        for day in (yesterday, today):
            note = note_path(day)
            note.parent.mkdir(parents=True, exist_ok=True)
            note.write_text(f"本人が確認した記録: {day}\n", encoding="utf-8")

        def run_cli(*arguments, terminal=False, **settings):
            calls_file.write_text("", encoding="utf-8")
            env = environment | settings
            master = slave = None
            try:
                if terminal:
                    master, slave = pty.openpty()
                result = subprocess.run(
                    ["bash", str(script), *map(str, arguments)],
                    stdin=slave if terminal else subprocess.DEVNULL,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    env=env,
                    text=True,
                    timeout=30,
                )
            finally:
                if master is not None:
                    os.close(master)
                    os.close(slave)
            calls = [json.loads(line) for line in calls_file.read_text(encoding="utf-8").splitlines()]
            return result, calls

        def drafts(calls):
            return [call for call in calls if "mcp__claude_ai_Slack__slack_send_message_draft" in call["args"]]

        # 当日・単日・対話実行でのみ、一つの下書きを作る。
        result, calls = run_cli(terminal=True)
        check(result.returncode == 0, f"当日生成が失敗しました: {result.stderr}")
        check(len(calls) == 2 and len(drafts(calls)) == 1, "当日生成で下書きが 1 回だけ作られていません")
        summary_args = calls[0]["args"]
        check(summary_args[summary_args.index("--tools") + 1] == "", "要約の組み込みツールが無効ではありません")
        check("--strict-mcp-config" in summary_args and "mcp__*" in summary_args, "要約の MCP ツールが無効ではありません")
        draft = drafts(calls)[0]
        args = draft["args"]
        check(args[args.index("--permission-mode") + 1] == "dontAsk", "下書きの権限制御が不正です")
        check(args[args.index("--setting-sources") + 1] == "", "下書きで利用者設定の権限が読み込まれています")
        allowed = args[args.index("--allowedTools") + 1:args.index("--disallowedTools")]
        check(allowed == ["ToolSearch", "mcp__claude_ai_Slack__slack_send_message_draft"], "下書き以外のツールが許可されています")
        denied = args[args.index("--disallowedTools") + 1:]
        check("mcp__claude_ai_Slack__slack_send_message" in denied and "mcp__claude_ai_Slack__slack_schedule_message" in denied, "送信ツールが禁止されていません")
        payload = json.loads(re.search(r"\n(\{\n.*?\n\})\n", draft["prompt"], re.S).group(1))
        check(payload == {"channel_id": "C_TEST", "message": "やったこと\n\n■ 検証\n  - 判断を記録\n\n明日やること\n\n■ 検証\n  - 未確認事項を確認"}, "下書きの本文・空行・チャンネルが不正です")
        daily = report_path(today).read_text(encoding="utf-8")
        check("#### 検証施策" in daily and "https://example.invalid" not in daily, "見出し・未確認リンクの処理が不正です")

        for arguments, terminal in (((today,), False), ((yesterday,), True), ((today, today), True), (("-j", 2, yesterday, today), True)):
            result, calls = run_cli(*arguments, terminal=terminal)
            check(result.returncode == 0 and not drafts(calls), f"対象外の実行で下書きが作られました: {arguments}")

        result, calls = run_cli(today, terminal=True, NIPPOU_TEST_NO_LISTS="1")
        check(result.returncode == 0 and len(calls) == 1 and not drafts(calls), "一覧のない日報で下書きが作られました")
        note = note_path(today)
        backup = note.with_suffix(".backup")
        note.rename(backup)
        result, calls = run_cli(today, terminal=True)
        backup.rename(note)
        check(result.returncode == 0 and not calls, "記録のない日報で Claude が呼ばれました")

        # 本物の要約失敗とファイル書き込み失敗が、並列集計にも反映される。
        result, calls = run_cli("-j", 2, yesterday, today, NIPPOU_TEST_FAIL_DAY=str(today))
        check(result.returncode != 0 and "成功: 1, 失敗: 1" in result.stdout, "並列実行の要約失敗が成功扱いされています")
        check("529 Overloaded" in result.stdout, "並列実行でエラー原文が表示されません")
        daily = report_path(today).read_text(encoding="utf-8")
        check("要約の生成に失敗しました" in daily and "本人が確認した記録" in daily, "要約失敗時に収集済みの根拠が保存されません")
        result, calls = run_cli(today, terminal=True, NIPPOU_TEST_FAIL_DAY=str(today))
        check(result.returncode == 7 and not drafts(calls), "単日の要約失敗の終了コード・下書き制御が不正です")
        blocked = report_path(today)
        blocked.unlink()
        blocked.mkdir()
        result, calls = run_cli("-j", 2, yesterday, today)
        blocked.rmdir()
        check(result.returncode != 0 and "成功: 1, 失敗: 1" in result.stdout, "書き込み失敗が成功扱いされています")

        result, calls = run_cli(today, terminal=True, NIPPOU_TEST_DRAFT_FAIL="1")
        check(result.returncode == 0 and len(drafts(calls)) == 1, "下書き失敗時の終了状態・試行数が不正です")
        check("警告: Slack の下書きを作成できませんでした" in result.stderr, "下書き失敗の警告がありません")
        print("PASS: 実際の CLI で並列失敗・要約失敗・下書き条件を検証しました")


if __name__ == "__main__":
    main()
