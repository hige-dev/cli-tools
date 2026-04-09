"""共通ユーティリティ"""

import sys
from datetime import datetime, timezone


def die(msg: str) -> None:
    print(f"エラー: {msg}", file=sys.stderr)
    sys.exit(1)


def pick_one(prompt: str, items: list[str]) -> str:
    """対話的に一つ選択する"""
    if not items:
        die(f"{prompt}: 選択肢がありません")

    if len(items) == 1:
        return items[0]

    print(f"\n{prompt}:")
    for i, item in enumerate(items, 1):
        print(f"  {i}) {item}")

    while True:
        try:
            choice = input("番号を入力> ").strip()
            idx = int(choice) - 1
            if 0 <= idx < len(items):
                return items[idx]
            print("無効な番号です。もう一度入力してください。")
        except ValueError:
            print("数値を入力してください。")
        except (EOFError, KeyboardInterrupt):
            die("選択がキャンセルされました")


def parse_datetime(s: str) -> datetime:
    """日時文字列をパースする"""
    for fmt in [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M",
        "%Y-%m-%d",
    ]:
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    die(f"日時のパースに失敗しました: {s} (例: 2024-01-01 09:00)")
