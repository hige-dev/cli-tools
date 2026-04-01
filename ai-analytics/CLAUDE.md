# ai_analytics — Claude Code 利用分析ツール

## プロジェクト概要
Claude Code の hooks 機能を使って利用データを DuckDB に蓄積・分析するツール。
個人利用から始めて、将来的にチーム・社内展開を想定。

## 現在の状態
- **収集スクリプト** (`main.py`): 完成・動作確認済み
- **hooks 設定** (`~/.claude/settings.json`): 6イベント登録済み
- **分析クエリ** (`queries.sql`): 基本6種類作成済み
- **DB**: `claude_usage.duckdb`（hooks 経由で自動生成される）

## アーキテクチャ
```
Claude Code → hooks (6イベント) → main.py (stdin JSON) → DuckDB
```

### 収集対象イベント
SessionStart, SessionEnd, UserPromptSubmit, PreToolUse, PostToolUse, Stop

### DB スキーマ (`events` テーブル)
id, timestamp, session_id, hook_event_name, tool_name, tool_input(JSON),
tool_output(JSON), prompt, cwd, permission_mode, agent_id, agent_type, raw(JSON)

## 技術スタック
- Python 3.14 / uv でパッケージ管理
- DuckDB (列指向DB、JSON クエリ・Parquet エクスポートに強い)
- hooks の制約: **トークン数・コスト情報は取得不可**

## 将来の展開方針
1. データが溜まったら Langfuse (Docker セルフホスト) に移行検討
2. Jupyter notebook での可視化ダッシュボード追加
3. チーム展開時は hooks 設定をプロジェクト `.claude/settings.json` で配布
