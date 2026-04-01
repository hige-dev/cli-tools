# nippou

GitHub コミット・シェル履歴・Claude Code セッション・Slack メッセージを収集し、Claude CLI で要約して日報 Markdown を生成するツール。

## 必要なもの

- bash
- [GitHub CLI (`gh`)](https://cli.github.com/) — 認証済みであること
- [Claude CLI (`claude`)](https://docs.anthropic.com/en/docs/claude-cli) — 要約生成に使用
- `jq`, `perl`, `curl` — データ処理用

## セットアップ

```bash
# 設定ファイルを作成
cp .env.example .env
chmod 600 .env

# 必要に応じて .env を編集
vi .env
```

## 使い方

```bash
# 前日の日報を生成（カレントディレクトリに出力）
nippou

# 指定日
nippou 2025-12-01

# 期間指定
nippou 2025-12-01 2025-12-05
```

## 月報の使い方

日報データを集約して、職務経歴書に転用しやすい月報を生成する。

```bash
# 指定月の月報を生成（日報から集約）
nippou --monthly 2025-12

# 出力先を指定
nippou --monthly 2025-12 -o ./output/
```

月報は対象月の日報ファイル（`logs/YYYY/MM/*.md`）を読み込み、Claude CLI でプロジェクト・施策単位に整理して出力する。日報が存在しない月は生成できない。

出力先はデフォルトでスクリプトと同階層の `logs/`。`REPORT_DIR` で変更可能。

```
logs/
└── 2025/
    └── 12/
        └── 2025-12-01.md
```

## 設定項目

`.env` に記述する。すべて省略可能。

| 変数 | 説明 | デフォルト |
|------|------|-----------|
| `GH_AUTHOR` | GitHub ユーザー名 | `gh` の認証ユーザーを自動取得 |
| `REPORT_DIR` | 出力先ディレクトリ | カレントディレクトリ |
| `HIST_FILE` | zsh 履歴ファイルのパス | `~/.zsh_history` |
| `CLAUDE_HISTORY` | Claude Code 履歴ファイルのパス | `~/.claude/history.jsonl` |
| `SLACK_TOKEN` | Slack User Token | 未設定（Slack 収集を無効化） |

環境変数 `NIPPOU_CONF` で `.env` のパスを指定することもできる。

## Slack トークンの取得

Slack のメッセージ検索には **User Token (`xoxp-...`)** が必要。
Bot Token (`xoxb-...`) では `search.messages` API を利用できない。

### 手順

1. [Slack API: Your Apps](https://api.slack.com/apps) にアクセス
2. **Create New App** → **From scratch** を選択
3. アプリ名（例: `nippou`）と対象ワークスペースを指定して作成
4. 左メニュー **OAuth & Permissions** を開く
5. **User Token Scopes** に以下を追加:
   - `search:read` — メッセージ検索に必要
6. ページ上部の **Install to Workspace** をクリックし、権限を許可
7. 表示される **User OAuth Token** (`xoxp-...`) をコピー
8. `.env` に設定:
   ```
   SLACK_TOKEN="xoxp-xxxx-xxxx-xxxx-xxxx"
   ```

### 注意事項

- User Token はそのユーザーの権限でアクセスするため、
  自分が閲覧できるチャンネルのメッセージのみ検索対象になる
- トークンは `.env` に保存し、**Git にコミットしないこと**
  （`.gitignore` に `.env` を追加推奨）
