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
| `REPORT_DIR` | 出力先ディレクトリ | スクリプトと同階層の `logs/` |
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

## 根拠を残す日報・月報

日報にはコミット全文とURL、PR本文・取得時点の状態、自分が投稿したレビュー、
Slack本文と投稿リンク、Claudeへの依頼履歴を保存します。
要約では本人の担当範囲・判断・合意形成・進捗状態・確認根拠を整理し、
記録のない背景や効果を推測で補いません。実測／試算と測定条件を区別します。
依頼やコマンド実行、PRマージだけから、本番適用や効果確認を断定しません。

### 重要な案件の補足

```bash
nippou --notes 2026-09-16
# 日付省略時は当日。表示されたMarkdownを編集する
nippou 2026-09-16
nippou --monthly 2026-09
```

補足は `REPORT_DIR/notes/YYYY/MM/YYYY-MM-DD.md` に保存します。
起点、担当範囲、判断理由、関係者との調整、状態、確認結果、測定条件、未確認事項を
必要な範囲で記入してください。空欄は実績として扱いません。
既存の補足は上書きせず、日報再生成でも保持します。
月報は対象日の日報と最新の補足を読むため、日報生成後の追記も反映できます。
月報生成には少なくとも1件の日報が必要です。

### 収集範囲と注意点

- PR候補は「自分が作成またはレビューしたPRのうち、最終更新日が対象日のもの」です。
  過去分を再生成する場合、後日に更新されたPRは候補から漏れる場合があります。
  必要なPRや設計相談は補足に記録してください。
- PR本文・状態は取得時点の情報で、対象日当時のスナップショットではありません。
  レビューは日時付きで保存し、対象日以外の活動と区別します。
- Slackは自分の投稿が対象です。相手の返信や承認内容は自動収集しません。
- コミット・Slackは最大100件、シェル履歴は先頭100件です。
  Claude履歴は依頼内容であり、セッション全体の実行結果ではありません。
- PR候補とレビューはページをたどって取得しますが、GitHub検索の上限に従います。
  API取得失敗はPR収集時に警告します。
- 月報は同じ施策の継続・重複をまとめ、後日の訂正を反映します。

### 検証

```bash
bash -n nippou/nippou.sh
bash nippou/tests.sh
```

テストは外部API・Claudeを呼ばず、補足の保護、日報・月報への取り込み、
対象日報の選別を確認します。
