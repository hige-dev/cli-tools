# nippou

GitHub コミット・PR・本人のレビュー、ローカル Markdown、本人補足、シェル履歴、Claude Code セッション、Slack の本人投稿を収集し、Claude CLI で日報を生成するツール。

## 必要なもの

- Bash 4.3 以上
- 認証済みの GitHub CLI（`gh`）と Claude CLI（`claude`）
- `git`, `jq`, `perl`, `curl`
- GNU の `date`, `find`, `stat`, `readlink`, `realpath`, `sort` と `awk`, `grep`, `sed`

## セットアップと使い方

```bash
cp .env.example .env
chmod 600 .env
# 設定ファイルを編集
vi .env

# 当日の日報
nippou
# 指定日の日報
nippou 2026-10-08
# 期間指定（既定で最大 5 並列）
nippou 2026-10-01 2026-10-08
nippou -j 3 2026-10-01 2026-10-08

# 本人補足のテンプレートを作成（省略時は当日）
nippou --notes
nippou --notes 2026-10-08

# 月報を生成
nippou --monthly 2026-10
nippou --monthly 2026-10 -o ./output/
```

日報は `REPORT_DIR/YYYY/MM/YYYY-MM-DD.md`、本人補足は `REPORT_DIR/notes/YYYY/MM/YYYY-MM-DD.md`、月報は `REPORT_DIR/YYYYMM.md` に保存する。月報の出力先は `-o` で変更できる。日報・月報の再生成では生成物を更新する。`--notes` は既存ファイルを上書きしないため、本人補足は直接編集して記録を追加できる。

## Slack 用一覧

日報の要約と同じ Claude 呼び出しで「やったこと」「明日やること」を生成する。要約本文から切り離し、日報末尾には `## やったこと（Slack 用）` と `## 明日やること（Slack 用）` として保存する。

標準出力末尾の以下の部分をそのまま Slack に貼り付けられる。

```text
やったこと
■ nippou
  - 日報の根拠を整理
  - (進行中) 記録の検証

明日やること
■ nippou
  - 未確認の結果を検証
```

一覧が生成されなかった場合は空欄のまま保存し、追加の要約呼び出しはしない。記録のない日は「（記録なし）」の日報を作る。

`GOAL_SHEET_URL` と `GOAL_SHEET_PERSON` を指定すると、対象日の月に対応する目標シートの本人行を「明日やること」の材料にする。月と本人の識別値が一致する JSON 応答だけを採用し、目標を当日の実績には数えない。

`NOTION_INITIATIVES_URL` を指定すると、作業に対応する Notion の施策名・URL・説明を取得する。要約には施策単位のグループ化と施策・PR へのリンクを指示する。生成された日報・月報のリンクは、入力に同じ URL があるものだけを残す。入力にないリンクは表示名だけにし、未確認の裸 URL は「（リンク未確認）」に置き換える。

目標シートと Notion は、Claude CLI から claude.ai のコネクタを使って読み取る。未設定なら参照しない。取得失敗・不正な JSON・該当データなしの場合は警告し、取得できた作業記録で日報を生成する。記録のない日はコネクタも呼ばない。

## Slack への下書き

`SLACK_DRAFT_CHANNEL` にチャンネル ID を設定すると、次の条件をすべて満たす場合に claude.ai の Slack コネクタで下書きを 1 件作成する。

- 当日を対象にした単日の実行
- 標準入力が端末
- 日報の生成に成功し、「やったこと」「明日やること」のいずれかに内容がある

期間指定は開始日と終了日が同じでも対象外。過去日の再生成、cron、パイプなどの非対話実行でも下書きは作成しない。

日報末尾の二つの一覧を一つのメッセージにし、空の一覧を省く。二つ目の見出しと各 `■` 行の前に空行を入れる。下書きの作成は再試行せず、失敗時は警告する。下書きの失敗は、日報生成の終了コードを変更しない。Slack への送信・予約送信は行わない。

Claude CLI にログインし、claude.ai で Slack コネクタに接続しておく。投稿検索用の `SLACK_TOKEN` と、下書き用コネクタの認証は別々に設定する。

## 設定項目

`.env` に記述する。`NIPPOU_CONF` で別の設定ファイルを指定できる。設定ファイルを読み込んだ後、未設定の値に既定値を適用する。パーミッションは `600` 以下にする。

| 変数 | 説明 | 既定値 |
|---|---|---|
| `GH_AUTHOR` | GitHub ユーザー名。空文字を明示すると GitHub 収集を無効化 | `gh api user` の認証ユーザー |
| `REPORT_DIR` | 日報・補足・月報の保存先 | スクリプトと同階層の `logs/` |
| `HIST_FILE` | zsh 履歴ファイル | `$HOME/.zsh_history` |
| `CLAUDE_HISTORY` | Claude Code の入力履歴 | `$HOME/.claude/history.jsonl` |
| `SLACK_TOKEN` | メッセージ検索用 User Token | 未設定（収集無効） |
| `CLAUDE_MAX_ATTEMPTS` | Claude 呼び出しの最大試行数（正の整数） | `3` |
| `CLAUDE_RETRY_WAIT` | 最初の再試行までの秒数（非負の整数） | `15` |
| `DOC_ROOTS` | ローカル資料の探索先。空白区切りで複数指定 | `$HOME/work` |
| `CLAUDE_PROJECTS_DIR` | Claude Code のセッショントランスクリプト | `$HOME/.claude/projects` |
| `DOC_MAX_FILES` | 日報ごとのローカル資料上限 | `20` |
| `DOC_MAX_LINES` | 1 資料あたりの本文行数上限（空行を除く） | `60` |
| `PR_BODY_MAX_CHARS` | 月報に渡す PR 本文の文字数上限 | `800` |
| `SLACK_DRAFT_CHANNEL` | 当日・単日・対話実行で下書きを作るチャンネル ID | 未設定（作成無効） |
| `GOAL_SHEET_URL` | 対象月の本人行を読む目標シートの URL | 未設定（参照無効） |
| `GOAL_SHEET_PERSON` | 目標シートの本人行の氏名・識別値。URL 設定時は必須 | 未設定 |
| `GOAL_SHEET_READ_TOOLS` | 目標シート参照で許可する MCP 読み取りツール名。空白区切り | Google Drive の `google_drive_search` / `google_drive_fetch` |
| `NOTION_INITIATIVES_URL` | 施策一覧のページ・データベース URL | 未設定（参照無効） |
| `NOTION_READ_TOOLS` | Notion 参照で許可する MCP 読み取りツール名。空白区切り | Notion の `search` / `fetch` / `notion-search` / `notion-fetch` |

件数・行数・文字数の上限は正の整数で指定する。`DOC_ROOTS` では空白を含むディレクトリ名を指定できない。

Claude は `/tmp` で実行する。失敗時は stderr（空なら stdout）の先頭 200 文字を進捗ログに出し、待機時間を失敗ごとに倍増して再試行する。既定では最大 3 回試行し、15 秒・30 秒待機する。下書き作成は 1 回だけ試行する。日報の全試行失敗時は要約欄に失敗を記録し、収集した根拠を保存して非ゼロで終了する。期間指定でも失敗した日として集計する。月報の全試行失敗時はエラー終了し、既存の月報を保持する。進捗ログは stderr に出力する。

日報・月報の要約では `--tools ""`・`--strict-mcp-config`・`--disallowedTools 'mcp__*'` でツールを禁止する。コネクタ参照と下書き作成では `--tools ToolSearch`・`--permission-mode dontAsk` を指定し、読み取りまたは下書き作成に使うツール名だけを明示して許可する。`--setting-sources ""` で利用者・プロジェクト設定からの許可追加を無効にし、送信・予約送信のツールを明示して禁止する。[Claude CLI の引数仕様](https://code.claude.com/docs/en/cli-reference)も参照。

コネクタの MCP ツール名は接続方法によって異なる場合がある。既定の完全な名前は `.env.example` を参照し、Claude CLI の `/mcp` で接続状態と実際の読み取りツール名を確認する。必要なら `GOAL_SHEET_READ_TOOLS`・`NOTION_READ_TOOLS` に完全な名前を設定する。目標シートは、その接続がシート内容の取得に対応している必要がある。

## ローカルドキュメントの収集

以下の 2 経路の和集合から `.md` を集め、実体パスで重複を除いてパス順に上限件数まで読む。

| 経路 | 対象 | 日付の判定 | Git 追跡ファイル |
|---|---|---|---|
| A: `DOC_ROOTS` | 未追跡・gitignore 対象、および Git 管理外の資料 | mtime が対象日の範囲内 | 除外 |
| B: `CLAUDE_PROJECTS_DIR` | `Write` / `Edit` / `MultiEdit` / `NotebookEdit` の `file_path` | トランスクリプトの timestamp が対象日の範囲内 | 対象 |

追跡ファイルの mtime はチェックアウトでも変化するため、経路 A では作業の証拠として扱わない。経路 B はツール実行の記録を使う。timestamp は UTC から変換し、実行環境のローカル日付の境界と比較する。対象日以降に更新された JSONL のうち `file_path` を含む行だけを解析する。ツール実行の記録は変更の完了・成功を証明するものではない。

`node_modules`・`.git`・`/tmp`・`REPORT_DIR`・`CLAUDE_PROJECTS_DIR` 配下は除外する。先頭の YAML frontmatter を飛ばし、空行を除いた本文の前半を `DOC_MAX_LINES` 行まで読む。本文の `---` 区切り線で読み込みを終了する。日報の根拠セクションには本文を複製せず、読んだ資料のパスだけを残す。

## 根拠を残す日報・月報

日報の要約は 1 施策 2〜3 行を目安にし、要約内の見出しを `####` に揃える。コミットと Slack 投稿に根拠 URL を付ける。PR のタイトル・作者・取得時点の状態・日時と本人のレビューを保存する。日報に PR 本文は保存しない。本人補足は生成時点の内容を日報に残す。補足やローカル資料だけの日も日報を生成する。目標シートと Notion から取得した参考情報も日報に保存する。

月報は対象月のディレクトリ直下にある `YYYY-MM-??.md` の日報だけを読み込む。日報に対応する最新の本人補足を連結し、資料内の PR URL を重複排除して GitHub API からタイトル・本文を取得する。PR 本文は `PR_BODY_MAX_CHARS` 文字までに切り詰める。対象月の日報がない場合は生成できない。

要約の指示では依頼・提案・実装・検証・本番適用・効果確認を区別し、本人の担当範囲、判断、合意形成、根拠リンク、実測／試算と測定条件、未確認事項を残す。不明な背景や成果を推測で補わず、最新補足と後日の訂正を優先する。予定を実績として扱わない。

### 収集範囲と注意点

- PR 候補は `author` / `reviewed-by` と `updated` 日付の検索結果。後日更新された PR は過去日を再生成すると漏れる場合がある。検索 API 自体の取得上限も適用される。
- PR の状態・本文は取得時点のもの。本人のレビューには対象日以外の記録も含まれるため、日時を保存して区別する。API 失敗時は警告を出して他の収集を続ける。
- Slack はトークンの本人投稿だけを検索し、最大 100 件。15 文字未満の投稿を除外し、本文を 200 文字に切り詰める。コミット検索は最大 100 件、シェル履歴は最大 100 行、Claude 入力履歴は各 200 文字まで。
- ローカル資料は最大 `DOC_MAX_FILES` 件・各 `DOC_MAX_LINES` 行。過去日の再生成でも本文は現在のファイル内容になる。削除済みの資料は読めない。
- 本人補足も現在のファイル内容を読む。過去日の再生成と月報では追記した内容が反映される。
- プロンプトの指示はモデル出力を保証しない。生成された要約と成果の表現は根拠と照合して確認する。

### 検証

Python 3.9 以上が必要。`nippou/` ディレクトリで実行する。

```bash
bash -n nippou.sh
bash -n tests.sh
bash tests.sh
```

テストは設定・履歴・ホームを一時領域に隔離し、`gh`・`claude`・収集関数を差し替える。`tests/cli.py` では実際のスクリプトを別プロセスで起動し、要約失敗・書き込み失敗の並列集計と、擬似端末による下書きの作成条件も確認する。外部サービスには接続しない。成功時は `PASS` 行を出す。

## Slack トークンの取得

Slack の検索には User Token（`xoxp-...`）を使う。Bot Token（`xoxb-...`）は使えない。

1. [Slack API: Your Apps](https://api.slack.com/apps) でアプリを作成する。
2. **OAuth & Permissions** の **User Token Scopes** に `search:read` を追加する。
3. ワークスペースにインストールして User OAuth Token を取得する。
4. `.env` に `SLACK_TOKEN="xoxp-..."` を設定する。

トークンで閲覧できるチャンネルの本人投稿を検索する。トークンを含む `.env` は Git にコミットしない。
