# ai-analytics — Claude Code 利用分析ツール

Claude Code の [hooks](https://docs.anthropic.com/en/docs/claude-code/hooks) 機能を使い、利用データを DuckDB に自動蓄積・分析するツールです。
オプションで [Langfuse](https://langfuse.com/) にもトレースを送信でき、Web UI での可視化やチーム共有が可能です。

## 仕組み

```
Claude Code → hooks (6イベント) → main.py (stdin JSON) → DuckDB
                                                       → Langfuse (オプション)
```

Claude Code が発火する以下のイベントを自動で記録します:

| イベント | 内容 |
|---|---|
| SessionStart | セッション開始 |
| SessionEnd | セッション終了 |
| UserPromptSubmit | ユーザーがプロンプトを送信 |
| PreToolUse | ツール実行前 |
| PostToolUse | ツール実行後 |
| Stop | エージェント停止 |

## 前提条件

- [Claude Code](https://docs.anthropic.com/en/docs/claude-code) がインストール済み
- Python 3.14 以上
- [uv](https://docs.astral.sh/uv/) (Python パッケージマネージャ)
- [Langfuse](https://langfuse.com/) (オプション — トレース可視化を使う場合)

## セットアップ

### 1. リポジトリをクローン

```bash
git clone <repository-url>
cd ai-analytics
```

### 2. 依存パッケージをインストール

```bash
uv sync
```

### 3. hooks を登録

```bash
uv run python setup.py
```

これで `~/.claude/settings.json` に hooks が自動登録されます。
既存の設定（他の hooks など）はそのまま保持されます。

### 4. Langfuse を設定する（オプション）

Langfuse にトレースを送信する場合は、Langfuse サーバーを立てて `.env` を作成します。

```bash
# Langfuse をセルフホストで起動
git clone https://github.com/langfuse/langfuse.git
cd langfuse
docker compose up -d
```

`http://localhost:3000` にアクセスし、アカウント作成 → プロジェクト作成 → API キーを取得します。

プロジェクトルートに `.env` を作成:

```bash
LANGFUSE_SECRET_KEY="sk-lf-..."
LANGFUSE_PUBLIC_KEY="pk-lf-..."
LANGFUSE_BASE_URL="http://localhost:3000"
```

`.env` がなければ Langfuse 送信はスキップされ、DuckDB のみに記録されます。

### 5. 動作確認

Claude Code で何か操作した後、DB ファイルが生成されていることを確認します:

```bash
ls claude_usage.duckdb
```

## 何が分かるようになるか

### 個人利用

| 分析 | 分かること |
|---|---|
| ツール利用ランキング | Read/Edit/Bash のどれを多用しているか<br>→ 自分の使い方の癖が見える |
| 時間帯別パターン | いつ Claude Code を使っているか<br>→ 集中時間帯の把握 |
| セッション長・プロンプト数 | 1セッションでどれだけやり取りしているか<br>→ タスクの粒度が適切か |
| プロジェクト別利用頻度 | どのリポジトリで多く使っているか |

### チーム展開時

| 分析 | 分かること |
|---|---|
| メンバー別利用状況 | 誰がどれくらい活用しているか<br>→ 導入効果の定量化 |
| プロジェクト横断の利用傾向 | どのプロジェクトで効果が高いか<br>→ 社内展開の優先度判断 |
| ツール利用パターンの比較 | 上手く使っている人の使い方を共有できる |

> **注意:** トークン数・コスト情報は hooks 経由で取得できないため、
> 「いくら使ったか」ではなく「どう使っているか」の行動分析が中心です。

## データの分析

### 付属のクエリを使う

`queries.sql` に基本的な分析クエリが用意されています。

```bash
# DuckDB CLI で直接実行
duckdb claude_usage.duckdb < queries.sql
```

### 用意されているクエリ

| クエリ | 内容 |
|---|---|
| イベント種別ごとの件数 | 全体の利用傾向を把握 |
| 日別セッション数 | 日ごとの利用頻度 |
| ツール利用ランキング | よく使うツールの特定 |
| セッションごとのプロンプト数 | セッションの長さ・密度 |
| プロジェクト別利用頻度 | どのプロジェクトで多く使っているか |
| 時間帯別利用パターン | 何時頃に使っているか |

### Python から直接クエリ

```python
import duckdb

con = duckdb.connect("claude_usage.duckdb")
print(con.sql("SELECT hook_event_name, count(*) FROM events GROUP BY 1").fetchdf())
```

## DB スキーマ

`events` テーブルに全イベントが格納されます:

| カラム | 型 | 内容 |
|---|---|---|
| id | INTEGER | 自動採番 |
| timestamp | TIMESTAMPTZ | 記録日時 (UTC) |
| session_id | VARCHAR | セッション識別子 |
| hook_event_name | VARCHAR | イベント名 |
| tool_name | VARCHAR | ツール名 |
| tool_input | JSON | ツールへの入力 |
| tool_output | JSON | ツールの出力 |
| prompt | VARCHAR | ユーザーのプロンプト |
| cwd | VARCHAR | 作業ディレクトリ |
| permission_mode | VARCHAR | 権限モード |
| agent_id | VARCHAR | エージェント ID |
| agent_type | VARCHAR | エージェント種別 |
| raw | JSON | 生データ全体 |

## ファイル構成

```
ai-analytics/
├── main.py        # 収集スクリプト (hooks から呼ばれる)
├── setup.py       # hooks 登録スクリプト
├── queries.sql    # 分析クエリ集
├── .env           # Langfuse 接続情報 (Git管理外)
├── pyproject.toml # プロジェクト設定
└── CLAUDE.md      # Claude Code 向けプロジェクト説明
```

## 注意事項

- hooks 経由で取得できる情報に**トークン数・コスト情報は含まれません**
- DB ファイル (`claude_usage.duckdb`) はローカルに保存されます。Git 管理外にしてください
- `.env` には API キーが含まれるため、Git 管理外にしてください
- Langfuse の session_id は MD5 ハッシュで 32 文字 hex に変換されます
  （Langfuse の trace_id 仕様に合わせるため）
