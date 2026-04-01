# cli-tools

AWS 運用や日常業務で使う CLI ツール集。

## ツール一覧

| コマンド | 説明 |
|---|---|
| [`nippou`](nippou/) | GitHub コミット・シェル履歴・Slack メッセージから日報を自動生成 |
| [`ecs_exec`](ecs-exec/) | ECS タスクへの対話的な接続ラッパー |
| [`db_connect`](db-connect/) | SSM 経由の踏み台ログイン・RDS ポートフォワード |
| [`waf_logs`](waf-logs/) | WAF ログを S3 から取得し DuckDB で対話的に分析 |

## その他

| ツール | 説明 |
|---|---|
| [`ai-analytics`](ai-analytics/) | Claude Code の利用データを DuckDB / Langfuse に蓄積・分析 |

## インストール

```bash
./install.sh
```

`~/.local/bin` にシンボリックリンクが作成され、各ツールをコマンド名で直接実行できるようになる。

`~/.local/bin` が PATH に含まれていない場合は `.zshrc` に以下を追加:

```bash
export PATH="$HOME/.local/bin:$PATH"
```

### アンインストール

```bash
./install.sh --remove
```
