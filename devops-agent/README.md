# devops-agent

AWS DevOps Agent の対話型 CLI ラッパー。

Agent Space の管理、AWS アカウント連携、GitHub リポジトリ連携を
fzf ベースの対話的 UI で実行できる。

## 必要なもの

- AWS CLI v2
- jq
- curl（初回セットアップ時）
- fzf（任意: インストール済みなら自動で使用）

## 使い方

```bash
# 対話的にサブコマンドを選択
devops_agent

# サブコマンドを直接指定
devops_agent setup
devops_agent create-space
devops_agent associate-aws
devops_agent associate-gh
```

## 初回セットアップの流れ

```bash
# 1. サービスモデルのインストール・IAM ロール作成
devops_agent setup

# 2. Agent Space を作成
devops_agent create-space

# 3. AWS アカウントを連携（CloudWatch 等へのアクセスを許可）
devops_agent associate-aws

# 4. GitHub リポジトリを連携
#    ※ 事前に AWS コンソールで GitHub OAuth 認証を完了しておくこと
devops_agent associate-gh

# 5. AWS コンソールで Operator App を有効化 → 調査開始
#    https://us-east-1.console.aws.amazon.com/devopsagent/
```

## サブコマンド一覧

| コマンド | 説明 |
|---------|------|
| `setup` | サービスモデルのインストール・IAM ロール作成 |
| `spaces` | Agent Space の一覧表示 |
| `create-space` | Agent Space の作成 |
| `delete-space` | Agent Space の削除 |
| `associate-aws` | AWS アカウントの関連付け |
| `associate-gh` | GitHub リポジトリの関連付け |
| `associations` | 関連付けの一覧表示 |
| `status` | Agent Space の詳細・関連付け状況 |
| `help` | ヘルプ表示 |

## 環境変数

| 変数 | 説明 |
|------|------|
| `AWS_PROFILE` | 使用する AWS プロファイル |

## 設定ファイル

`~/.config/devops-agent/config.json` に最後に使用した Agent Space ID などを保存する。
