# db-connect

SSM 経由で踏み台インスタンスにログイン、または RDS へのポートフォワードを対話的に確立する。

## 必要なもの

- AWS CLI v2
- Session Manager Plugin
- jq
- fzf（任意 — なければ番号選択にフォールバック）

## 使い方

```bash
db_connect [config-profile]
```

引数なしで実行すると設定ファイルのプロファイル一覧から選択できる。

## 接続モード

- **port-forward** — SSM ポートフォワードで `localhost:XXXXX` → RDS のトンネルを確立
- **login** — SSM で踏み台にログイン

## 設定ファイル

スクリプトと同じディレクトリの `config.json` を参照する。
初回実行時にサンプルを自動生成する。`config.example.json` をコピーして使うこともできる。

```bash
cp config.example.json config.json
vi config.json  # 環境に合わせて編集
```

```json
{
  "profiles": {
    "dev": {
      "aws_profile": "dev",
      "bastion_instance_id": "i-xxxxxxxxxxxxxxxxx",
      "databases": {
        "main": {
          "endpoint": "main.cluster-xxxx.ap-northeast-1.rds.example:5432",
          "local_port": 15432
        },
        "sub": {
          "endpoint": "sub.cluster-xxxx.ap-northeast-1.rds.example:5432",
          "local_port": 15433
        }
      },
      "description": "開発環境"
    }
  }
}
```

### プロファイル

| フィールド | 必須 | 説明 |
|---|---|---|
| `aws_profile` | ○ | AWS CLI プロファイル名 |
| `bastion_instance_id` | - | デフォルトの踏み台インスタンス ID。<br>未設定なら EC2 一覧から選択 |
| `databases` | ○ | 接続先 DB の定義（下記参照） |
| `description` | - | プロファイル選択時の表示名 |

### databases

プロファイル内に複数の DB を定義できる。`local_port` を DB ごとに固定することで、SQL クライアントの接続設定を使い回せる。

| フィールド | 必須 | 説明 |
|---|---|---|
| `endpoint` | ○ | RDS エンドポイント（`host:port` 形式） |
| `local_port` | ○ | ローカルポート番号（DB ごとに固定） |

## 前提条件

- 踏み台インスタンスに SSM Agent が導入・起動していること
- IAM に `ssm:StartSession` 権限があること
- ポートフォワードの場合、踏み台から RDS へのネットワーク疎通があること
