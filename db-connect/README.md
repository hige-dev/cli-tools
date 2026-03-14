# db-connect

SSM 経由で踏み台インスタンスにログイン、または RDS へのポートフォワードを対話的に確立する。

## 必要なもの

- AWS CLI v2
- Session Manager Plugin
- jq
- fzf（任意 — なければ番号選択にフォールバック）

## 使い方

```bash
./db-connect.sh [config-profile]
```

引数なしで実行すると設定ファイルのプロファイル一覧から選択できる。

## 接続モード

- **port-forward** — SSM ポートフォワードで `localhost:XXXXX` → RDS のトンネルを確立
- **login** — SSM で踏み台にログイン

## 設定ファイル

`~/.config/db-connect/config.json` にアカウント（環境）ごとのデフォルト値を定義する。初回実行時にサンプルを自動生成。

```json
{
  "profiles": {
    "dev": {
      "aws_profile": "dev",
      "bastion_instance_id": "i-xxxxxxxxxxxxxxxxx",
      "default_rds": "mydb.cluster-xxxx.ap-northeast-1.rds.amazonaws.com",
      "default_engine": "mysql",
      "default_local_port": 13306,
      "description": "開発環境"
    }
  }
}
```

| フィールド | 必須 | 説明 |
|---|---|---|
| `aws_profile` | ○ | AWS CLI プロファイル名 |
| `bastion_instance_id` | - | デフォルトの踏み台インスタンス ID。<br>未設定なら EC2 一覧から選択 |
| `default_rds` | - | デフォルトの RDS エンドポイント。<br>未設定なら RDS 一覧から選択 |
| `default_engine` | - | DB エンジン (`mysql` / `postgresql`)。<br>ポート自動判定に使用 |
| `default_local_port` | - | ローカルポート番号。<br>未設定ならリモートポートと同じ値 |
| `description` | - | プロファイル選択時の表示名 |

## 前提条件

- 踏み台インスタンスに SSM Agent が導入・起動していること
- IAM に `ssm:StartSession` 権限があること
- ポートフォワードの場合、踏み台から RDS へのネットワーク疎通があること
