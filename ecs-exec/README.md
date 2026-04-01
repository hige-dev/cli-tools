# ecs-exec

`aws ecs execute-command` の対話型ラッパー。クラスタ・サービス・タスク・コンテナ・シェルを順に選択して接続する。

## 必要なもの

- AWS CLI v2
- jq
- fzf（任意 — なければ番号選択にフォールバック）

## 使い方

```bash
ecs_exec <aws-profile>
```

実行すると以下の順で対話的に選択できる:

1. クラスタ
2. サービス
3. タスク（RUNNING のみ）
4. コンテナ
5. シェル（bash / sh）

選択肢が1つしかない場合は自動で確定する。

## 前提条件

- 対象タスクで ECS Exec が有効化されていること（`enableExecuteCommand: true`）
- IAM に `ecs:ExecuteCommand` 権限があること
- Session Manager Plugin がインストール済みであること
