# ecs-exec
![demo](./ecs_exec.gif)

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

選択肢が1つしかない場合は自動で確定する。シェルは常に `bash` で接続し、bash が無いコンテナでは自動的に `sh` で再接続する。

## ECS Exec が無効な場合の代替

サービスの `enableExecuteCommand=false` を検知した場合、以下を提示する:

1. **一時デバッグタスクを起動** — 同じネットワーク設定で独立したデバッグコンテナ (デフォルトは `debian:stable-slim` + ネットワークツール) を `run-task` 起動し、自動で接続。セッション終了時に自動停止
2. **CloudShell VPC でネットワーク調査** — 対象タスクの VPC/Subnet/SG を表示し、CloudShell のコンソールを開く
3. 中止

### 1. 一時デバッグタスク

- 既存サービスのアプリコンテナには触らず、独立した `ecs-exec-debug` タスク定義を動的登録する
- ネットワーク設定（VPC/Subnet/SG）・実行ロール・タスクロールは既存サービスから流用
- CPU 256 / Memory 512 の Fargate 最小スペック
- セッション終了 (`exit`) で `stop-task` 自動実行

#### デフォルトイメージ

- `public.ecr.aws/docker/library/debian:stable-slim`（約 35MB）
- 起動時に `apt install` で以下を導入: `dnsutils` / `curl` / `netcat-openbsd` / `tcpdump` / `traceroute` / `iputils-ping` / `iproute2`
- `healthCheck` で install 完了を待ってから接続（約 30〜60 秒）
- apt 到達不可な閉域環境では install 失敗 → カスタムイメージで差し替えを検討

#### カスタムイメージへの切替

環境変数 `ECS_DEBUG_TASK_IMAGE` を指定すると、そのイメージをそのまま使う（install 処理はスキップされる）:

```bash
# netshoot (ネットワーク調査ツール全部入り)
export ECS_DEBUG_TASK_IMAGE=nicolaka/netshoot:v0.14
ecs_exec <aws-profile>

# 自社 ECR イメージ
export ECS_DEBUG_TASK_IMAGE=123456789012.dkr.ecr.ap-northeast-1.amazonaws.com/debug:latest
```

カスタムイメージには事前に調査ツールが含まれている必要があります。

#### タスクが残らない仕組み（二重保険）

1. **`trap` による停止** — `exit` / Ctrl+C / kill / SSH 切断 (HUP) / スクリプト内エラーで `stop-task` 発火
2. **コンテナ TTL** — `trap` が発火しない異常終了 (PC 電源断 / `kill -9` / ネットワーク切断でのゾンビ化) に備えて、コンテナを `sleep 14400`（4時間）で起動。trap 失敗時も最大 4 時間で自動終了

TTL は環境変数で変更可能:

```bash
export ECS_DEBUG_TASK_TTL_SEC=3600  # 1時間
```

#### 複数人での同時利用

同一タスクに複数セッション接続可能（Session Manager 仕様）。ペアで調査する場合、タスク ID (`xxxx` 部分) を共有してもらい、もう一人が以下を実行すれば同時接続できます:

```bash
aws ecs execute-command --cluster <cluster> --task <task-id> \
  --container debug --interactive --command bash
```

流用するタスクロールが SSM メッセージング権限を持たない場合は接続に失敗します。その場合は環境変数で別ロールを指定:

```bash
export ECS_DEBUG_TASK_ROLE_ARN=arn:aws:iam::123456789012:role/ecsTaskDebugRole
ecs_exec <aws-profile>
```

必要な追加 IAM 権限: `ecs:RegisterTaskDefinition`, `ecs:RunTask`, `ecs:StopTask`, `iam:PassRole`

### 2. CloudShell VPC

選択するとタスクの VPC / サブネット / セキュリティグループが表示され、URL が自動でブラウザ / クリップボードに送られる:

- リージョン
- VPC ID
- サブネット ID
- セキュリティグループ

左上 `[+] > Create VPC environment` で上記を入力して起動。

> CloudShell VPC はコンソール専用機能のため、環境作成の最終ステップだけは手動操作になる（[AWS CloudShell FAQs](https://aws.amazon.com/cloudshell/faqs/)）。VPC 内 CloudShell からインターネット / AWS API へ出るには NAT GW もしくは VPC エンドポイントが必要。

## 前提条件

- 対象タスクで ECS Exec が有効化されていること（`enableExecuteCommand: true`） — 無効時は上記フォールバックを利用
- Session Manager Plugin がインストール済みであること
- 後述の IAM ポリシーが付与されていること（実行者・タスクロール両方）

## IAM ポリシー

<details>
<summary><b>1. 実行者 (ecs_exec を実行するユーザー / ロール) 用</b></summary>

全機能を利用する場合のサンプル。不要な機能は該当 Statement を削除してください。最小構成（通常の ecs_exec のみ利用）の場合は `EcsExecBase` のみで動作します。

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "EcsExecBase",
      "Effect": "Allow",
      "Action": [
        "sts:GetCallerIdentity",
        "ecs:ListClusters",
        "ecs:ListServices",
        "ecs:DescribeServices",
        "ecs:ListTasks",
        "ecs:DescribeTasks",
        "ecs:DescribeTaskDefinition",
        "ecs:ExecuteCommand"
      ],
      "Resource": "*"
    },
    {
      "Sid": "EcsExecDebugTask",
      "Effect": "Allow",
      "Action": [
        "ecs:RegisterTaskDefinition",
        "ecs:RunTask",
        "ecs:StopTask"
      ],
      "Resource": "*"
    },
    {
      "Sid": "PassRoleForDebugTask",
      "Effect": "Allow",
      "Action": "iam:PassRole",
      "Resource": "*",
      "Condition": {
        "StringEquals": {
          "iam:PassedToService": "ecs-tasks.amazonaws.com"
        }
      }
    },
    {
      "Sid": "CloudShellVpcDiscovery",
      "Effect": "Allow",
      "Action": [
        "ec2:DescribeNetworkInterfaces"
      ],
      "Resource": "*"
    },
    {
      "Sid": "CloudShell",
      "Effect": "Allow",
      "Action": [
        "cloudshell:*"
      ],
      "Resource": "*"
    }
  ]
}
```

</details>

<details>
<summary><b>2. タスクロール側 (接続先コンテナ) 用</b></summary>

ECS Exec で接続するコンテナのタスクロールには SSM メッセージング権限が必要です。一時デバッグタスクも同じタスクロールを流用するため、この権限が無いと接続できません。

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "ssmmessages:CreateControlChannel",
        "ssmmessages:CreateDataChannel",
        "ssmmessages:OpenControlChannel",
        "ssmmessages:OpenDataChannel"
      ],
      "Resource": "*"
    }
  ]
}
```

※ タスクロールに付与権限がない場合は、環境変数 `ECS_DEBUG_TASK_ROLE_ARN` で上記権限を持つ別ロールを指定可能。

</details>
