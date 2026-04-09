# aws-logs

AWS の各種ログ (WAF / CloudFront / ALB) を DuckDB httpfs で S3 から直接取り込み、対話的に SQL 分析を行うツール。
ダウンロード不要で、S3 上のログをそのままクエリできる。

## 対応ログ種別

| サブコマンド | ログ種別 |
|---|---|
| `aws_logs waf` | AWS WAF |
| `aws_logs cf` | CloudFront |
| `aws_logs alb` | ALB |

## 必要なもの

- Python 3.9+
- AWS 認証情報（対象サービス, S3 の読み取り権限）

## セットアップ

```bash
########### 任意 ################
# globalにインストールしたくない場合
# python -m venv venv
# source venv/bin/activate
################################

pip install -r requirements.txt
```

リポジトリルートの `install.sh` を実行すると `~/.local/bin/aws_logs` にシンボリックリンクが作成される。

```bash
cd ../
./install.sh
```

## 使い方

すべてのサブコマンドで共通のオプションが使える。

```bash
# 直近1時間のログを分析（デフォルト）
aws_logs waf
aws_logs cf
aws_logs alb

# 直近N時間
aws_logs waf --hours 6

# 期間指定
aws_logs cf --from "2026-03-13 14:45" --to "2026-03-13 15:00"

# AWS プロファイル・リージョン指定
aws_logs alb -p production -r ap-northeast-1

# DB ファイルパスを指定
aws_logs waf --hours 24 --db ./waf_2026-03-13.duckdb

# 保存済み DB を再利用（S3 取得をスキップ）
aws_logs waf --db ./waf_2026-03-13.duckdb

# S3 からローカルにダウンロードしてから取り込む（従来方式）
aws_logs waf --download

# ダウンロード済みログディレクトリを指定
aws_logs alb --local-dir /tmp/alb-logs-20260313

# インメモリモード（DB ファイルを作成しない）
aws_logs cf --memory
```

## 処理の流れ

1. ログ配信元を対話的に選択
   - WAF: WebACL を選択
   - CloudFront: ディストリビューションを選択
   - ALB: ロードバランサーを選択
2. ログ配信先の S3 バケットを自動検出
3. DuckDB httpfs で S3 から直接取り込み（ダウンロード不要）
4. 概要を表示
5. SQL シェルを起動（`.claude` コマンドで AI 分析も可能）

`--db` で既存 DB ファイルを指定した場合、1〜3 をスキップして即座に分析に入る。
`--local-dir` でダウンロード済みディレクトリを指定した場合、1〜2 をスキップする。
`--download` で従来のダウンロード方式（10並列）を使用する。

## プリセットビュー

各ログ種別に応じたビューが SQL シェル内でそのまま使える。

### WAF

| ビュー | 内容 |
|---|---|
| `top_blocked` | ブロックされた IP 別の集計 |
| `top_rules` | マッチしたルール別の集計 |
| `top_uri` | URI 別のリクエスト数 |
| `timeline` | 5分間隔の時系列リクエスト数 |
| `top_countries` | 国別のリクエスト数 |
| `blocked_details` | ブロックされたリクエストの詳細 |

### CloudFront

| ビュー | 内容 |
|---|---|
| `top_status` | ステータスコード別の集計 |
| `top_uri` | URI 別のリクエスト数 |
| `top_edge` | エッジロケーション別の集計 |
| `timeline` | 5分間隔の時系列リクエスト数 |
| `top_ips` | クライアント IP 別の集計 |
| `error_details` | エラーリクエスト (4xx/5xx) の詳細 |
| `cache_hit_ratio` | キャッシュヒット率 |

### ALB

| ビュー | 内容 |
|---|---|
| `top_status` | ステータスコード別の集計 |
| `top_uri` | URI 別のリクエスト数 |
| `slow_requests` | レスポンスタイムが遅いリクエスト |
| `timeline` | 5分間隔の時系列リクエスト数 |
| `top_ips` | クライアント IP 別の集計 |
| `error_details` | エラーリクエスト (4xx/5xx) の詳細 |
| `target_health` | ターゲット別の応答状況 |

### クエリ例

```sql
-- WAF: ブロック数の多い IP トップ20
SELECT * FROM top_blocked LIMIT 20;

-- CloudFront: キャッシュヒット率
SELECT * FROM cache_hit_ratio;

-- ALB: レスポンスが遅いリクエスト
SELECT * FROM slow_requests LIMIT 20;

-- 共通: 時系列のリクエスト推移
SELECT * FROM timeline;
```

## SQL シェルのコマンド

| コマンド | 説明 |
|---|---|
| `.claude <質問>` | claude に自然言語で分析を依頼（セッション維持） |
| `.help` | ヘルプ表示 |
| `.tables` | テーブル・ビュー一覧 |
| `.schema` | テーブルのカラム一覧 |
| `.quit` | 終了 |

`.claude` は初回呼び出し時に DB の概要・スキーマ・ビュー情報を自動で渡し、
2回目以降はセッションを維持して文脈を引き継ぐ。
claude コマンドが未インストールの場合は SQL シェルのみで動作する。

## 機微情報に関する注意

WAF ログの `httpRequest.headers` には、デフォルトで **Cookie** や **Authorization** ヘッダーの値がそのまま記録される。
これにはセッション ID、認証トークンなどの機微情報が含まれる可能性がある。

本ツールはログ取り込み時にこれらのヘッダーを検出すると警告を表示するが、
**AWS 側でリダクト設定を行うことを強く推奨する。**

### 推奨設定

AWS WAF コンソール → 対象 WebACL → Logging → 「Redacted fields」に以下を追加:

- **Cookie** ヘッダー
- **Authorization** ヘッダー
- その他、機微情報を含むカスタムヘッダー

### ローカルでの対策

| 方法 | 説明 |
|---|---|
| `--memory` オプション | DB ファイルをディスクに残さずインメモリで分析 |
| 分析後の手動削除 | `rm /tmp/*-logs-*` で DB と一時ファイルを削除 |

## 必要な IAM 権限

### WAF

```json
{
  "Effect": "Allow",
  "Action": [
    "wafv2:ListWebACLs",
    "wafv2:GetLoggingConfiguration"
  ],
  "Resource": "*"
}
```

### CloudFront

```json
{
  "Effect": "Allow",
  "Action": [
    "cloudfront:ListDistributions",
    "cloudfront:GetDistributionConfig"
  ],
  "Resource": "*"
}
```

### ALB

```json
{
  "Effect": "Allow",
  "Action": [
    "elasticloadbalancing:DescribeLoadBalancers",
    "elasticloadbalancing:DescribeLoadBalancerAttributes"
  ],
  "Resource": "*"
}
```

### 共通

```json
{
  "Effect": "Allow",
  "Action": ["sts:GetCallerIdentity"],
  "Resource": "*"
}
```

```json
{
  "Effect": "Allow",
  "Action": ["s3:ListBucket", "s3:GetObject"],
  "Resource": [
    "arn:aws:s3:::YOUR-LOG-BUCKET",
    "arn:aws:s3:::YOUR-LOG-BUCKET/*"
  ]
}
```

## オプション一覧

| オプション | 短縮 | 説明 | デフォルト |
|---|---|---|---|
| `--profile` | `-p` | AWS プロファイル | デフォルトプロファイル |
| `--region` | `-r` | AWS リージョン | `ap-northeast-1` |
| `--from` | | 開始日時 | 直近1時間前 |
| `--to` | | 終了日時 | 現在時刻 |
| `--hours` | | 直近N時間を取得 | `1` |
| `--db` | | DuckDB ファイルパス（既存なら再利用） | `/tmp/{name}-{timestamp}.duckdb` |
| `--memory` | | インメモリモード | - |
| `--local-dir` | | ダウンロード済みログディレクトリ | - |
| `--download` | | S3 からローカルにダウンロードして取り込む（従来方式） | - |
