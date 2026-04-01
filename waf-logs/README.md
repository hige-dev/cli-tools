# waf-logs

AWS WAF ログを S3 からダウンロードし、DuckDB に取り込んで対話的に SQL 分析を行うツール。

## 必要なもの

- Python 3.9+
- AWS 認証情報（WAF, S3 の読み取り権限）

## セットアップ

```bash
########### 任意 ################
# globalにインストールしたくない場合
# python -m venv venv
# source venv/bin/activate
################################

pip install -r requirements.txt
```

リポジトリルートの `install.sh` を実行すると `~/.local/bin/waf_logs` にシンボリックリンクが作成される。

```bash
cd ../
./install.sh
```

## 使い方

```bash
# 直近1時間のログを分析（デフォルト）
# DB は /tmp/waf-logs-{timestamp}.duckdb に自動保存される
waf_logs

# 直近N時間
waf_logs --hours 6

# 期間指定（分単位で正確に取得）
waf_logs --from "2026-03-13 14:45" --to "2026-03-13 15:00"

# AWS プロファイル・リージョン指定
waf_logs -p production -r ap-northeast-1

# DB ファイルパスを指定
waf_logs --hours 24 --db ./waf_2026-03-13.duckdb

# 保存済み DB を再利用（S3 取得をスキップ）
waf_logs --db ./waf_2026-03-13.duckdb

# ダウンロード済みログディレクトリを指定
waf_logs --local-dir /tmp/waf-logs-20260313-1445

# インメモリモード（DB ファイルを作成しない）
waf_logs --memory
```

## 処理の流れ

1. WebACL を対話的に選択（リージョナル / CloudFront）
2. ログ配信先の S3 バケットを自動検出
3. 指定期間のログファイルを並列ダウンロード（10並列）
4. DuckDB に取り込み、概要を表示
5. SQL シェルを起動（`.claude` コマンドで AI 分析も可能）

`--db` で既存 DB ファイルを指定した場合、1〜3 をスキップして即座に分析に入る。
`--local-dir` でダウンロード済みディレクトリを指定した場合、1〜2 をスキップする。

## プリセットビュー

SQL シェル内でそのまま使えるビューが用意されている。

| ビュー | 内容 |
|---|---|
| `top_blocked` | ブロックされた IP 別の集計 |
| `top_rules` | マッチしたルール別の集計 |
| `top_uri` | URI 別のリクエスト数 |
| `timeline` | 5分間隔の時系列リクエスト数 |
| `top_countries` | 国別のリクエスト数 |
| `blocked_details` | ブロックされたリクエストの詳細 |

### クエリ例

```sql
-- ブロック数の多い IP トップ20
SELECT * FROM top_blocked LIMIT 20;

-- 時系列のリクエスト推移
SELECT * FROM timeline;

-- 特定 IP の詳細
SELECT * FROM blocked_details
WHERE client_ip = '203.0.113.1';

-- 特定 URI パターンへの攻撃
SELECT client_ip, country, uri, method, rule_id, ts
FROM blocked_details
WHERE uri LIKE '%/wp-admin%';

-- 生データを直接クエリ
SELECT httpRequest.clientIp,
       httpRequest.uri,
       httpRequest.country,
       terminatingRuleId
FROM waf_logs
WHERE action = 'BLOCK'
LIMIT 50;
```

## SQL シェルのコマンド

| コマンド | 説明 |
|---|---|
| `.claude <質問>` | claude に自然言語で分析を依頼（セッション維持） |
| `.help` | ヘルプ表示 |
| `.tables` | テーブル・ビュー一覧 |
| `.schema` | waf_logs テーブルのカラム一覧 |
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
| 分析後の手動削除 | `rm /tmp/waf-logs-*` で DB と一時ファイルを削除 |

## 必要な IAM 権限

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "wafv2:ListWebACLs",
        "wafv2:GetLoggingConfiguration",
        "sts:GetCallerIdentity"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket",
        "s3:GetObject"
      ],
      "Resource": [
        "arn:aws:s3:::aws-waf-logs-*",
        "arn:aws:s3:::aws-waf-logs-*/*"
      ]
    }
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
| `--db` | | DuckDB ファイルパス（既存なら再利用） | `/tmp/waf-logs-{timestamp}.duckdb` |
| `--memory` | | インメモリモード | - |
| `--local-dir` | | ダウンロード済み .log.gz ディレクトリ | - |
