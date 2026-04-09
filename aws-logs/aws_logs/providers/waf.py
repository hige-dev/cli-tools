"""WAF ログプロバイダー"""

import sys
from datetime import datetime, timedelta

import duckdb
from botocore.exceptions import ClientError

from ..core.utils import die, pick_one
from .base import LogProvider


class WafProvider(LogProvider):
    @property
    def name(self) -> str:
        return "WAF"

    @property
    def table_name(self) -> str:
        return "waf_logs"

    @property
    def shell_prompt(self) -> str:
        return "waf> "

    @property
    def file_extension(self) -> str:
        return ".log.gz"

    def discover_log_source(self, session, region: str, account_id: str) -> dict:
        """WebACL を選択し、ログ配信先 S3 バケットを特定する"""
        webacls = self._get_webacls(session, region)
        if not webacls:
            die("WebACL が見つかりません")

        display = [f"{w['name']}  ({w['scope']})" for w in webacls]
        selected = pick_one("WebACL を選択", display)
        idx = display.index(selected)
        webacl = webacls[idx]
        print(f"-- WebACL: {webacl['name']} ({webacl['scope']})")

        log_dest = self._get_logging_config(session, webacl)
        if not log_dest:
            die(
                f"WebACL '{webacl['name']}' のログ配信設定が見つかりません。\n"
                "  WAF コンソールでログ記録を S3 に有効化してください。"
            )

        parts = log_dest.split("/", 1)
        bucket = parts[0]
        base_prefix = parts[1] if len(parts) > 1 else ""
        if base_prefix and not base_prefix.endswith("/"):
            base_prefix += "/"
        print(f"-- S3: s3://{bucket}/{base_prefix}")

        return {
            "bucket": bucket,
            "base_prefix": base_prefix,
            "metadata": {
                "webacl_name": webacl["name"],
                "scope": webacl["scope"],
                "region": webacl["region"],
            },
        }

    def build_s3_prefix(self, base_prefix: str, account_id: str, metadata: dict) -> str:
        waf_region = (
            "cloudfront" if metadata["scope"] == "CLOUDFRONT" else metadata["region"]
        )
        return (
            f"{base_prefix}AWSLogs/{account_id}/WAFLogs/"
            f"{waf_region}/{metadata['webacl_name']}/"
        )

    def build_time_prefixes(self, prefix: str, start: datetime, end: datetime) -> set[str]:
        """5分単位でプレフィックスを生成"""
        current = start.replace(minute=(start.minute // 5) * 5, second=0, microsecond=0)
        prefixes = set()
        while current <= end:
            date_prefix = current.strftime("%Y/%m/%d/%H/%M/")
            prefixes.add(f"{prefix}{date_prefix}")
            current += timedelta(minutes=5)
        return prefixes

    def create_table_sql(self, s3_urls: list[str]) -> str:
        url_list = ", ".join(f"'{u}'" for u in s3_urls)
        return (
            f"CREATE TABLE waf_logs AS\n"
            f"SELECT * FROM read_json_auto(\n"
            f"    [{url_list}],\n"
            f"    compression='gzip',\n"
            f"    maximum_object_size=10485760,\n"
            f"    ignore_errors=true\n"
            f")"
        )

    def create_table_from_local_sql(self, file_paths: list[str]) -> str:
        # ローカルファイルの場合は gzip JSON を JSONL に展開して読む
        # cli.py 側で展開処理を行い、JSONL パスを渡す想定
        if len(file_paths) == 1:
            path = file_paths[0]
        else:
            path_list = ", ".join(f"'{p}'" for p in file_paths)
            path = f"[{path_list}]"
            return (
                f"CREATE TABLE waf_logs AS\n"
                f"SELECT * FROM read_json_auto(\n"
                f"    {path},\n"
                f"    compression='gzip',\n"
                f"    maximum_object_size=10485760,\n"
                f"    ignore_errors=true\n"
                f")"
            )
        return (
            f"CREATE TABLE waf_logs AS\n"
            f"SELECT * FROM read_json_auto(\n"
            f"    '{path}',\n"
            f"    maximum_object_size=10485760,\n"
            f"    ignore_errors=true\n"
            f")"
        )

    def create_views(self, db: duckdb.DuckDBPyConnection) -> None:
        db.execute("""
            CREATE OR REPLACE VIEW top_blocked AS
            SELECT
                httpRequest.clientIp AS client_ip,
                count(*) AS cnt,
                min(to_timestamp(timestamp / 1000)) AS first_seen,
                max(to_timestamp(timestamp / 1000)) AS last_seen
            FROM waf_logs
            WHERE action = 'BLOCK'
            GROUP BY client_ip
            ORDER BY cnt DESC
        """)

        db.execute("""
            CREATE OR REPLACE VIEW top_rules AS
            SELECT
                terminatingRuleId AS rule_id,
                terminatingRuleType AS rule_type,
                action,
                count(*) AS cnt
            FROM waf_logs
            WHERE terminatingRuleId != 'Default_Action'
            GROUP BY rule_id, rule_type, action
            ORDER BY cnt DESC
        """)

        db.execute("""
            CREATE OR REPLACE VIEW top_uri AS
            SELECT
                httpRequest.uri AS uri,
                action,
                count(*) AS cnt
            FROM waf_logs
            GROUP BY uri, action
            ORDER BY cnt DESC
        """)

        db.execute("""
            CREATE OR REPLACE VIEW timeline AS
            SELECT
                time_bucket(
                    INTERVAL '5 minutes',
                    to_timestamp(timestamp / 1000)
                ) AS bucket,
                action,
                count(*) AS cnt
            FROM waf_logs
            GROUP BY bucket, action
            ORDER BY bucket
        """)

        db.execute("""
            CREATE OR REPLACE VIEW top_countries AS
            SELECT
                httpRequest.country AS country,
                action,
                count(*) AS cnt
            FROM waf_logs
            GROUP BY country, action
            ORDER BY cnt DESC
        """)

        db.execute("""
            CREATE OR REPLACE VIEW blocked_details AS
            SELECT
                to_timestamp(timestamp / 1000) AS ts,
                httpRequest.clientIp AS client_ip,
                httpRequest.country AS country,
                httpRequest.uri AS uri,
                httpRequest.httpMethod AS method,
                terminatingRuleId AS rule_id,
                action
            FROM waf_logs
            WHERE action = 'BLOCK'
            ORDER BY ts DESC
        """)

    def get_help_text(self) -> str:
        return """
=== 利用可能なビュー ===
  top_blocked      ブロックされたリクエストの IP 別集計
  top_rules        マッチしたルール別集計
  top_uri          URI 別リクエスト数
  timeline         5分間隔の時系列リクエスト数
  top_countries    国別リクエスト数
  blocked_details  ブロックされたリクエストの詳細

=== 使用例 ===
  SELECT * FROM top_blocked LIMIT 20;
  SELECT * FROM timeline;
  SELECT * FROM blocked_details LIMIT 50;
  SELECT httpRequest.clientIp, httpRequest.uri, httpRequest.country
    FROM waf_logs WHERE action = 'BLOCK' LIMIT 20;

=== コマンド ===
  .claude <質問>  claude に分析を依頼
  .tables         テーブル・ビュー一覧
  .schema         テーブルのスキーマ表示
  .help           このヘルプを表示
  .quit           終了
"""

    def get_summary_queries(self) -> dict:
        return {
            "total": "SELECT count(*) FROM waf_logs",
            "breakdown": (
                "SELECT action, count(*) AS cnt FROM waf_logs "
                "GROUP BY action ORDER BY cnt DESC"
            ),
            "time_range": (
                "SELECT min(to_timestamp(timestamp / 1000)), "
                "max(to_timestamp(timestamp / 1000)) FROM waf_logs"
            ),
            "breakdown_label": "アクション別",
        }

    def warn_sensitive_data(self, db: duckdb.DuckDBPyConnection) -> None:
        """Cookie や Authorization ヘッダーがリダクトされていない場合に警告する"""
        try:
            rows = db.execute(
                """
                SELECT DISTINCT lower(h.name) AS header_name
                FROM waf_logs,
                     LATERAL unnest(httpRequest.headers) AS h
                WHERE lower(h.name) IN ('cookie', 'authorization', 'x-api-key')
                  AND h.value IS NOT NULL
                  AND h.value != ''
                  AND h.value != '-'
                LIMIT 1
                """
            ).fetchall()
        except Exception:
            return

        if not rows:
            return

        found = [r[0] for r in rows]
        header_list = ", ".join(found)

        print(
            "\n"
            "╔══════════════════════════════════════════════════════════════╗\n"
            "║  ⚠  警告: 機微情報がリダクトされていません                 ║\n"
            "╠══════════════════════════════════════════════════════════════╣\n"
            "║                                                            ║\n"
            f"║  検出ヘッダー: {header_list:<44}║\n"
            "║                                                            ║\n"
            "║  WAF ログにセッション ID やトークンなどの機微情報が        ║\n"
            "║  平文で含まれています。このデータはローカルの DuckDB       ║\n"
            "║  ファイルおよび /tmp の一時ファイルに保存されます。        ║\n"
            "║                                                            ║\n"
            "║  対策:                                                     ║\n"
            "║   1. AWS WAF コンソールでログ設定の「Redacted fields」に   ║\n"
            "║      Cookie / Authorization ヘッダーを追加してください     ║\n"
            "║   2. 分析完了後、DB ファイルと /tmp/waf-logs-* を          ║\n"
            "║      速やかに削除してください                              ║\n"
            "║   3. --memory オプションでファイルに残さず分析できます     ║\n"
            "║                                                            ║\n"
            "╚══════════════════════════════════════════════════════════════╝",
            file=sys.stderr,
        )

    # --- 内部メソッド ---

    def _get_webacls(self, session, region: str) -> list[dict]:
        """WebACL 一覧を取得する（リージョナル + CloudFront）"""
        webacls = []

        try:
            wafv2 = session.client("wafv2", region_name=region)
            resp = wafv2.list_web_acls(Scope="REGIONAL")
            for acl in resp.get("WebACLs", []):
                webacls.append({
                    "name": acl["Name"],
                    "arn": acl["ARN"],
                    "scope": "REGIONAL",
                    "region": region,
                })
        except ClientError as e:
            print(f"警告: リージョナル WAF の取得に失敗: {e}", file=sys.stderr)

        try:
            wafv2_cf = session.client("wafv2", region_name="us-east-1")
            resp = wafv2_cf.list_web_acls(Scope="CLOUDFRONT")
            for acl in resp.get("WebACLs", []):
                webacls.append({
                    "name": acl["Name"],
                    "arn": acl["ARN"],
                    "scope": "CLOUDFRONT",
                    "region": "us-east-1",
                })
        except ClientError as e:
            print(f"警告: CloudFront WAF の取得に失敗: {e}", file=sys.stderr)

        return webacls

    def _get_logging_config(self, session, webacl: dict) -> str | None:
        """WebACL のログ配信先 S3 バケットを取得する"""
        wafv2 = session.client("wafv2", region_name=webacl["region"])
        try:
            resp = wafv2.get_logging_configuration(ResourceArn=webacl["arn"])
            destinations = resp["LoggingConfiguration"]["LogDestinationConfigs"]
            for dest in destinations:
                if ":s3:::" in dest:
                    return dest.split(":s3:::")[-1]
            return None
        except ClientError:
            return None
