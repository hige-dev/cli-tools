"""ALB ログプロバイダー"""

import sys
from datetime import datetime, timedelta

import duckdb
from botocore.exceptions import ClientError

from ..core.utils import die, pick_one
from .base import LogProvider

# ALB ログのカラム名（スペース区切り）
ALB_COLUMNS = [
    "type", "timestamp", "elb", "client_port", "target_port",
    "request_processing_time", "target_processing_time",
    "response_processing_time", "elb_status_code", "target_status_code",
    "received_bytes", "sent_bytes", "request", "user_agent",
    "ssl_cipher", "ssl_protocol", "target_group_arn", "trace_id",
    "domain_name", "chosen_cert_arn", "matched_rule_priority",
    "request_creation_time", "actions_executed", "redirect_url",
    "error_reason", "target_port_list", "target_status_code_list",
    "classification", "classification_reason", "conn_trace_id",
]


class AlbProvider(LogProvider):
    @property
    def name(self) -> str:
        return "ALB"

    @property
    def table_name(self) -> str:
        return "alb_logs"

    @property
    def shell_prompt(self) -> str:
        return "alb> "

    @property
    def file_extension(self) -> str:
        return ".log.gz"

    def discover_log_source(self, session, region: str, account_id: str) -> dict:
        """ALB を選択し、ログ配信先 S3 を特定する"""
        albs = self._list_albs(session, region)
        if not albs:
            die("ALB が見つかりません")

        display = [f"{a['name']}  ({a['dns']})" for a in albs]
        selected = pick_one("ALB を選択", display)
        idx = display.index(selected)
        alb = albs[idx]
        print(f"-- ALB: {alb['name']}")

        log_config = self._get_logging_config(session, region, alb["arn"])
        if not log_config:
            die(
                f"ALB '{alb['name']}' のアクセスログが有効化されていません。\n"
                "  EC2 コンソール → ロードバランサー → 属性 → "
                "アクセスログを有効化してください。"
            )

        bucket = log_config["bucket"]
        prefix = log_config["prefix"]
        if prefix and not prefix.endswith("/"):
            prefix += "/"
        print(f"-- S3: s3://{bucket}/{prefix}")

        return {
            "bucket": bucket,
            "base_prefix": prefix,
            "metadata": {
                "alb_name": alb["name"],
                "region": region,
            },
        }

    def build_s3_prefix(self, base_prefix: str, account_id: str, metadata: dict) -> str:
        region = metadata["region"]
        return (
            f"{base_prefix}AWSLogs/{account_id}/"
            f"elasticloadbalancing/{region}/"
        )

    def build_time_prefixes(self, prefix: str, start: datetime, end: datetime) -> set[str]:
        """日単位でプレフィックスを生成"""
        current = start.replace(hour=0, minute=0, second=0, microsecond=0)
        prefixes = set()
        while current <= end:
            date_prefix = current.strftime("%Y/%m/%d/")
            prefixes.add(f"{prefix}{date_prefix}")
            current += timedelta(days=1)
        return prefixes

    def create_table_sql(self, s3_urls: list[str]) -> str:
        url_list = ", ".join(f"'{u}'" for u in s3_urls)
        col_names = ", ".join(f"'{c}'" for c in ALB_COLUMNS)
        return (
            f"CREATE TABLE alb_logs AS\n"
            f"SELECT * FROM read_csv(\n"
            f"    [{url_list}],\n"
            f"    delim=' ',\n"
            f"    quote='\"',\n"
            f"    compression='gzip',\n"
            f"    names=[{col_names}],\n"
            f"    ignore_errors=true,\n"
            f"    all_varchar=true\n"
            f")"
        )

    def create_table_from_local_sql(self, file_paths: list[str]) -> str:
        col_names = ", ".join(f"'{c}'" for c in ALB_COLUMNS)
        if len(file_paths) == 1:
            src = f"'{file_paths[0]}'"
        else:
            path_list = ", ".join(f"'{p}'" for p in file_paths)
            src = f"[{path_list}]"
        return (
            f"CREATE TABLE alb_logs AS\n"
            f"SELECT * FROM read_csv(\n"
            f"    {src},\n"
            f"    delim=' ',\n"
            f"    quote='\"',\n"
            f"    compression='gzip',\n"
            f"    names=[{col_names}],\n"
            f"    ignore_errors=true,\n"
            f"    all_varchar=true\n"
            f")"
        )

    def create_views(self, db: duckdb.DuckDBPyConnection) -> None:
        db.execute("""
            CREATE OR REPLACE VIEW top_status AS
            SELECT
                elb_status_code,
                count(*) AS cnt
            FROM alb_logs
            GROUP BY elb_status_code
            ORDER BY cnt DESC
        """)

        db.execute("""
            CREATE OR REPLACE VIEW top_uri AS
            SELECT
                regexp_extract(request, '[A-Z]+ (\\S+) ', 1) AS uri,
                elb_status_code,
                count(*) AS cnt
            FROM alb_logs
            GROUP BY uri, elb_status_code
            ORDER BY cnt DESC
        """)

        db.execute("""
            CREATE OR REPLACE VIEW slow_requests AS
            SELECT
                CAST(timestamp AS TIMESTAMP) AS ts,
                split_part(client_port, ':', 1) AS client_ip,
                regexp_extract(request, '[A-Z]+ (\\S+) ', 1) AS uri,
                CAST(target_processing_time AS DOUBLE) AS target_time,
                CAST(response_processing_time AS DOUBLE) AS response_time,
                elb_status_code,
                target_status_code
            FROM alb_logs
            WHERE target_processing_time != '-'
            ORDER BY target_time DESC
        """)

        db.execute("""
            CREATE OR REPLACE VIEW timeline AS
            SELECT
                time_bucket(
                    INTERVAL '5 minutes',
                    CAST(timestamp AS TIMESTAMP)
                ) AS bucket,
                count(*) AS cnt
            FROM alb_logs
            GROUP BY bucket
            ORDER BY bucket
        """)

        db.execute("""
            CREATE OR REPLACE VIEW top_ips AS
            SELECT
                split_part(client_port, ':', 1) AS client_ip,
                count(*) AS cnt
            FROM alb_logs
            GROUP BY client_ip
            ORDER BY cnt DESC
        """)

        db.execute("""
            CREATE OR REPLACE VIEW error_details AS
            SELECT
                CAST(timestamp AS TIMESTAMP) AS ts,
                split_part(client_port, ':', 1) AS client_ip,
                regexp_extract(request, '[A-Z]+ (\\S+) ', 1) AS uri,
                elb_status_code,
                target_status_code,
                error_reason,
                actions_executed
            FROM alb_logs
            WHERE CAST(elb_status_code AS INTEGER) >= 400
            ORDER BY ts DESC
        """)

        db.execute("""
            CREATE OR REPLACE VIEW target_health AS
            SELECT
                target_port,
                target_status_code,
                count(*) AS cnt,
                avg(CASE WHEN target_processing_time != '-'
                    THEN CAST(target_processing_time AS DOUBLE) END) AS avg_time
            FROM alb_logs
            GROUP BY target_port, target_status_code
            ORDER BY cnt DESC
        """)

    def get_help_text(self) -> str:
        return """
=== 利用可能なビュー ===
  top_status       ステータスコード別集計
  top_uri          URI 別リクエスト数
  slow_requests    レスポンスタイムが遅いリクエスト
  timeline         5分間隔の時系列リクエスト数
  top_ips          クライアント IP 別集計
  error_details    エラーリクエスト (4xx/5xx) の詳細
  target_health    ターゲット別の応答状況

=== 使用例 ===
  SELECT * FROM top_status;
  SELECT * FROM slow_requests LIMIT 20;
  SELECT * FROM top_uri LIMIT 20;
  SELECT * FROM error_details LIMIT 50;
  SELECT * FROM target_health;

=== コマンド ===
  .claude <質問>  claude に分析を依頼
  .tables         テーブル・ビュー一覧
  .schema         テーブルのスキーマ表示
  .help           このヘルプを表示
  .quit           終了
"""

    def get_summary_queries(self) -> dict:
        return {
            "total": "SELECT count(*) FROM alb_logs",
            "breakdown": (
                "SELECT elb_status_code, count(*) AS cnt FROM alb_logs "
                "GROUP BY elb_status_code ORDER BY cnt DESC"
            ),
            "time_range": (
                "SELECT min(CAST(timestamp AS TIMESTAMP)), "
                "max(CAST(timestamp AS TIMESTAMP)) FROM alb_logs"
            ),
            "breakdown_label": "ステータス別",
        }

    # --- 内部メソッド ---

    def _list_albs(self, session, region: str) -> list[dict]:
        """ALB 一覧を取得する"""
        elbv2 = session.client("elbv2", region_name=region)
        albs = []
        try:
            paginator = elbv2.get_paginator("describe_load_balancers")
            for page in paginator.paginate():
                for lb in page.get("LoadBalancers", []):
                    if lb["Type"] == "application":
                        albs.append({
                            "name": lb["LoadBalancerName"],
                            "arn": lb["LoadBalancerArn"],
                            "dns": lb["DNSName"],
                        })
        except ClientError as e:
            print(f"警告: ALB の取得に失敗: {e}", file=sys.stderr)
        return albs

    def _get_logging_config(self, session, region: str, alb_arn: str) -> dict | None:
        """ALB のアクセスログ設定を取得する"""
        elbv2 = session.client("elbv2", region_name=region)
        try:
            resp = elbv2.describe_load_balancer_attributes(
                LoadBalancerArn=alb_arn
            )
            attrs = {a["Key"]: a["Value"] for a in resp["Attributes"]}
            if attrs.get("access_logs.s3.enabled") != "true":
                return None
            return {
                "bucket": attrs["access_logs.s3.bucket"],
                "prefix": attrs.get("access_logs.s3.prefix", ""),
            }
        except ClientError:
            return None
