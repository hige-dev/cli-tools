"""CloudFront ログプロバイダー"""

import sys
from datetime import datetime, timedelta

import duckdb
from botocore.exceptions import ClientError

from ..core.utils import die, pick_one
from .base import LogProvider

# CloudFront 標準ログの TSV カラム名
CF_COLUMNS = [
    "date", "time", "x_edge_location", "sc_bytes", "c_ip",
    "cs_method", "cs_host", "cs_uri_stem", "sc_status", "cs_referer",
    "cs_user_agent", "cs_uri_query", "cs_cookie", "x_edge_result_type",
    "x_edge_request_id", "x_host_header", "cs_protocol", "cs_bytes",
    "time_taken", "x_forwarded_for", "ssl_protocol", "ssl_cipher",
    "x_edge_response_result_type", "cs_protocol_version", "fle_status",
    "fle_encrypted_fields", "c_port", "time_to_first_byte",
    "x_edge_detailed_result_type", "sc_content_type", "sc_content_len",
    "sc_range_start", "sc_range_end",
]


class CloudFrontProvider(LogProvider):
    @property
    def name(self) -> str:
        return "CloudFront"

    @property
    def table_name(self) -> str:
        return "cf_logs"

    @property
    def shell_prompt(self) -> str:
        return "cf> "

    @property
    def file_extension(self) -> str:
        return ".gz"

    def discover_log_source(self, session, region: str, account_id: str) -> dict:
        """CloudFront ディストリビューションを選択し、ログ配信先 S3 を特定する"""
        distributions = self._list_distributions(session)
        if not distributions:
            die("CloudFront ディストリビューションが見つかりません")

        display = [
            f"{d['id']}  ({', '.join(d['aliases']) or d['domain']})"
            for d in distributions
        ]
        selected = pick_one("CloudFront ディストリビューションを選択", display)
        idx = display.index(selected)
        dist = distributions[idx]
        print(f"-- ディストリビューション: {dist['id']} ({dist['domain']})")

        logging_config = self._get_logging_config(session, dist["id"])
        if not logging_config:
            die(
                f"ディストリビューション '{dist['id']}' のログ配信設定が"
                "見つかりません。\n"
                "  CloudFront コンソールで Standard Logging を有効化してください。"
            )

        bucket = logging_config["bucket"]
        prefix = logging_config["prefix"]
        if prefix and not prefix.endswith("/"):
            prefix += "/"
        print(f"-- S3: s3://{bucket}/{prefix}")

        return {
            "bucket": bucket,
            "base_prefix": prefix,
            "metadata": {
                "distribution_id": dist["id"],
                "domain": dist["domain"],
            },
        }

    def build_s3_prefix(self, base_prefix: str, account_id: str, metadata: dict) -> str:
        # CloudFront ログは {prefix}{distribution_id}.{date} の形式
        # プレフィックスまでを返し、時間フィルタは build_time_prefixes で行う
        return base_prefix

    def build_time_prefixes(self, prefix: str, start: datetime, end: datetime) -> set[str]:
        """日単位でプレフィックスを生成

        CloudFront ログのファイル名は {distribution_id}.{YYYY-MM-DD-HH}.{unique}.gz
        プレフィックスはディレクトリ構造を持たないため、ベースプレフィックスのみ返す
        """
        # CloudFront ログはフラットな構造なので、プレフィックスでの絞り込みは限定的
        return {prefix}

    def create_table_sql(self, s3_urls: list[str]) -> str:
        url_list = ", ".join(f"'{u}'" for u in s3_urls)
        col_names = ", ".join(f"'{c}'" for c in CF_COLUMNS)
        return (
            f"CREATE TABLE cf_logs AS\n"
            f"SELECT * FROM read_csv(\n"
            f"    [{url_list}],\n"
            f"    delim='\\t',\n"
            f"    skip=2,\n"
            f"    compression='gzip',\n"
            f"    names=[{col_names}],\n"
            f"    ignore_errors=true,\n"
            f"    all_varchar=true\n"
            f")"
        )

    def create_table_from_local_sql(self, file_paths: list[str]) -> str:
        col_names = ", ".join(f"'{c}'" for c in CF_COLUMNS)
        if len(file_paths) == 1:
            src = f"'{file_paths[0]}'"
        else:
            path_list = ", ".join(f"'{p}'" for p in file_paths)
            src = f"[{path_list}]"
        return (
            f"CREATE TABLE cf_logs AS\n"
            f"SELECT * FROM read_csv(\n"
            f"    {src},\n"
            f"    delim='\\t',\n"
            f"    skip=2,\n"
            f"    compression='gzip',\n"
            f"    names=[{col_names}],\n"
            f"    ignore_errors=true,\n"
            f"    all_varchar=true\n"
            f")"
        )

    def create_views(self, db: duckdb.DuckDBPyConnection) -> None:
        # タイムスタンプ結合用の共通式
        ts_expr = "strptime(date || ' ' || time, '%Y-%m-%d %H:%M:%S')"

        db.execute(f"""
            CREATE OR REPLACE VIEW top_status AS
            SELECT
                sc_status,
                count(*) AS cnt
            FROM cf_logs
            GROUP BY sc_status
            ORDER BY cnt DESC
        """)

        db.execute(f"""
            CREATE OR REPLACE VIEW top_uri AS
            SELECT
                cs_uri_stem AS uri,
                sc_status,
                count(*) AS cnt
            FROM cf_logs
            GROUP BY uri, sc_status
            ORDER BY cnt DESC
        """)

        db.execute(f"""
            CREATE OR REPLACE VIEW top_edge AS
            SELECT
                x_edge_location,
                count(*) AS cnt
            FROM cf_logs
            GROUP BY x_edge_location
            ORDER BY cnt DESC
        """)

        db.execute(f"""
            CREATE OR REPLACE VIEW timeline AS
            SELECT
                time_bucket(
                    INTERVAL '5 minutes',
                    {ts_expr}
                ) AS bucket,
                count(*) AS cnt
            FROM cf_logs
            GROUP BY bucket
            ORDER BY bucket
        """)

        db.execute(f"""
            CREATE OR REPLACE VIEW top_ips AS
            SELECT
                c_ip,
                count(*) AS cnt
            FROM cf_logs
            GROUP BY c_ip
            ORDER BY cnt DESC
        """)

        db.execute(f"""
            CREATE OR REPLACE VIEW error_details AS
            SELECT
                {ts_expr} AS ts,
                c_ip,
                cs_method,
                cs_uri_stem AS uri,
                sc_status,
                x_edge_result_type,
                x_edge_detailed_result_type
            FROM cf_logs
            WHERE CAST(sc_status AS INTEGER) >= 400
            ORDER BY ts DESC
        """)

        db.execute(f"""
            CREATE OR REPLACE VIEW cache_hit_ratio AS
            SELECT
                x_edge_result_type AS result_type,
                count(*) AS cnt,
                round(count(*) * 100.0 / sum(count(*)) OVER (), 2) AS pct
            FROM cf_logs
            GROUP BY result_type
            ORDER BY cnt DESC
        """)

    def get_help_text(self) -> str:
        return """
=== 利用可能なビュー ===
  top_status       ステータスコード別集計
  top_uri          URI 別リクエスト数
  top_edge         エッジロケーション別集計
  timeline         5分間隔の時系列リクエスト数
  top_ips          クライアント IP 別集計
  error_details    エラーリクエスト (4xx/5xx) の詳細
  cache_hit_ratio  キャッシュヒット率

=== 使用例 ===
  SELECT * FROM top_status;
  SELECT * FROM top_uri LIMIT 20;
  SELECT * FROM cache_hit_ratio;
  SELECT * FROM error_details LIMIT 50;

=== コマンド ===
  .claude <質問>  claude に分析を依頼
  .tables         テーブル・ビュー一覧
  .schema         テーブルのスキーマ表示
  .help           このヘルプを表示
  .quit           終了
"""

    def get_summary_queries(self) -> dict:
        ts_expr = "strptime(date || ' ' || time, '%Y-%m-%d %H:%M:%S')"
        return {
            "total": "SELECT count(*) FROM cf_logs",
            "breakdown": (
                "SELECT sc_status, count(*) AS cnt FROM cf_logs "
                "GROUP BY sc_status ORDER BY cnt DESC"
            ),
            "time_range": (
                f"SELECT min({ts_expr}), max({ts_expr}) FROM cf_logs"
            ),
            "breakdown_label": "ステータス別",
        }

    # --- 内部メソッド ---

    def _list_distributions(self, session) -> list[dict]:
        """CloudFront ディストリビューション一覧を取得する"""
        cf = session.client("cloudfront")
        distributions = []
        try:
            paginator = cf.get_paginator("list_distributions")
            for page in paginator.paginate():
                dist_list = page.get("DistributionList", {})
                for item in dist_list.get("Items", []):
                    aliases = []
                    alias_items = item.get("Aliases", {}).get("Items", [])
                    if alias_items:
                        aliases = alias_items
                    distributions.append({
                        "id": item["Id"],
                        "domain": item["DomainName"],
                        "aliases": aliases,
                    })
        except ClientError as e:
            print(f"警告: CloudFront ディストリビューションの取得に失敗: {e}",
                  file=sys.stderr)
        return distributions

    def _get_logging_config(self, session, distribution_id: str) -> dict | None:
        """ディストリビューションのログ配信設定を取得する"""
        cf = session.client("cloudfront")
        try:
            resp = cf.get_distribution_config(Id=distribution_id)
            config = resp["DistributionConfig"]
            logging = config.get("Logging", {})
            if not logging.get("Enabled"):
                return None
            bucket = logging["Bucket"]
            # bucket は "mybucket.s3.amazonaws.com" 形式
            if bucket.endswith(".s3.amazonaws.com"):
                bucket = bucket[: -len(".s3.amazonaws.com")]
            return {
                "bucket": bucket,
                "prefix": logging.get("Prefix", ""),
            }
        except ClientError:
            return None
