"""CloudWatch Logs プロバイダー"""

import json
import sys
from datetime import datetime

import boto3
import duckdb
from botocore.exceptions import ClientError

from ..core.utils import die, pick_one
from .base import LogProvider


class CwlProvider(LogProvider):
    @property
    def name(self) -> str:
        return "CloudWatch Logs"

    @property
    def table_name(self) -> str:
        return "cwl_logs"

    @property
    def shell_prompt(self) -> str:
        return "cwl> "

    @property
    def uses_s3(self) -> bool:
        return False

    @property
    def file_extension(self) -> str:
        return ""

    def discover_log_source(self, session, region: str, account_id: str) -> dict:
        """ロググループを対話的に選択する"""
        logs_client = session.client("logs", region_name=region)

        # プレフィックスで絞り込み
        prefixes = [
            "/ecs/",
            "/aws/lambda/",
            "/aws/",
            "",  # すべて表示
        ]
        prefix_labels = [
            "/ecs/ (ECS タスク)",
            "/aws/lambda/ (Lambda 関数)",
            "/aws/ (AWS サービス全般)",
            "すべてのロググループ",
        ]
        selected_label = pick_one("ロググループのフィルタ", prefix_labels)
        prefix = prefixes[prefix_labels.index(selected_label)]

        log_groups = self._list_log_groups(logs_client, prefix)
        if not log_groups:
            die(f"ロググループが見つかりません (プレフィックス: {prefix or '(なし)'})")

        display = [
            f"{lg['logGroupName']}  ({self._format_bytes(lg.get('storedBytes', 0))})"
            for lg in log_groups
        ]
        selected = pick_one("ロググループを選択", display)
        idx = display.index(selected)
        log_group = log_groups[idx]
        print(f"-- ロググループ: {log_group['logGroupName']}")

        return {
            "bucket": "",
            "base_prefix": "",
            "metadata": {
                "log_group_name": log_group["logGroupName"],
            },
        }

    def build_s3_prefix(self, base_prefix: str, account_id: str, metadata: dict) -> str:
        return ""

    def build_time_prefixes(self, prefix: str, start: datetime, end: datetime) -> set[str]:
        return set()

    def create_table_sql(self, s3_urls: list[str]) -> str:
        return ""

    def create_table_from_local_sql(self, file_paths: list[str]) -> str:
        return ""

    def fetch_and_load(
        self,
        db: duckdb.DuckDBPyConnection,
        session: boto3.Session,
        region: str,
        account_id: str,
        start: datetime,
        end: datetime,
    ) -> int:
        """CloudWatch Logs API でログを取得し DuckDB に取り込む"""
        logs_client = session.client("logs", region_name=region)

        source = self.discover_log_source(session, region, account_id)
        log_group_name = source["metadata"]["log_group_name"]

        start_ms = int(start.timestamp() * 1000)
        end_ms = int(end.timestamp() * 1000)

        print("\nログイベントを取得中...")
        events = self._fetch_log_events(logs_client, log_group_name, start_ms, end_ms)
        if not events:
            die("指定期間のログイベントが見つかりません")
        print(f"  {len(events):,} イベント取得しました")

        print("DuckDB に取り込み中...")
        return self._load_events_to_duckdb(db, events, log_group_name)

    def create_views(self, db: duckdb.DuckDBPyConnection) -> None:
        db.execute("""
            CREATE OR REPLACE VIEW timeline AS
            SELECT
                time_bucket(INTERVAL '1 minute', ts) AS bucket,
                log_stream,
                count(*) AS cnt
            FROM cwl_logs
            GROUP BY bucket, log_stream
            ORDER BY bucket
        """)

        db.execute("""
            CREATE OR REPLACE VIEW error_logs AS
            SELECT ts, log_stream, message
            FROM cwl_logs
            WHERE lower(message) LIKE '%error%'
               OR lower(message) LIKE '%exception%'
               OR lower(message) LIKE '%traceback%'
               OR log_level IN ('ERROR', 'CRITICAL', 'FATAL')
            ORDER BY ts DESC
        """)

        db.execute("""
            CREATE OR REPLACE VIEW warn_logs AS
            SELECT ts, log_stream, message
            FROM cwl_logs
            WHERE lower(message) LIKE '%warn%'
               OR log_level = 'WARN'
               OR log_level = 'WARNING'
            ORDER BY ts DESC
        """)

        db.execute("""
            CREATE OR REPLACE VIEW log_streams AS
            SELECT
                log_stream,
                count(*) AS cnt,
                min(ts) AS first_seen,
                max(ts) AS last_seen
            FROM cwl_logs
            GROUP BY log_stream
            ORDER BY cnt DESC
        """)

        db.execute("""
            CREATE OR REPLACE VIEW log_levels AS
            SELECT
                log_level,
                count(*) AS cnt
            FROM cwl_logs
            GROUP BY log_level
            ORDER BY cnt DESC
        """)

        # JSON ログの場合に便利なビュー
        try:
            db.execute("""
                CREATE OR REPLACE VIEW json_logs AS
                SELECT
                    ts,
                    log_stream,
                    log_level,
                    json_data
                FROM cwl_logs
                WHERE json_data IS NOT NULL
                ORDER BY ts DESC
            """)
        except Exception:
            pass

    def get_help_text(self) -> str:
        return """
=== 利用可能なビュー ===
  error_logs    ERROR / EXCEPTION / TRACEBACK を含むログ
  warn_logs     WARN / WARNING を含むログ
  timeline      1分間隔の時系列イベント数
  log_streams   ログストリーム別集計
  log_levels    ログレベル別集計
  json_logs     JSON パース済みログ (JSON ログの場合のみ)

=== 主要カラム ===
  ts            タイムスタンプ
  log_stream    ログストリーム名
  log_group     ロググループ名
  message       ログメッセージ (生テキスト)
  log_level     ログレベル (自動検出)
  json_data     JSON パース結果 (JSON ログの場合)

=== 使用例 ===
  SELECT * FROM error_logs LIMIT 20;
  SELECT * FROM timeline;
  SELECT * FROM log_streams;
  SELECT message FROM cwl_logs WHERE message LIKE '%timeout%' LIMIT 20;
  SELECT json_data->>'statusCode' AS status, count(*) AS cnt
    FROM cwl_logs WHERE json_data IS NOT NULL
    GROUP BY status ORDER BY cnt DESC;

=== コマンド ===
  .claude <質問>  claude に分析を依頼
  .tables         テーブル・ビュー一覧
  .schema         テーブルのスキーマ表示
  .help           このヘルプを表示
  .quit           終了
"""

    def get_summary_queries(self) -> dict:
        return {
            "total": "SELECT count(*) FROM cwl_logs",
            "breakdown": (
                "SELECT log_level, count(*) AS cnt FROM cwl_logs "
                "GROUP BY log_level ORDER BY cnt DESC"
            ),
            "time_range": (
                "SELECT min(ts), max(ts) FROM cwl_logs"
            ),
            "breakdown_label": "ログレベル別",
        }

    # --- 内部メソッド ---

    def _list_log_groups(self, logs_client, prefix: str) -> list[dict]:
        """ロググループ一覧を取得する"""
        log_groups = []
        kwargs = {}
        if prefix:
            kwargs["logGroupNamePrefix"] = prefix

        try:
            paginator = logs_client.get_paginator("describe_log_groups")
            for page in paginator.paginate(**kwargs):
                log_groups.extend(page.get("logGroups", []))
        except ClientError as e:
            die(f"ロググループの取得に失敗しました: {e}")

        log_groups.sort(key=lambda x: x.get("storedBytes", 0), reverse=True)
        return log_groups

    def _fetch_log_events(
        self,
        logs_client,
        log_group_name: str,
        start_ms: int,
        end_ms: int,
    ) -> list[dict]:
        """FilterLogEvents でログイベントを取得する"""
        events = []
        kwargs = {
            "logGroupName": log_group_name,
            "startTime": start_ms,
            "endTime": end_ms,
            "interleaved": True,
        }

        try:
            while True:
                resp = logs_client.filter_log_events(**kwargs)
                events.extend(resp.get("events", []))

                next_token = resp.get("nextToken")
                if not next_token:
                    break
                kwargs["nextToken"] = next_token

                if len(events) % 5000 < 100:
                    print(f"\r  {len(events):,} イベント取得中...", end="", flush=True)
        except ClientError as e:
            if events:
                print(
                    f"\n  警告: 途中でエラーが発生しました ({len(events):,} 件取得済み): {e}",
                    file=sys.stderr,
                )
            else:
                die(f"ログイベントの取得に失敗しました: {e}")

        if events:
            print(f"\r  {len(events):,} イベント取得しました")
        return events

    def _load_events_to_duckdb(
        self,
        db: duckdb.DuckDBPyConnection,
        events: list[dict],
        log_group_name: str,
    ) -> int:
        """ログイベントを DuckDB テーブルに投入する"""
        rows = []
        for ev in events:
            message = ev.get("message", "").rstrip("\n")
            log_stream = ev.get("logStreamName", "")
            ts = ev.get("timestamp", 0)
            ingestion_time = ev.get("ingestionTime", 0)

            log_level = self._detect_log_level(message)
            json_data = self._try_parse_json(message)

            rows.append({
                "ts": ts,
                "ingestion_time": ingestion_time,
                "log_group": log_group_name,
                "log_stream": log_stream,
                "message": message,
                "log_level": log_level,
                "json_data": json.dumps(json_data) if json_data else None,
            })

        db.execute("""
            CREATE TABLE cwl_logs (
                ts TIMESTAMP,
                ingestion_time TIMESTAMP,
                log_group VARCHAR,
                log_stream VARCHAR,
                message VARCHAR,
                log_level VARCHAR,
                json_data JSON
            )
        """)

        db.executemany(
            """
            INSERT INTO cwl_logs VALUES (
                to_timestamp(? / 1000.0),
                to_timestamp(? / 1000.0),
                ?, ?, ?, ?, ?::JSON
            )
            """,
            [
                (
                    r["ts"],
                    r["ingestion_time"],
                    r["log_group"],
                    r["log_stream"],
                    r["message"],
                    r["log_level"],
                    r["json_data"],
                )
                for r in rows
            ],
        )

        count = db.execute("SELECT count(*) FROM cwl_logs").fetchone()[0]
        return count

    @staticmethod
    def _detect_log_level(message: str) -> str:
        """ログメッセージからログレベルを推定する"""
        upper = message[:200].upper()
        for level in ("CRITICAL", "FATAL", "ERROR", "WARN", "WARNING", "INFO", "DEBUG", "TRACE"):
            if level in upper:
                if level == "WARNING":
                    return "WARN"
                if level == "FATAL":
                    return "CRITICAL"
                return level
        return "UNKNOWN"

    @staticmethod
    def _try_parse_json(message: str) -> dict | None:
        """メッセージが JSON ならパースして返す"""
        stripped = message.strip()
        if not stripped.startswith("{"):
            return None
        try:
            return json.loads(stripped)
        except (json.JSONDecodeError, ValueError):
            return None

    @staticmethod
    def _format_bytes(n: int) -> str:
        """バイト数を人間が読みやすい形式に変換する"""
        for unit in ("B", "KB", "MB", "GB", "TB"):
            if n < 1024:
                return f"{n:.1f} {unit}"
            n /= 1024
        return f"{n:.1f} PB"
