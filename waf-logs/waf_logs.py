#!/usr/bin/env python3
"""
waf-logs - WAF ログ分析ツール

指定期間の AWS WAF ログを S3 からダウンロードし、
DuckDB に取り込んで対話的に SQL 分析を行う。

Usage:
    waf-logs [--profile PROFILE] [--region REGION]
             [--from DATETIME] [--to DATETIME]
             [--hours N]

必要なもの:
    - Python 3.9+
    - boto3, duckdb (pip install -r requirements.txt)
    - AWS 認証情報 (WAF, S3 の読み取り権限)
"""

import argparse
import gzip
import readline
import shutil
import subprocess
import sys
import tempfile
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError
except ImportError:
    print(
        "エラー: boto3 がインストールされていません。pip install boto3 を実行してください。",
        file=sys.stderr,
    )
    sys.exit(1)

try:
    import duckdb
except ImportError:
    print(
        "エラー: duckdb がインストールされていません。pip install duckdb を実行してください。",
        file=sys.stderr,
    )
    sys.exit(1)


# ---------------------------------------------------------------------------
# ユーティリティ
# ---------------------------------------------------------------------------


def die(msg: str) -> None:
    print(f"エラー: {msg}", file=sys.stderr)
    sys.exit(1)


def pick_one(prompt: str, items: list[str]) -> str:
    """対話的に一つ選択する"""
    if not items:
        die(f"{prompt}: 選択肢がありません")

    if len(items) == 1:
        return items[0]

    print(f"\n{prompt}:")
    for i, item in enumerate(items, 1):
        print(f"  {i}) {item}")

    while True:
        try:
            choice = input("番号を入力> ").strip()
            idx = int(choice) - 1
            if 0 <= idx < len(items):
                return items[idx]
            print("無効な番号です。もう一度入力してください。")
        except ValueError:
            print("数値を入力してください。")
        except (EOFError, KeyboardInterrupt):
            die("選択がキャンセルされました")


def parse_datetime(s: str) -> datetime:
    """日時文字列をパースする"""
    for fmt in [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M",
        "%Y-%m-%d",
    ]:
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    die(f"日時のパースに失敗しました: {s} (例: 2024-01-01 09:00)")


# ---------------------------------------------------------------------------
# AWS 操作
# ---------------------------------------------------------------------------


def get_webacls(session: boto3.Session, region: str) -> list[dict]:
    """WebACL 一覧を取得する（リージョナル + CloudFront）"""
    webacls = []

    # リージョナル WAF
    try:
        wafv2 = session.client("wafv2", region_name=region)
        resp = wafv2.list_web_acls(Scope="REGIONAL")
        for acl in resp.get("WebACLs", []):
            webacls.append(
                {
                    "name": acl["Name"],
                    "arn": acl["ARN"],
                    "scope": "REGIONAL",
                    "region": region,
                }
            )
    except ClientError as e:
        print(f"警告: リージョナル WAF の取得に失敗: {e}", file=sys.stderr)

    # CloudFront WAF (us-east-1)
    try:
        wafv2_cf = session.client("wafv2", region_name="us-east-1")
        resp = wafv2_cf.list_web_acls(Scope="CLOUDFRONT")
        for acl in resp.get("WebACLs", []):
            webacls.append(
                {
                    "name": acl["Name"],
                    "arn": acl["ARN"],
                    "scope": "CLOUDFRONT",
                    "region": "us-east-1",
                }
            )
    except ClientError as e:
        print(f"警告: CloudFront WAF の取得に失敗: {e}", file=sys.stderr)

    return webacls


def get_logging_config(session: boto3.Session, webacl: dict) -> str | None:
    """WebACL のログ配信先 S3 バケットを取得する"""
    wafv2 = session.client("wafv2", region_name=webacl["region"])
    try:
        resp = wafv2.get_logging_configuration(ResourceArn=webacl["arn"])
        destinations = resp["LoggingConfiguration"]["LogDestinationConfigs"]
        for dest in destinations:
            # arn:aws:s3:::bucket-name の形式
            if ":s3:::" in dest:
                return dest.split(":s3:::")[-1]
        return None
    except ClientError:
        return None


def list_log_objects(
    session: boto3.Session,
    bucket: str,
    prefix: str,
    start: datetime,
    end: datetime,
) -> list[str]:
    """期間内の WAF ログオブジェクトキーを列挙する"""
    s3 = session.client("s3")
    keys = []

    # 5分単位でプレフィックスを絞り込む
    current = start.replace(minute=(start.minute // 5) * 5, second=0, microsecond=0)
    prefixes_to_scan = set()
    while current <= end:
        date_prefix = current.strftime("%Y/%m/%d/%H/%M/")
        prefixes_to_scan.add(f"{prefix}{date_prefix}")
        current += timedelta(minutes=5)

    for scan_prefix in sorted(prefixes_to_scan):
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=scan_prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith(".log.gz"):
                    keys.append(key)

    return keys


def download_logs(
    session: boto3.Session,
    bucket: str,
    keys: list[str],
    dest_dir: Path,
    max_workers: int = 10,
) -> list[Path]:
    """S3 からログファイルを並列ダウンロードする"""
    s3 = session.client("s3")
    downloaded = []

    def _download(key: str) -> Path:
        local_path = dest_dir / key.replace("/", "_")
        s3.download_file(bucket, key, str(local_path))
        return local_path

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_download, key): key for key in keys}
        for i, future in enumerate(as_completed(futures), 1):
            try:
                path = future.result()
                downloaded.append(path)
                print(f"\r  ダウンロード: {i}/{len(keys)}", end="", flush=True)
            except Exception as e:
                key = futures[future]
                print(
                    f"\n  警告: {key} のダウンロードに失敗: {e}",
                    file=sys.stderr,
                )

    print()
    return downloaded


# ---------------------------------------------------------------------------
# DuckDB
# ---------------------------------------------------------------------------


def load_to_duckdb(db: duckdb.DuckDBPyConnection, files: list[Path]) -> int:
    """gzip JSON ログを DuckDB に取り込む"""
    all_lines = []
    for f in files:
        with gzip.open(f, "rt", encoding="utf-8") as gz:
            for line in gz:
                line = line.strip()
                if line:
                    all_lines.append(line)

    if not all_lines:
        return 0

    # 一時 JSONL ファイルに結合して DuckDB で読み込む
    jsonl_path = files[0].parent / "waf_logs.jsonl"
    with open(jsonl_path, "w", encoding="utf-8") as out:
        out.write("\n".join(all_lines))

    db.execute(
        f"""
        CREATE TABLE waf_logs AS
        SELECT * FROM read_json_auto(
            '{jsonl_path}',
            maximum_object_size=10485760,
            ignore_errors=true
        )
    """
    )

    count = db.execute("SELECT count(*) FROM waf_logs").fetchone()[0]
    return count


def create_views(db: duckdb.DuckDBPyConnection) -> None:
    """分析用の便利なビューを作成する"""
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


# ---------------------------------------------------------------------------
# SQL シェル
# ---------------------------------------------------------------------------

HELP_TEXT = """
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


def _build_claude_context(db_path: str, summary: str) -> str:
    """claude に渡す初回コンテキストを構築する"""
    venv_hint = ""
    if sys.prefix != sys.base_prefix:
        activate = Path(sys.prefix) / "bin" / "activate"
        venv_hint = (
            "venv 環境で実行しています。duckdb コマンドを使う前に "
            f"activate してください:\n  source {activate}\n"
        )

    return f"""WAF ログが DuckDB に取り込まれています。分析を手伝ってください。

DB ファイル: {db_path}
{venv_hint}
{summary}

利用可能なビュー:
  top_blocked      ブロックされたリクエストの IP 別集計
  top_rules        マッチしたルール別集計
  top_uri          URI 別リクエスト数
  timeline         5分間隔の時系列リクエスト数
  top_countries    国別リクエスト数
  blocked_details  ブロックされたリクエストの詳細

duckdb コマンドで SQL を実行して分析してください。"""


def _launch_claude(
    message: str,
    db_path: str,
    summary: str,
    session_id: str,
    is_first: bool,
) -> None:
    """claude を対話的に起動する。終了後に SQL シェルに戻る。"""
    if not shutil.which("claude"):
        print("エラー: claude コマンドが見つかりません", file=sys.stderr)
        return

    if is_first:
        context = _build_claude_context(db_path, summary)
        prompt = f"{context}\n\n---\n\n{message}"
        cmd = ["claude", "--session-id", session_id, prompt]
    else:
        cmd = ["claude", "-r", session_id, message]

    subprocess.run(cmd)
    print("\n-- SQL シェルに戻りました (.help でヘルプ表示)")


def sql_shell(
    db: duckdb.DuckDBPyConnection,
    db_path: str,
    summary: str,
) -> None:
    """対話的 SQL シェルを起動する"""
    print(HELP_TEXT)

    claude_session_id = str(uuid.uuid4())
    claude_first = True

    while True:
        try:
            query = input("waf> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n終了します。")
            break

        if not query:
            continue
        if query.lower() in (".quit", ".exit", "quit", "exit"):
            break
        if query.lower() in (".help", "help"):
            print(HELP_TEXT)
            continue
        if query.lower() == ".tables":
            db.sql(
                "SELECT table_name, table_type FROM information_schema.tables "
                "WHERE table_schema = 'main' ORDER BY table_type, table_name"
            ).show()
            continue
        if query.lower() == ".schema":
            db.sql("DESCRIBE waf_logs").show()
            continue

        # .claude コマンド
        if query.lower().startswith(".claude"):
            message = query[len(".claude"):].strip()
            if not message:
                print("使い方: .claude <質問>")
                continue
            # claude が DB にアクセスできるようロックを解放
            db.close()
            _launch_claude(
                message, db_path, summary,
                claude_session_id, claude_first,
            )
            claude_first = False
            db = duckdb.connect(db_path)
            create_views(db)
            continue

        # セミコロンで終わるまで複数行入力
        while not query.endswith(";"):
            try:
                line = input("  -> ").strip()
                query += " " + line
            except (EOFError, KeyboardInterrupt):
                query = ""
                break

        if not query:
            continue

        try:
            result = db.sql(query)
            result.show()
        except Exception as e:
            print(f"エラー: {e}", file=sys.stderr)


# ---------------------------------------------------------------------------
# 概要表示
# ---------------------------------------------------------------------------


def print_summary(db: duckdb.DuckDBPyConnection) -> None:
    """取り込んだログの概要を表示する"""
    total = db.execute("SELECT count(*) FROM waf_logs").fetchone()[0]
    actions = db.execute(
        "SELECT action, count(*) AS cnt FROM waf_logs GROUP BY action ORDER BY cnt DESC"
    ).fetchall()
    time_range = db.execute(
        "SELECT min(to_timestamp(timestamp / 1000)), "
        "max(to_timestamp(timestamp / 1000)) FROM waf_logs"
    ).fetchone()

    print(f"\n=== ログ概要 ===")
    print(f"  レコード数: {total:,}")
    print(f"  期間: {time_range[0]} ~ {time_range[1]}")
    print(f"  アクション別:")
    for action, cnt in actions:
        print(f"    {action}: {cnt:,}")


# ---------------------------------------------------------------------------
# メイン
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="WAF ログ分析ツール")
    parser.add_argument("--profile", "-p", help="AWS プロファイル")
    parser.add_argument(
        "--region",
        "-r",
        default="ap-northeast-1",
        help="AWS リージョン (default: ap-northeast-1)",
    )
    parser.add_argument(
        "--from",
        dest="time_from",
        help="開始日時 (例: 2024-01-01 09:00)",
    )
    parser.add_argument(
        "--to",
        dest="time_to",
        help="終了日時 (例: 2024-01-01 10:00)",
    )
    parser.add_argument(
        "--hours",
        type=int,
        help="直近N時間を取得 (--from/--to 未指定時のデフォルト: 1)",
    )
    parser.add_argument(
        "--db",
        help="DuckDB ファイルパス (デフォルト: /tmp/waf-logs-{timestamp}.duckdb)",
    )
    parser.add_argument(
        "--memory",
        action="store_true",
        help="インメモリモード (DB ファイルを作成しない)",
    )
    parser.add_argument(
        "--local-dir",
        help="ダウンロード済みログの .log.gz があるディレクトリ (S3 取得をスキップ)",
    )
    return parser.parse_args()


def _get_summary_text(db: duckdb.DuckDBPyConnection) -> str:
    """ログ概要をテキストで返す"""
    total = db.execute("SELECT count(*) FROM waf_logs").fetchone()[0]
    actions = db.execute(
        "SELECT action, count(*) AS cnt FROM waf_logs GROUP BY action ORDER BY cnt DESC"
    ).fetchall()
    time_range = db.execute(
        "SELECT min(to_timestamp(timestamp / 1000)), "
        "max(to_timestamp(timestamp / 1000)) FROM waf_logs"
    ).fetchone()
    schema = db.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_name = 'waf_logs' ORDER BY ordinal_position"
    ).fetchall()

    lines = [
        f"レコード数: {total:,}",
        f"期間: {time_range[0]} ~ {time_range[1]}",
        "アクション別:",
    ]
    for action, cnt in actions:
        lines.append(f"  {action}: {cnt:,}")
    lines.append("")
    lines.append("主要カラム:")
    for col_name, col_type in schema[:15]:
        lines.append(f"  {col_name} ({col_type})")
    if len(schema) > 15:
        lines.append(f"  ... 他 {len(schema) - 15} カラム")

    return "\n".join(lines)


def main() -> None:
    args = parse_args()

    # --- DB パスの決定 ---
    if args.memory:
        db_path = ":memory:"
    elif args.db:
        db_path = args.db
    else:
        ts = datetime.now().strftime("%Y%m%d-%H%M%S")
        db_path = f"/tmp/waf-logs-{ts}.duckdb"

    db = duckdb.connect(db_path)

    # 既存の DB ファイルに waf_logs テーブルがあればそのまま再利用
    existing = db.execute(
        "SELECT count(*) FROM information_schema.tables "
        "WHERE table_name = 'waf_logs' AND table_type = 'BASE TABLE'"
    ).fetchone()[0]

    if existing:
        count = db.execute("SELECT count(*) FROM waf_logs").fetchone()[0]
        print(f"-- 既存の DuckDB ファイルを使用: {db_path} ({count:,} 件)")
    else:
        # --- ログファイルの取得 ---
        files = _fetch_log_files(args)

        print("DuckDB に取り込み中...")
        count = load_to_duckdb(db, files)
        if count == 0:
            die("ログレコードが 0 件です")

    # ビュー作成
    try:
        create_views(db)
    except Exception as e:
        print(f"警告: 一部のビュー作成に失敗しました: {e}", file=sys.stderr)

    # 概要表示
    print_summary(db)

    if db_path != ":memory:":
        print(f"-- DB ファイル: {db_path}")

    # SQL シェル起動（.claude コマンドで AI 分析も可能）
    summary = _get_summary_text(db)
    sql_shell(db, db_path, summary)
    db.close()


def _fetch_log_files(args: argparse.Namespace) -> list[Path]:
    """ローカルまたは S3 からログファイルを取得する"""
    local_dir = Path(args.local_dir) if args.local_dir else None

    if local_dir:
        if not local_dir.is_dir():
            die(f"ディレクトリが見つかりません: {local_dir}")
        files = sorted(local_dir.glob("*.log.gz"))
        if not files:
            die(f".log.gz ファイルが見つかりません: {local_dir}")
        print(f"-- ローカルディレクトリ: {local_dir}")
        print(f"  {len(files)} ファイル見つかりました")
        return files

    # --- AWS セッション ---
    session_kwargs = {"region_name": args.region}
    if args.profile:
        session_kwargs["profile_name"] = args.profile

    try:
        session = boto3.Session(**session_kwargs)
        sts = session.client("sts")
        identity = sts.get_caller_identity()
    except (NoCredentialsError, ClientError) as e:
        die(f"AWS 認証に失敗しました: {e}")

    print(f"-- AWS アカウント: {identity['Account']}")
    print(f"-- IAM: {identity['Arn']}")
    print(f"-- リージョン: {args.region}")
    if args.profile:
        print(f"-- プロファイル: {args.profile}")

    # --- WebACL 選択 ---
    webacls = get_webacls(session, args.region)
    if not webacls:
        die("WebACL が見つかりません")

    display = [f"{w['name']}  ({w['scope']})" for w in webacls]
    selected = pick_one("WebACL を選択", display)
    idx = display.index(selected)
    webacl = webacls[idx]
    print(f"-- WebACL: {webacl['name']} ({webacl['scope']})")

    # --- ログ配信先取得 ---
    log_dest = get_logging_config(session, webacl)
    if not log_dest:
        die(
            f"WebACL '{webacl['name']}' のログ配信設定が見つかりません。\n"
            "  WAF コンソールでログ記録を S3 に有効化してください。"
        )

    # S3 バケットとプレフィックスを分離
    parts = log_dest.split("/", 1)
    bucket = parts[0]
    base_prefix = parts[1] if len(parts) > 1 else ""
    if base_prefix and not base_prefix.endswith("/"):
        base_prefix += "/"
    print(f"-- S3: s3://{bucket}/{base_prefix}")

    # --- 期間の決定 ---
    now = datetime.now(timezone.utc)
    if args.time_from:
        start = parse_datetime(args.time_from)
        end = parse_datetime(args.time_to) if args.time_to else now
    else:
        hours = args.hours or 1
        start = now - timedelta(hours=hours)
        end = now

    print(
        f"-- 期間: {start.strftime('%Y-%m-%d %H:%M')} ~ "
        f"{end.strftime('%Y-%m-%d %H:%M')} (UTC)"
    )

    # --- ログの S3 プレフィックス構築 ---
    account_id = identity["Account"]
    waf_region = (
        "cloudfront" if webacl["scope"] == "CLOUDFRONT" else webacl["region"]
    )
    log_prefix = (
        f"{base_prefix}AWSLogs/{account_id}/WAFLogs/"
        f"{waf_region}/{webacl['name']}/"
    )

    # --- S3 オブジェクト列挙 ---
    print("\nログファイルを検索中...")
    keys = list_log_objects(session, bucket, log_prefix, start, end)
    if not keys:
        die("指定期間のログファイルが見つかりません")
    print(f"  {len(keys)} ファイル見つかりました")

    # --- ダウンロード ---
    tmp_dir = Path(tempfile.mkdtemp(prefix="waf-logs-"))
    print(f"  ダウンロード先: {tmp_dir}")
    files = download_logs(session, bucket, keys, tmp_dir)
    if not files:
        die("ダウンロードしたファイルがありません")

    return files


if __name__ == "__main__":
    main()
