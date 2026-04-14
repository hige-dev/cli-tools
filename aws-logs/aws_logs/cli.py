"""aws-logs - AWS ログ分析ツール (WAF / CloudFront / ALB / CloudWatch Logs)"""

import argparse
import sys
import tempfile
from pathlib import Path

try:
    import boto3
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

from .core.aws_session import (
    create_session,
    get_account_id,
    print_session_info,
    resolve_time_range,
)
from .core.duckdb_loader import (
    check_existing_table,
    configure_s3_access,
    create_db,
    execute_create_table,
    resolve_db_path,
)
from .core.s3 import download_objects, list_objects
from .core.sql_shell import sql_shell
from .core.utils import die
from .providers.alb import AlbProvider
from .providers.base import LogProvider
from .providers.cloudfront import CloudFrontProvider
from .providers.cwl import CwlProvider
from .providers.waf import WafProvider


PROVIDERS = {
    "waf": WafProvider,
    "cf": CloudFrontProvider,
    "alb": AlbProvider,
    "cwl": CwlProvider,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="aws_logs",
        description="AWS ログ分析ツール (WAF / CloudFront / ALB / CloudWatch Logs)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "使用例:\n"
            "  aws_logs waf                        直近1時間の WAF ログを分析\n"
            "  aws_logs cf --hours 6               直近6時間の CloudFront ログ\n"
            "  aws_logs alb --from '2026-04-15 09:00' --to '2026-04-15 10:00'\n"
            "  aws_logs cwl -p production           本番環境の CloudWatch Logs\n"
            "  aws_logs waf --db ./saved.duckdb     保存済み DB を再利用"
        ),
    )

    subparsers = parser.add_subparsers(dest="subcommand", help="ログ種別")

    # 各サブコマンドに共通オプションを追加するヘルパー
    def add_common_args(sub: argparse.ArgumentParser, is_s3: bool = True) -> None:
        # 期間指定グループ
        time_group = sub.add_argument_group("期間指定")
        time_group.add_argument(
            "--from", dest="time_from",
            metavar="DATETIME",
            help=(
                "開始日時。以下の形式に対応:\n"
                "  '2026-04-15 09:00'  (日時)\n"
                "  '2026-04-15'        (日付のみ = 00:00)\n"
                "  未指定時は --hours で直近N時間を取得"
            ),
        )
        time_group.add_argument(
            "--to", dest="time_to",
            metavar="DATETIME",
            help="終了日時 (--from と同じ形式。未指定時は現在時刻)",
        )
        time_group.add_argument(
            "--hours", type=int,
            metavar="N",
            help="直近N時間を取得 (--from/--to 未指定時のデフォルト: 1)",
        )

        # AWS 接続グループ
        aws_group = sub.add_argument_group("AWS 接続")
        aws_group.add_argument(
            "--profile", "-p",
            metavar="NAME",
            help="AWS プロファイル名 (~/.aws/config のプロファイル)",
        )
        aws_group.add_argument(
            "--region", "-r",
            default="ap-northeast-1",
            metavar="REGION",
            help="AWS リージョン (デフォルト: ap-northeast-1)",
        )

        # DB グループ
        db_group = sub.add_argument_group("データベース")
        db_group.add_argument(
            "--db",
            metavar="PATH",
            help=(
                "DuckDB ファイルパス。既存ファイルを指定すると\n"
                "ログ取得をスキップして即座に分析を開始する\n"
                "(デフォルト: /tmp/{name}-{timestamp}.duckdb)"
            ),
        )
        db_group.add_argument(
            "--memory", action="store_true",
            help="インメモリモード (DB ファイルをディスクに残さない)",
        )

        if is_s3:
            s3_group = sub.add_argument_group("取得方式 (S3)")
            s3_group.add_argument(
                "--local-dir",
                metavar="DIR",
                help="ダウンロード済みログのディレクトリを指定 (S3 取得をスキップ)",
            )
            s3_group.add_argument(
                "--download", action="store_true",
                help="S3 からローカルにダウンロードしてから取り込む (従来方式)",
            )
        else:
            # 非S3 プロバイダーでも argparse エラーを防ぐためデフォルト値を設定
            sub.set_defaults(local_dir=None, download=False)

    # サブコマンド
    waf_parser = subparsers.add_parser(
        "waf", help="WAF ログ分析",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="AWS WAF ログを S3 から取り込んで分析する",
        epilog=(
            "使用例:\n"
            "  aws_logs waf                        直近1時間\n"
            "  aws_logs waf --hours 24              直近24時間\n"
            "  aws_logs waf --from '2026-04-14' --to '2026-04-15'\n"
            "  aws_logs waf -p staging --db ./waf.duckdb"
        ),
    )
    add_common_args(waf_parser)

    cf_parser = subparsers.add_parser(
        "cf", help="CloudFront ログ分析",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="CloudFront アクセスログを S3 から取り込んで分析する",
        epilog=(
            "使用例:\n"
            "  aws_logs cf                         直近1時間\n"
            "  aws_logs cf --hours 6 -p production\n"
            "  aws_logs cf --from '2026-04-15 09:00' --to '2026-04-15 10:00'"
        ),
    )
    add_common_args(cf_parser)

    alb_parser = subparsers.add_parser(
        "alb", help="ALB ログ分析",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="ALB アクセスログを S3 から取り込んで分析する",
        epilog=(
            "使用例:\n"
            "  aws_logs alb                        直近1時間\n"
            "  aws_logs alb --hours 3 --memory\n"
            "  aws_logs alb --from '2026-04-15 14:00' --to '2026-04-15 15:00'"
        ),
    )
    add_common_args(alb_parser)

    cwl_parser = subparsers.add_parser(
        "cwl", help="CloudWatch Logs 分析",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=(
            "CloudWatch Logs のログイベントを API 経由で取得し分析する\n"
            "ECS・Lambda などのロググループに対応"
        ),
        epilog=(
            "使用例:\n"
            "  aws_logs cwl                        直近1時間\n"
            "  aws_logs cwl --hours 3 -p production\n"
            "  aws_logs cwl --from '2026-04-15 09:00' --to '2026-04-15 10:00'\n"
            "\n"
            "注意:\n"
            "  CloudWatch Logs API (FilterLogEvents) で取得するため、\n"
            "  大量のログ取得時には時間がかかる場合があります"
        ),
    )
    add_common_args(cwl_parser, is_s3=False)

    args = parser.parse_args()

    if not args.subcommand:
        parser.print_help()
        sys.exit(1)

    return args


def run(args: argparse.Namespace, provider: LogProvider) -> None:
    """共通のログ分析フロー"""
    # DB パスの決定
    db_path = resolve_db_path(args.db, args.memory, provider.table_name)
    db = create_db(db_path)

    # 既存テーブルチェック
    if check_existing_table(db, provider.table_name):
        count = db.execute(f"SELECT count(*) FROM {provider.table_name}").fetchone()[0]
        print(f"-- 既存の DuckDB ファイルを使用: {db_path} ({count:,} 件)")
    elif not provider.uses_s3:
        # API 経由でログを取得 (CloudWatch Logs など)
        session = create_session(args.profile, args.region)
        print_session_info(session, args.region, args.profile)
        account_id = get_account_id(session)
        start, end = resolve_time_range(args.time_from, args.time_to, args.hours)

        print(
            f"-- 期間: {start.strftime('%Y-%m-%d %H:%M')} ~ "
            f"{end.strftime('%Y-%m-%d %H:%M')} (UTC)"
        )

        count = provider.fetch_and_load(db, session, args.region, account_id, start, end)
        if count == 0:
            die("ログレコードが 0 件です")
    elif args.local_dir or args.download:
        # ローカルファイルから取り込み
        files = _fetch_local_files(args, provider)
        print("DuckDB に取り込み中...")
        sql = provider.create_table_from_local_sql([str(f) for f in files])
        count = execute_create_table(db, sql)
        if count == 0:
            die("ログレコードが 0 件です")
    else:
        # S3 から直接読み込み
        session = create_session(args.profile, args.region)
        print_session_info(session, args.region, args.profile)
        account_id = get_account_id(session)
        start, end = resolve_time_range(args.time_from, args.time_to, args.hours)

        print(
            f"-- 期間: {start.strftime('%Y-%m-%d %H:%M')} ~ "
            f"{end.strftime('%Y-%m-%d %H:%M')} (UTC)"
        )

        source = provider.discover_log_source(session, args.region, account_id)
        prefix = provider.build_s3_prefix(
            source["base_prefix"], account_id, source["metadata"]
        )

        print("\nログファイルを検索中...")
        keys = list_objects(session, source["bucket"], prefix, start, end, provider)
        if not keys:
            die("指定期間のログファイルが見つかりません")
        print(f"  {len(keys)} ファイル見つかりました")

        print("DuckDB httpfs で S3 から直接取り込み中...")
        configure_s3_access(db, session, args.region)
        s3_urls = [f"s3://{source['bucket']}/{k}" for k in keys]
        sql = provider.create_table_sql(s3_urls)
        count = execute_create_table(db, sql)
        if count == 0:
            die("ログレコードが 0 件です")

    # ビュー作成
    try:
        provider.create_views(db)
    except Exception as e:
        print(f"警告: 一部のビュー作成に失敗しました: {e}", file=sys.stderr)

    # 機微情報警告
    provider.warn_sensitive_data(db)

    # 概要表示
    _print_summary(db, provider)

    if db_path != ":memory:":
        print(f"-- DB ファイル: {db_path}")

    # SQL シェル起動
    summary = _get_summary_text(db, provider)
    sql_shell(db, db_path, summary, provider)
    db.close()


def _fetch_local_files(args: argparse.Namespace, provider: LogProvider) -> list[Path]:
    """ローカルまたは S3 からログファイルを取得する"""
    local_dir = Path(args.local_dir) if args.local_dir else None

    if local_dir:
        if not local_dir.is_dir():
            die(f"ディレクトリが見つかりません: {local_dir}")
        files = sorted(local_dir.glob(f"*{provider.file_extension}"))
        if not files:
            die(f"{provider.file_extension} ファイルが見つかりません: {local_dir}")
        print(f"-- ローカルディレクトリ: {local_dir}")
        print(f"  {len(files)} ファイル見つかりました")
        return files

    # S3 からダウンロード（--download モード）
    session = create_session(args.profile, args.region)
    print_session_info(session, args.region, args.profile)
    account_id = get_account_id(session)
    start, end = resolve_time_range(args.time_from, args.time_to, args.hours)

    print(
        f"-- 期間: {start.strftime('%Y-%m-%d %H:%M')} ~ "
        f"{end.strftime('%Y-%m-%d %H:%M')} (UTC)"
    )

    source = provider.discover_log_source(session, args.region, account_id)
    prefix = provider.build_s3_prefix(
        source["base_prefix"], account_id, source["metadata"]
    )

    print("\nログファイルを検索中...")
    keys = list_objects(session, source["bucket"], prefix, start, end, provider)
    if not keys:
        die("指定期間のログファイルが見つかりません")
    print(f"  {len(keys)} ファイル見つかりました")

    tmp_dir = Path(tempfile.mkdtemp(prefix=f"{provider.table_name}-"))
    print(f"  ダウンロード先: {tmp_dir}")
    files = download_objects(session, source["bucket"], keys, tmp_dir)
    if not files:
        die("ダウンロードしたファイルがありません")

    return files


def _print_summary(db: duckdb.DuckDBPyConnection, provider: LogProvider) -> None:
    """取り込んだログの概要を表示する"""
    queries = provider.get_summary_queries()
    total = db.execute(queries["total"]).fetchone()[0]
    breakdown = db.execute(queries["breakdown"]).fetchall()
    time_range = db.execute(queries["time_range"]).fetchone()

    print(f"\n=== ログ概要 ===")
    print(f"  レコード数: {total:,}")
    print(f"  期間: {time_range[0]} ~ {time_range[1]}")
    label = queries.get("breakdown_label", "内訳")
    print(f"  {label}:")
    for row in breakdown:
        print(f"    {row[0]}: {row[1]:,}")


def _get_summary_text(db: duckdb.DuckDBPyConnection, provider: LogProvider) -> str:
    """ログ概要をテキストで返す"""
    queries = provider.get_summary_queries()
    total = db.execute(queries["total"]).fetchone()[0]
    breakdown = db.execute(queries["breakdown"]).fetchall()
    time_range = db.execute(queries["time_range"]).fetchone()
    schema = db.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_name = ? ORDER BY ordinal_position",
        [provider.table_name],
    ).fetchall()

    label = queries.get("breakdown_label", "内訳")
    lines = [
        f"レコード数: {total:,}",
        f"期間: {time_range[0]} ~ {time_range[1]}",
        f"{label}:",
    ]
    for row in breakdown:
        lines.append(f"  {row[0]}: {row[1]:,}")
    lines.append("")
    lines.append("主要カラム:")
    for col_name, col_type in schema[:15]:
        lines.append(f"  {col_name} ({col_type})")
    if len(schema) > 15:
        lines.append(f"  ... 他 {len(schema) - 15} カラム")

    return "\n".join(lines)


def main() -> None:
    args = parse_args()
    provider_cls = PROVIDERS.get(args.subcommand)
    if not provider_cls:
        die(f"未対応のログ種別: {args.subcommand}")
    provider = provider_cls()
    run(args, provider)


if __name__ == "__main__":
    main()
