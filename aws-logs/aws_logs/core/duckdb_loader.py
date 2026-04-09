"""DuckDB 操作"""

from datetime import datetime

import boto3
import duckdb

from .utils import die


def create_db(db_path: str) -> duckdb.DuckDBPyConnection:
    """DuckDB 接続を作成する"""
    return duckdb.connect(db_path)


def resolve_db_path(db: str | None, memory: bool, name: str) -> str:
    """DB ファイルパスを決定する"""
    if memory:
        return ":memory:"
    if db:
        return db
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    return f"/tmp/{name}-{ts}.duckdb"


def check_existing_table(db: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    """既存テーブルの有無を確認する"""
    count = db.execute(
        "SELECT count(*) FROM information_schema.tables "
        "WHERE table_name = ? AND table_type = 'BASE TABLE'",
        [table_name],
    ).fetchone()[0]
    return count > 0


def configure_s3_access(
    db: duckdb.DuckDBPyConnection, session: boto3.Session, region: str
) -> None:
    """boto3 セッションの認証情報を DuckDB httpfs に設定する"""
    db.execute("INSTALL httpfs")
    db.execute("LOAD httpfs")

    credentials = session.get_credentials()
    if credentials is None:
        die("AWS 認証情報を取得できませんでした")

    frozen = credentials.get_frozen_credentials()
    db.execute(f"SET s3_region = '{region}'")
    db.execute(f"SET s3_access_key_id = '{frozen.access_key}'")
    db.execute(f"SET s3_secret_access_key = '{frozen.secret_key}'")
    if frozen.token:
        db.execute(f"SET s3_session_token = '{frozen.token}'")


def execute_create_table(db: duckdb.DuckDBPyConnection, sql: str) -> int:
    """CREATE TABLE を実行しレコード数を返す"""
    db.execute(sql)
    # テーブル名を SQL から抽出
    # "CREATE TABLE xxx AS ..." の形式を想定
    parts = sql.split()
    table_idx = parts.index("TABLE") + 1
    table_name = parts[table_idx]
    count = db.execute(f"SELECT count(*) FROM {table_name}").fetchone()[0]
    return count
