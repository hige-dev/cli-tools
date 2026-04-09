"""LogProvider 抽象基底クラス"""

from abc import ABC, abstractmethod

import duckdb


class LogProvider(ABC):
    @property
    @abstractmethod
    def name(self) -> str:
        """ログ種別名 (例: "WAF")"""

    @property
    @abstractmethod
    def table_name(self) -> str:
        """DuckDB テーブル名 (例: "waf_logs")"""

    @property
    @abstractmethod
    def shell_prompt(self) -> str:
        """SQL シェルのプロンプト (例: "waf> ")"""

    @property
    @abstractmethod
    def file_extension(self) -> str:
        """ログファイルの拡張子 (例: ".log.gz")"""

    @abstractmethod
    def discover_log_source(self, session, region: str, account_id: str) -> dict:
        """対話的にログ配信元を選択し S3 バケット/プレフィックスを返す

        戻り値: {"bucket": str, "base_prefix": str, "metadata": dict}
        """

    @abstractmethod
    def build_s3_prefix(self, base_prefix: str, account_id: str, metadata: dict) -> str:
        """S3 のログプレフィックスを構築"""

    @abstractmethod
    def build_time_prefixes(self, prefix: str, start, end) -> set[str]:
        """期間からスキャン対象のプレフィックス一覧を生成"""

    @abstractmethod
    def create_table_sql(self, s3_urls: list[str]) -> str:
        """S3 URL リストから CREATE TABLE 文を生成"""

    @abstractmethod
    def create_table_from_local_sql(self, file_paths: list[str]) -> str:
        """ローカルファイルから CREATE TABLE 文を生成"""

    @abstractmethod
    def create_views(self, db: duckdb.DuckDBPyConnection) -> None:
        """プリセットビュー作成"""

    @abstractmethod
    def get_help_text(self) -> str:
        """SQL シェルのヘルプテキスト"""

    @abstractmethod
    def get_summary_queries(self) -> dict:
        """概要表示用のクエリ群を返す

        戻り値: {"total": str, "breakdown": str, "time_range": str}
        """

    def warn_sensitive_data(self, db: duckdb.DuckDBPyConnection) -> None:
        """機微情報の警告（必要なプロバイダーのみオーバーライド）"""
        pass
