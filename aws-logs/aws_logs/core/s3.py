"""S3 操作"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import boto3

from .utils import die


def list_objects(
    session: boto3.Session,
    bucket: str,
    prefix: str,
    start: datetime,
    end: datetime,
    provider,
) -> list[str]:
    """期間内のログオブジェクトキーを列挙する"""
    s3 = session.client("s3")
    keys = []

    prefixes_to_scan = provider.build_time_prefixes(prefix, start, end)

    for scan_prefix in sorted(prefixes_to_scan):
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=scan_prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith(provider.file_extension):
                    keys.append(key)

    return keys


def download_objects(
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
                import sys
                print(
                    f"\n  警告: {key} のダウンロードに失敗: {e}",
                    file=sys.stderr,
                )

    print()
    return downloaded
