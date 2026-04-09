"""AWS セッション管理"""

import sys
from datetime import datetime, timedelta, timezone

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

from .utils import die, parse_datetime


def create_session(profile: str | None, region: str) -> boto3.Session:
    """AWS セッションを作成する"""
    kwargs = {"region_name": region}
    if profile:
        kwargs["profile_name"] = profile

    try:
        session = boto3.Session(**kwargs)
        # 認証情報の検証
        session.client("sts").get_caller_identity()
        return session
    except (NoCredentialsError, ClientError) as e:
        die(f"AWS 認証に失敗しました: {e}")


def get_account_id(session: boto3.Session) -> str:
    """AWS アカウント ID を取得する"""
    sts = session.client("sts")
    identity = sts.get_caller_identity()
    return identity["Account"]


def print_session_info(session: boto3.Session, region: str, profile: str | None) -> None:
    """セッション情報を表示する"""
    sts = session.client("sts")
    identity = sts.get_caller_identity()
    print(f"-- AWS アカウント: {identity['Account']}")
    print(f"-- IAM: {identity['Arn']}")
    print(f"-- リージョン: {region}")
    if profile:
        print(f"-- プロファイル: {profile}")


def resolve_time_range(
    time_from: str | None,
    time_to: str | None,
    hours: int | None,
) -> tuple[datetime, datetime]:
    """期間を解決する"""
    now = datetime.now(timezone.utc)
    if time_from:
        start = parse_datetime(time_from)
        end = parse_datetime(time_to) if time_to else now
    else:
        h = hours or 1
        start = now - timedelta(hours=h)
        end = now
    return start, end
