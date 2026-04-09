#!/usr/bin/env python3
"""
WAF ログのテストデータを生成するスクリプト。
生成された .log.gz ファイルは --local-dir オプションで waf_logs に読み込める。

Usage:
    python generate_test_data.py [--output-dir DIR] [--records N]
"""

import argparse
import gzip
import json
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# データ定義
# ---------------------------------------------------------------------------

ACCOUNT_ID = "123456789012"
WEBACL_NAME = "test-webacl"
WEBACL_ID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
WEBACL_ARN = (
    f"arn:aws:wafv2:ap-northeast-1:{ACCOUNT_ID}:regional/webacl/"
    f"{WEBACL_NAME}/{WEBACL_ID}"
)

CLIENT_IPS = [
    # 正常トラフィック
    "203.0.113.10",
    "203.0.113.11",
    "203.0.113.12",
    "198.51.100.20",
    "198.51.100.21",
    # 攻撃元（ブロック多め）
    "192.0.2.100",
    "192.0.2.101",
    "192.0.2.200",
    "10.0.0.50",
    "172.16.0.99",
]

COUNTRIES = ["JP", "JP", "JP", "US", "US", "CN", "CN", "RU", "DE", "KR"]

NORMAL_URIS = [
    "/",
    "/index.html",
    "/api/v1/users",
    "/api/v1/items",
    "/api/v1/orders",
    "/api/v1/health",
    "/assets/style.css",
    "/assets/app.js",
    "/images/logo.png",
    "/favicon.ico",
]

ATTACK_URIS = [
    "/wp-admin/admin-ajax.php",
    "/wp-login.php",
    "/.env",
    "/admin",
    "/phpmyadmin/",
    "/api/v1/users?id=1 OR 1=1",
    "/api/v1/search?q=<script>alert(1)</script>",
    "/etc/passwd",
    "/../../../etc/shadow",
    "/cgi-bin/test.cgi",
    "/xmlrpc.php",
    "/api/v1/items?sort=name;DROP TABLE items;--",
    "/.git/config",
    "/server-status",
    "/actuator/env",
]

HTTP_METHODS = ["GET", "GET", "GET", "POST", "POST", "PUT", "DELETE", "OPTIONS"]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15",
    "Mozilla/5.0 (Linux; Android 14) AppleWebKit/537.36 Chrome/120.0.0.0 Mobile",
    "python-requests/2.31.0",
    "curl/8.4.0",
    "Go-http-client/2.0",
    "sqlmap/1.7",
    "Nikto/2.5.0",
    "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
]

RULES = {
    "BLOCK": [
        ("AWS-AWSManagedRulesCommonRuleSet", "MANAGED_RULE_GROUP", "CrossSiteScripting_BODY"),
        ("AWS-AWSManagedRulesCommonRuleSet", "MANAGED_RULE_GROUP", "SizeRestrictions_BODY"),
        ("AWS-AWSManagedRulesSQLiRuleSet", "MANAGED_RULE_GROUP", "SQLi_BODY"),
        ("AWS-AWSManagedRulesSQLiRuleSet", "MANAGED_RULE_GROUP", "SQLi_QUERYARGUMENTS"),
        ("AWS-AWSManagedRulesKnownBadInputsRuleSet", "MANAGED_RULE_GROUP", "Log4JRCE_BODY"),
        ("RateLimit-per-IP", "RATE_BASED", None),
        ("BlockBadBots", "REGULAR", None),
        ("GeoBlock", "REGULAR", None),
    ],
    "COUNT": [
        ("AWS-AWSManagedRulesCommonRuleSet", "MANAGED_RULE_GROUP", "GenericRFI_BODY"),
        ("AWS-AWSManagedRulesBotControlRuleSet", "MANAGED_RULE_GROUP", "CategoryHttpLibrary"),
    ],
}


# ---------------------------------------------------------------------------
# ログ生成
# ---------------------------------------------------------------------------


def generate_headers(user_agent: str, host: str = "example.com") -> list[dict]:
    headers = [
        {"name": "Host", "value": host},
        {"name": "User-Agent", "value": user_agent},
        {"name": "Accept", "value": "text/html,application/json"},
        {"name": "Accept-Language", "value": "ja,en;q=0.9"},
        {"name": "Accept-Encoding", "value": "gzip, deflate, br"},
    ]
    if random.random() < 0.3:
        headers.append({"name": "Referer", "value": f"https://{host}/"})
    if random.random() < 0.2:
        headers.append(
            {"name": "Cookie", "value": f"session_id={uuid.uuid4().hex[:32]}"}
        )
    if random.random() < 0.1:
        headers.append(
            {"name": "Authorization", "value": f"Bearer test-token-{uuid.uuid4().hex[:16]}"}
        )
    if random.random() < 0.15:
        headers.append({"name": "X-Forwarded-For", "value": f"10.0.{random.randint(0,255)}.{random.randint(1,254)}"})
    return headers


def generate_rule_group_list(action: str, rule_info: tuple | None) -> list[dict]:
    if not rule_info or rule_info[1] != "MANAGED_RULE_GROUP":
        return []
    rule_group_id, _, matching_rule = rule_info
    return [
        {
            "ruleGroupId": rule_group_id,
            "terminatingRule": {
                "ruleId": matching_rule,
                "action": action,
            }
            if action == "BLOCK"
            else None,
            "nonTerminatingMatchingRules": [
                {"ruleId": matching_rule, "action": "COUNT"}
            ]
            if action == "COUNT"
            else [],
            "excludedRules": [],
        }
    ]


def generate_record(ts: datetime, action: str, is_attack: bool) -> dict:
    if is_attack:
        client_ip = random.choice(CLIENT_IPS[5:])  # 攻撃元 IP
        uri = random.choice(ATTACK_URIS)
        country = random.choice(["CN", "RU", "KR", "US"])
        user_agent = random.choice(USER_AGENTS[4:])  # ツール系 UA
    else:
        client_ip = random.choice(CLIENT_IPS[:5])  # 正常 IP
        uri = random.choice(NORMAL_URIS)
        country = random.choice(["JP", "JP", "JP", "US"])
        user_agent = random.choice(USER_AGENTS[:4])  # ブラウザ系 UA

    method = random.choice(HTTP_METHODS)

    if action == "ALLOW":
        terminating_rule_id = "Default_Action"
        terminating_rule_type = "REGULAR"
        rule_info = None
    elif action == "BLOCK":
        rule_info = random.choice(RULES["BLOCK"])
        terminating_rule_id = rule_info[0]
        terminating_rule_type = rule_info[1]
    else:  # COUNT
        rule_info = random.choice(RULES["COUNT"])
        terminating_rule_id = "Default_Action"
        terminating_rule_type = "REGULAR"

    timestamp_ms = int(ts.timestamp() * 1000)

    record = {
        "timestamp": timestamp_ms,
        "formatVersion": 1,
        "webaclId": WEBACL_ARN,
        "terminatingRuleId": terminating_rule_id,
        "terminatingRuleType": terminating_rule_type,
        "action": action,
        "terminatingRuleMatchDetails": [],
        "httpSourceName": "ALB",
        "httpSourceId": (
            f"{ACCOUNT_ID}-app/test-alb/"
            f"{uuid.uuid4().hex[:16]}"
        ),
        "ruleGroupList": generate_rule_group_list(action, rule_info),
        "rateBasedRuleList": [],
        "nonTerminatingMatchingRules": [],
        "requestHeadersInserted": None,
        "responseCodeSent": None,
        "httpRequest": {
            "clientIp": client_ip,
            "country": country,
            "headers": generate_headers(user_agent),
            "uri": uri,
            "args": "",
            "httpVersion": "HTTP/2.0",
            "httpMethod": method,
            "requestId": str(uuid.uuid4()),
        },
        "labels": [],
    }

    # RATE_BASED の場合
    if rule_info and rule_info[1] == "RATE_BASED":
        record["rateBasedRuleList"] = [
            {
                "rateBasedRuleId": rule_info[0],
                "limitKey": "IP",
                "maxRateAllowed": 2000,
            }
        ]

    return record


def generate_logs(
    num_records: int,
    start: datetime,
    end: datetime,
) -> list[dict]:
    """指定期間にわたるログレコードを生成する"""
    records = []
    duration = (end - start).total_seconds()

    for _ in range(num_records):
        offset = random.random() * duration
        ts = start + timedelta(seconds=offset)

        # 確率: ALLOW 60%, BLOCK 30%, COUNT 10%
        r = random.random()
        if r < 0.60:
            action = "ALLOW"
            is_attack = False
        elif r < 0.90:
            action = "BLOCK"
            is_attack = True
        else:
            action = "COUNT"
            is_attack = random.random() < 0.7

        records.append(generate_record(ts, action, is_attack))

    records.sort(key=lambda x: x["timestamp"])
    return records


# ---------------------------------------------------------------------------
# メイン
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description="WAF ログテストデータ生成")
    parser.add_argument(
        "--output-dir",
        "-o",
        default="/tmp/waf-test-data",
        help="出力ディレクトリ (default: /tmp/waf-test-data)",
    )
    parser.add_argument(
        "--records",
        "-n",
        type=int,
        default=500,
        help="生成レコード数 (default: 500)",
    )
    parser.add_argument(
        "--hours",
        type=int,
        default=3,
        help="ログの期間（時間） (default: 3)",
    )
    parser.add_argument(
        "--files",
        type=int,
        default=5,
        help="分割する .log.gz ファイル数 (default: 5)",
    )
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    end = datetime.now(timezone.utc)
    start = end - timedelta(hours=args.hours)

    print(f"テストデータ生成中...")
    print(f"  レコード数: {args.records}")
    print(f"  期間: {start.strftime('%Y-%m-%d %H:%M')} ~ {end.strftime('%Y-%m-%d %H:%M')} (UTC)")
    print(f"  ファイル数: {args.files}")

    records = generate_logs(args.records, start, end)

    # ファイルに分割して書き出し
    chunk_size = len(records) // args.files
    for i in range(args.files):
        chunk_start = i * chunk_size
        chunk_end = chunk_start + chunk_size if i < args.files - 1 else len(records)
        chunk = records[chunk_start:chunk_end]

        filename = (
            f"aws-waf-logs-test_{start.strftime('%Y%m%d%H%M')}_{i:03d}.log.gz"
        )
        filepath = output_dir / filename

        with gzip.open(filepath, "wt", encoding="utf-8") as gz:
            for record in chunk:
                gz.write(json.dumps(record, ensure_ascii=False) + "\n")

    print(f"\n出力先: {output_dir}")
    print(f"  {args.files} ファイル生成完了")
    print(f"\n使い方:")
    print(f"  python waf_logs.py --local-dir {output_dir}")


if __name__ == "__main__":
    main()
