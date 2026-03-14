#!/bin/bash
#
# devops-agent.sh - AWS DevOps Agent の対話型 CLI ラッパー
#
# Agent Space の管理、AWS アカウント連携、GitHub リポジトリ連携を
# 対話的に実行する。
#
# Usage:
#   ./devops-agent.sh [サブコマンド]
#
# サブコマンド:
#   setup          サービスモデルのインストール・IAM ロール作成
#   spaces         Agent Space の一覧表示
#   create-space   Agent Space の作成
#   delete-space   Agent Space の削除
#   associate-aws  AWS アカウントの関連付け
#   associate-gh   GitHub リポジトリの関連付け
#   associations   関連付けの一覧表示
#   status         Agent Space の詳細・関連付け状況を表示
#   help           ヘルプ表示
#
# 引数なしで実行すると対話的にサブコマンドを選択できる。
#
# 設定ファイル:
#   ~/.config/devops-agent/config.json
#
# 必要なもの:
#   - AWS CLI v2
#   - jq
#   - curl (setup 時)
#   - fzf (任意: インストール済みなら自動で使用)
#

set -euo pipefail

ENDPOINT_URL="https://api.prod.cp.aidevops.us-east-1.api.aws"
REGION="us-east-1"
SERVICE_MODEL_URL="https://d1co8nkiwcta1g.cloudfront.net/devopsagent.json"

CONFIG_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/devops-agent"
CONFIG_FILE="${CONFIG_DIR}/config.json"

# ---------------------------------------------------------------------------
# ユーティリティ
# ---------------------------------------------------------------------------

die() { echo "エラー: $1" >&2; exit 1; }
info() { echo "-- $1"; }
header() { echo ""; echo "=== $1 ==="; echo ""; }

pick_one() {
    local prompt="$1"
    shift
    local items=("$@")

    if [[ ${#items[@]} -eq 0 ]]; then
        return 1
    fi

    if [[ ${#items[@]} -eq 1 ]]; then
        echo "${items[0]}"
        return
    fi

    if command -v fzf >/dev/null 2>&1; then
        printf '%s\n' "${items[@]}" | fzf --prompt="${prompt}: " --height=~20
    else
        echo "${prompt}:" >&2
        local i
        for i in "${!items[@]}"; do
            echo "  $((i + 1))) ${items[$i]}" >&2
        done
        local choice
        read -rp "番号を入力> " choice
        if [[ "$choice" =~ ^[0-9]+$ ]] && (( choice >= 1 && choice <= ${#items[@]} )); then
            echo "${items[$((choice - 1))]}"
        else
            die "無効な選択です: ${choice}"
        fi
    fi
}

confirm() {
    local msg="${1:-続行しますか？}"
    echo "${msg} [Y/n]" >&2
    local answer
    read -r answer
    [[ ! "$answer" =~ ^[Nn] ]]
}

aws_da() {
    aws devopsagent "$@" \
        ${AWS_PROFILE:+--profile "$AWS_PROFILE"} \
        --endpoint-url "$ENDPOINT_URL" \
        --region "$REGION" \
        --output json
}

aws_iam() {
    aws iam "$@" \
        ${AWS_PROFILE:+--profile "$AWS_PROFILE"} \
        --region "$REGION" \
        --output json
}

# ---------------------------------------------------------------------------
# 設定ファイル
# ---------------------------------------------------------------------------

init_config() {
    if [[ ! -f "$CONFIG_FILE" ]]; then
        return
    fi
    jq empty "$CONFIG_FILE" 2>/dev/null || die "設定ファイルの JSON が不正です: ${CONFIG_FILE}"
}

save_config() {
    mkdir -p "$CONFIG_DIR"
    echo "$1" | jq . > "$CONFIG_FILE"
}

get_config() {
    if [[ ! -f "$CONFIG_FILE" ]]; then
        echo ""
        return
    fi
    jq -r "$1 // empty" "$CONFIG_FILE"
}

set_config() {
    local key="$1" value="$2"
    mkdir -p "$CONFIG_DIR"
    if [[ ! -f "$CONFIG_FILE" ]]; then
        echo '{}' > "$CONFIG_FILE"
    fi
    local tmp
    tmp=$(jq --arg v "$value" "${key} = \$v" "$CONFIG_FILE")
    echo "$tmp" > "$CONFIG_FILE"
}

# ---------------------------------------------------------------------------
# AWS 認証確認
# ---------------------------------------------------------------------------

check_auth() {
    local identity
    identity=$(aws sts get-caller-identity ${AWS_PROFILE:+--profile "$AWS_PROFILE"} --output json 2>/dev/null) \
        || die "AWS 認証に失敗しました${AWS_PROFILE:+ (profile: ${AWS_PROFILE})}"

    local account arn
    account=$(echo "$identity" | jq -r .Account)
    arn=$(echo "$identity" | jq -r .Arn)
    info "AWS アカウント: ${account}"
    info "IAM: ${arn}"
    info "プロファイル: ${AWS_PROFILE:-(デフォルト)}"

    ACCOUNT_ID="$account"
}

# ---------------------------------------------------------------------------
# Agent Space 選択ヘルパー
# ---------------------------------------------------------------------------

select_agent_space() {
    local spaces_json
    spaces_json=$(aws_da list-agent-spaces 2>/dev/null) \
        || die "Agent Space の取得に失敗しました"

    local items
    mapfile -t items < <(
        echo "$spaces_json" | jq -r '
            .agentSpaces[]? |
            .agentSpaceId + "\t" + .name + "\t" + (.status // "unknown")
        '
    )

    [[ ${#items[@]} -gt 0 ]] || die "Agent Space が見つかりません。先に create-space で作成してください"

    local display=()
    for item in "${items[@]}"; do
        [[ -z "$item" ]] && continue
        local id name status
        id=$(echo "$item" | cut -f1)
        name=$(echo "$item" | cut -f2)
        status=$(echo "$item" | cut -f3)
        display+=("${id}  ${name} [${status}]")
    done

    local selected
    selected=$(pick_one "Agent Space" "${display[@]}") || die "Agent Space が選択されませんでした"
    echo "$selected" | awk '{print $1}'
}

# ---------------------------------------------------------------------------
# サブコマンド: setup
# ---------------------------------------------------------------------------

cmd_setup() {
    header "初期セットアップ"

    # 1. サービスモデルのインストール
    echo "1) AWS CLI サービスモデルのインストール"
    echo ""

    if aws devopsagent help >/dev/null 2>&1; then
        info "サービスモデルは既にインストール済みです"
    else
        command -v curl >/dev/null 2>&1 || die "curl がインストールされていません"

        local tmpfile
        tmpfile=$(mktemp /tmp/devopsagent-XXXXXX.json)
        trap "rm -f '$tmpfile'" EXIT

        info "サービスモデルをダウンロード中..."
        curl -sL "$SERVICE_MODEL_URL" -o "$tmpfile" \
            || die "サービスモデルのダウンロードに失敗しました"

        info "AWS CLI に追加中..."
        aws configure add-model \
            --service-model "file://${tmpfile}" \
            --service-name devopsagent \
            || die "サービスモデルの追加に失敗しました"

        info "サービスモデルをインストールしました"
    fi

    echo ""

    # 2. IAM ロール作成
    echo "2) IAM ロールのセットアップ"
    echo ""

    if ! confirm "DevOps Agent 用の IAM ロールを作成しますか？"; then
        info "IAM ロールのセットアップをスキップしました"
        return
    fi

    check_auth

    local role_name="DevOpsAgentRole-AgentSpace"

    # ロールが既に存在するか確認
    if aws_iam get-role --role-name "$role_name" >/dev/null 2>&1; then
        info "ロール ${role_name} は既に存在します"
    else
        info "ロール ${role_name} を作成中..."

        local trust_policy
        trust_policy=$(cat <<'POLICY'
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "aiops.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}
POLICY
)

        aws_iam create-role \
            --role-name "$role_name" \
            --assume-role-policy-document "$trust_policy" \
            >/dev/null \
            || die "ロールの作成に失敗しました"

        aws_iam attach-role-policy \
            --role-name "$role_name" \
            --policy-arn "arn:aws:iam::aws:policy/AIOpsAssistantPolicy" \
            || die "ポリシーのアタッチに失敗しました"

        info "ロール ${role_name} を作成しました"
    fi

    # Operator App 用ロール
    local operator_role_name="DevOpsAgentRole-WebappAdmin"

    if aws_iam get-role --role-name "$operator_role_name" >/dev/null 2>&1; then
        info "ロール ${operator_role_name} は既に存在します"
    else
        info "ロール ${operator_role_name} を作成中..."

        local operator_trust_policy
        operator_trust_policy=$(cat <<POLICY
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::${ACCOUNT_ID}:root"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}
POLICY
)

        local operator_inline_policy
        operator_inline_policy=$(cat <<'POLICY'
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "aiops:*"
            ],
            "Resource": "*"
        }
    ]
}
POLICY
)

        aws_iam create-role \
            --role-name "$operator_role_name" \
            --assume-role-policy-document "$operator_trust_policy" \
            >/dev/null \
            || die "Operator ロールの作成に失敗しました"

        aws_iam put-role-policy \
            --role-name "$operator_role_name" \
            --policy-name "AIDevOpsBasicOperatorActionsPolicy" \
            --policy-document "$operator_inline_policy" \
            || die "Operator ポリシーの追加に失敗しました"

        info "ロール ${operator_role_name} を作成しました"
    fi

    echo ""
    info "セットアップ完了"
    echo ""
    echo "次のステップ:"
    echo "  1. devops-agent create-space  で Agent Space を作成"
    echo "  2. devops-agent associate-aws で AWS アカウントを連携"
    echo "  3. devops-agent associate-gh  で GitHub リポジトリを連携"
}

# ---------------------------------------------------------------------------
# サブコマンド: spaces (一覧)
# ---------------------------------------------------------------------------

cmd_spaces() {
    header "Agent Space 一覧"
    check_auth

    local spaces_json
    spaces_json=$(aws_da list-agent-spaces 2>/dev/null) \
        || die "Agent Space の取得に失敗しました"

    local count
    count=$(echo "$spaces_json" | jq '.agentSpaces | length')

    if [[ "$count" -eq 0 ]]; then
        echo "Agent Space はまだありません。"
        echo "  devops-agent create-space で作成してください。"
        return
    fi

    echo "$spaces_json" | jq -r '
        .agentSpaces[] |
        "  ID:     " + .agentSpaceId + "\n" +
        "  名前:   " + .name + "\n" +
        "  状態:   " + (.status // "unknown") + "\n" +
        "  ---"
    '
}

# ---------------------------------------------------------------------------
# サブコマンド: create-space
# ---------------------------------------------------------------------------

cmd_create_space() {
    header "Agent Space 作成"
    check_auth

    local name description
    read -rp "Agent Space 名: " name
    [[ -n "$name" ]] || die "名前を入力してください"

    read -rp "説明 (任意): " description
    description="${description:-${name} の Agent Space}"

    info "Agent Space を作成中..."
    local result
    result=$(aws_da create-agent-space \
        --name "$name" \
        --description "$description" 2>/dev/null) \
        || die "Agent Space の作成に失敗しました"

    local space_id
    space_id=$(echo "$result" | jq -r '.agentSpaceId')

    info "作成完了"
    info "Agent Space ID: ${space_id}"

    # 設定ファイルに保存
    set_config ".lastAgentSpaceId" "$space_id"
    info "Agent Space ID を設定ファイルに保存しました"

    echo ""
    echo "次のステップ:"
    echo "  devops-agent associate-aws  で AWS アカウントを連携"
}

# ---------------------------------------------------------------------------
# サブコマンド: delete-space
# ---------------------------------------------------------------------------

cmd_delete_space() {
    header "Agent Space 削除"
    check_auth

    local space_id
    space_id=$(select_agent_space)

    echo ""
    if ! confirm "Agent Space ${space_id} を削除しますか？（元に戻せません）"; then
        info "キャンセルしました"
        return
    fi

    aws_da delete-agent-space --agent-space-id "$space_id" >/dev/null 2>&1 \
        || die "Agent Space の削除に失敗しました"

    info "Agent Space ${space_id} を削除しました"
}

# ---------------------------------------------------------------------------
# サブコマンド: associate-aws
# ---------------------------------------------------------------------------

cmd_associate_aws() {
    header "AWS アカウント連携"
    check_auth

    local space_id
    space_id=$(select_agent_space)
    info "Agent Space: ${space_id}"

    local role_arn
    role_arn="arn:aws:iam::${ACCOUNT_ID}:role/DevOpsAgentRole-AgentSpace"

    # カスタムロール名を確認
    echo ""
    info "デフォルトロール: ${role_arn}"
    if ! confirm "このロールを使用しますか？"; then
        read -rp "ロール ARN を入力: " role_arn
        [[ -n "$role_arn" ]] || die "ロール ARN を入力してください"
    fi

    local config_json
    config_json=$(cat <<JSON
{
    "aws": {
        "assumableRoleArn": "${role_arn}",
        "accountId": "${ACCOUNT_ID}",
        "accountType": "monitor",
        "resources": []
    }
}
JSON
)

    info "AWS アカウントを関連付け中..."
    aws_da associate-service \
        --agent-space-id "$space_id" \
        --service-id aws \
        --configuration "$config_json" \
        >/dev/null 2>&1 \
        || die "AWS アカウントの関連付けに失敗しました"

    info "AWS アカウント ${ACCOUNT_ID} を関連付けました"
    echo ""
    echo "CloudWatch のログ・メトリクス・X-Ray トレースが"
    echo "DevOps Agent から参照可能になりました。"
}

# ---------------------------------------------------------------------------
# サブコマンド: associate-gh
# ---------------------------------------------------------------------------

cmd_associate_gh() {
    header "GitHub リポジトリ連携"
    check_auth

    local space_id
    space_id=$(select_agent_space)
    info "Agent Space: ${space_id}"

    # GitHub が OAuth 登録済みか確認
    echo ""
    echo "注意: GitHub 連携には、事前に DevOps Agent コンソールで"
    echo "      OAuth 認証を完了している必要があります。"
    echo ""
    echo "  コンソール: https://us-east-1.console.aws.amazon.com/devopsagent/"
    echo ""

    if ! confirm "OAuth 認証は完了していますか？"; then
        info "先に AWS コンソールで GitHub の OAuth 認証を完了してください"
        return
    fi

    # 登録済みサービスから GitHub を取得
    local services_json
    services_json=$(aws_da list-services 2>/dev/null) || true

    local github_service_id
    github_service_id=$(echo "$services_json" | jq -r '
        .services[]? | select(.service == "github" or .serviceId == "github") |
        .serviceId // .service // empty
    ' 2>/dev/null | head -1)

    if [[ -z "$github_service_id" ]]; then
        # GitHub のサービス ID がリストにない場合は "github" をデフォルトで使用
        github_service_id="github"
    fi

    # リポジトリ情報の入力
    local owner repo_name
    read -rp "GitHub オーナー (org or user): " owner
    [[ -n "$owner" ]] || die "オーナーを入力してください"

    read -rp "リポジトリ名: " repo_name
    [[ -n "$repo_name" ]] || die "リポジトリ名を入力してください"

    local owner_type
    owner_type=$(pick_one "オーナータイプ" "organization" "user") \
        || die "オーナータイプが選択されませんでした"

    # リポジトリ ID の取得を試みる
    local repo_id=""
    if command -v gh >/dev/null 2>&1; then
        repo_id=$(gh api "repos/${owner}/${repo_name}" --jq '.id' 2>/dev/null) || true
    fi

    if [[ -z "$repo_id" ]]; then
        read -rp "GitHub リポジトリ ID (不明なら空Enter): " repo_id
    fi

    local config_json
    if [[ -n "$repo_id" ]]; then
        config_json=$(cat <<JSON
{
    "github": {
        "repoName": "${repo_name}",
        "repoId": "${repo_id}",
        "owner": "${owner}",
        "ownerType": "${owner_type}"
    }
}
JSON
)
    else
        config_json=$(cat <<JSON
{
    "github": {
        "repoName": "${repo_name}",
        "owner": "${owner}",
        "ownerType": "${owner_type}"
    }
}
JSON
)
    fi

    info "GitHub リポジトリを関連付け中..."
    aws_da associate-service \
        --agent-space-id "$space_id" \
        --service-id "$github_service_id" \
        --configuration "$config_json" \
        >/dev/null 2>&1 \
        || die "GitHub リポジトリの関連付けに失敗しました"

    info "${owner}/${repo_name} を関連付けました"
}

# ---------------------------------------------------------------------------
# サブコマンド: associations (一覧)
# ---------------------------------------------------------------------------

cmd_associations() {
    header "関連付け一覧"
    check_auth

    local space_id
    space_id=$(select_agent_space)
    info "Agent Space: ${space_id}"
    echo ""

    local assoc_json
    assoc_json=$(aws_da list-associations --agent-space-id "$space_id" 2>/dev/null) \
        || die "関連付けの取得に失敗しました"

    local count
    count=$(echo "$assoc_json" | jq '.associations | length')

    if [[ "$count" -eq 0 ]]; then
        echo "関連付けはまだありません。"
        return
    fi

    echo "$assoc_json" | jq -r '
        .associations[]? |
        "  サービス: " + (.serviceId // "unknown") + "\n" +
        "  状態:     " + (.status // "unknown") + "\n" +
        "  ---"
    '
}

# ---------------------------------------------------------------------------
# サブコマンド: status
# ---------------------------------------------------------------------------

cmd_status() {
    header "Agent Space 状態"
    check_auth

    local space_id
    space_id=$(select_agent_space)

    echo ""
    info "Agent Space 詳細:"
    local space_json
    space_json=$(aws_da get-agent-space --agent-space-id "$space_id" 2>/dev/null) \
        || die "Agent Space の取得に失敗しました"

    echo "$space_json" | jq -r '
        "  ID:     " + .agentSpaceId +
        "\n  名前:   " + .name +
        "\n  状態:   " + (.status // "unknown") +
        "\n  説明:   " + (.description // "-")
    '

    echo ""
    info "関連付け:"
    local assoc_json
    assoc_json=$(aws_da list-associations --agent-space-id "$space_id" 2>/dev/null) || true

    local count
    count=$(echo "$assoc_json" | jq '.associations | length' 2>/dev/null || echo "0")

    if [[ "$count" -eq 0 ]]; then
        echo "  (なし)"
    else
        echo "$assoc_json" | jq -r '
            .associations[]? |
            "  - " + (.serviceId // "unknown") + " [" + (.status // "unknown") + "]"
        '
    fi
}

# ---------------------------------------------------------------------------
# サブコマンド: help
# ---------------------------------------------------------------------------

cmd_help() {
    cat <<'HELP'
AWS DevOps Agent CLI ラッパー

Usage: devops-agent [サブコマンド]

サブコマンド:
  setup          初期セットアップ (サービスモデル・IAM ロール)
  spaces         Agent Space の一覧表示
  create-space   Agent Space の作成
  delete-space   Agent Space の削除
  associate-aws  AWS アカウントの関連付け
  associate-gh   GitHub リポジトリの関連付け
  associations   関連付けの一覧表示
  status         Agent Space の詳細・関連付け状況
  help           このヘルプを表示

環境変数:
  AWS_PROFILE    使用する AWS プロファイル

設定ファイル:
  ~/.config/devops-agent/config.json

初回利用の流れ:
  1. devops-agent setup          # サービスモデル・IAM ロール作成
  2. devops-agent create-space   # Agent Space を作成
  3. devops-agent associate-aws  # AWS アカウントを連携
  4. devops-agent associate-gh   # GitHub リポジトリを連携
  5. AWS コンソールで Operator App を有効化 → 調査開始
HELP
}

# ---------------------------------------------------------------------------
# メイン
# ---------------------------------------------------------------------------

main() {
    command -v jq >/dev/null 2>&1 || die "jq がインストールされていません"
    command -v aws >/dev/null 2>&1 || die "AWS CLI がインストールされていません"

    init_config

    # AWS_PROFILE の設定（環境変数がなければ空）
    AWS_PROFILE="${AWS_PROFILE:-}"
    ACCOUNT_ID=""

    local subcmd="${1:-}"

    if [[ -z "$subcmd" ]]; then
        # 対話的にサブコマンドを選択
        subcmd=$(pick_one "操作" \
            "setup          初期セットアップ" \
            "spaces         Agent Space 一覧" \
            "create-space   Agent Space 作成" \
            "delete-space   Agent Space 削除" \
            "associate-aws  AWS アカウント連携" \
            "associate-gh   GitHub リポジトリ連携" \
            "associations   関連付け一覧" \
            "status         状態確認") \
            || die "操作が選択されませんでした"
        subcmd=$(echo "$subcmd" | awk '{print $1}')
    fi

    case "$subcmd" in
        setup)         cmd_setup ;;
        spaces)        cmd_spaces ;;
        create-space)  cmd_create_space ;;
        delete-space)  cmd_delete_space ;;
        associate-aws) cmd_associate_aws ;;
        associate-gh)  cmd_associate_gh ;;
        associations)  cmd_associations ;;
        status)        cmd_status ;;
        help|--help|-h) cmd_help ;;
        *)             die "不明なサブコマンド: ${subcmd}\n  devops-agent help でヘルプを表示" ;;
    esac
}

main "$@"
