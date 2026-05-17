#!/bin/bash
#
# ecs-exec.sh - ECS Exec の対話型ラッパー
#
# クラスタ → サービス → タスク → コンテナ → シェル を
# 対話的に選択して ECS Exec を実行する。
#
# Usage:
#   ./ecs-exec.sh [aws-profile]
#
# 必要なもの:
#   - AWS CLI v2 (aws ecs execute-command 対応)
#   - jq
#   - fzf (任意: インストール済みなら自動で使用)
#

set -euo pipefail

# ---------------------------------------------------------------------------
# ユーティリティ
# ---------------------------------------------------------------------------

die() { echo "エラー: $1" >&2; exit 1; }

# fzf が使えればfzf、なければ番号選択にフォールバック
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

aws_ecs() {
    aws ecs "$@" ${PROFILE:+--profile "$PROFILE"} --output json
}

aws_ec2() {
    aws ec2 "$@" ${PROFILE:+--profile "$PROFILE"} --output json
}

open_url() {
    local url="$1"
    if command -v wslview >/dev/null 2>&1; then
        wslview "$url" >/dev/null 2>&1 || true
    elif command -v xdg-open >/dev/null 2>&1; then
        xdg-open "$url" >/dev/null 2>&1 || true
    elif command -v open >/dev/null 2>&1; then
        open "$url" >/dev/null 2>&1 || true
    fi
}

# ---------------------------------------------------------------------------
# CloudShell VPC フォールバック
# ---------------------------------------------------------------------------
#
# サービスで ECS Exec が無効化されている場合、同一ネットワーク環境の
# CloudShell VPC 環境を作成するために必要な情報を表示する。
# CloudShell はコンソール専用機能で API 起動に対応していないため、
# 値のみ提示してマネコン側で手動作成させる方針。
#
launch_cloudshell_vpc() {
    local cluster="$1" service="$2"

    local tasks_json tasks
    tasks_json=$(aws_ecs list-tasks --cluster "$cluster" --service-name "$service" --desired-status RUNNING)
    mapfile -t tasks < <(
        echo "$tasks_json" | jq -r '.taskArns[] | split("/")[-1]'
    )
    [[ ${#tasks[@]} -gt 0 ]] || die "実行中のタスクがありません (service: ${service})"

    local task
    task=$(pick_one "ネットワーク情報を参照するタスク" "${tasks[@]}") \
        || die "タスクが選択されませんでした"
    echo "-- 参照タスク: ${task}"

    local describe_json subnet_id eni_id
    describe_json=$(aws_ecs describe-tasks --cluster "$cluster" --tasks "$task")
    subnet_id=$(echo "$describe_json" \
        | jq -r '.tasks[0].attachments[]?.details[]? | select(.name=="subnetId") | .value' | head -n1)
    eni_id=$(echo "$describe_json" \
        | jq -r '.tasks[0].attachments[]?.details[]? | select(.name=="networkInterfaceId") | .value' | head -n1)

    [[ -n "$subnet_id" && "$subnet_id" != "null" ]] \
        || die "サブネットIDを取得できませんでした (awsvpc ネットワークモードのタスクのみ対応)"
    [[ -n "$eni_id" && "$eni_id" != "null" ]] \
        || die "ENI IDを取得できませんでした"

    local eni_json vpc_id az region
    eni_json=$(aws_ec2 describe-network-interfaces --network-interface-ids "$eni_id")
    vpc_id=$(echo "$eni_json" | jq -r '.NetworkInterfaces[0].VpcId')
    az=$(echo "$eni_json" | jq -r '.NetworkInterfaces[0].AvailabilityZone')
    region="${az%?}"  # 末尾の a/b/c を削って region を抽出

    local sg_ids
    mapfile -t sg_ids < <(
        echo "$eni_json" | jq -r '.NetworkInterfaces[0].Groups[].GroupId'
    )

    local url="https://${region}.console.aws.amazon.com/cloudshell/home?region=${region}"

    echo ""
    echo "=== CloudShell VPC 環境作成に必要な情報 ==========="
    echo ""
    printf "  リージョン        : %s\n" "$region"
    printf "  VPC ID            : %s\n" "$vpc_id"
    printf "  サブネット ID     : %s\n" "$subnet_id"
    printf "  セキュリティグループ: %s\n" "${sg_ids[*]}"
    echo ""
    echo "  コンソール URL    :"
    echo "    ${url}"
    echo ""
    echo "  手順:"
    echo "    1. 上記 URL を開く (自動でブラウザが開かれます)"
    echo "    2. 左上 [+] > [Create VPC environment] を選択"
    echo "    3. 上記 VPC / サブネット / セキュリティグループを設定"
    echo "    4. [Create] で起動"
    echo ""
    echo "  注意:"
    echo "    - VPC CloudShell からはインターネットおよび AWS API への"
    echo "      直接アクセス不可。NAT GW もしくは VPC エンドポイントが必要。"
    echo "    - セッション上限は最長 12 時間 / アイドル 20〜30 分で切断。"
    echo "==================================================="
    echo ""

    # クリップボードにコピー (WSL / Mac / Linux)
    if command -v clip.exe >/dev/null 2>&1; then
        echo -n "$url" | clip.exe && echo "-- URL をクリップボードにコピーしました"
    elif command -v pbcopy >/dev/null 2>&1; then
        echo -n "$url" | pbcopy && echo "-- URL をクリップボードにコピーしました"
    elif command -v xclip >/dev/null 2>&1; then
        echo -n "$url" | xclip -selection clipboard && echo "-- URL をクリップボードにコピーしました"
    fi

    open_url "$url"
}

# ---------------------------------------------------------------------------
# メイン
# ---------------------------------------------------------------------------

main() {
    PROFILE="${1:-}"

    # プロファイルの疎通確認 & 利用中の identity を表示
    local identity
    identity=$(aws sts get-caller-identity ${PROFILE:+--profile "$PROFILE"} --output json 2>/dev/null) \
        || die "AWS 認証に失敗しました${PROFILE:+ (profile: ${PROFILE})}"

    local account arn
    account=$(echo "$identity" | jq -r .Account)
    arn=$(echo "$identity" | jq -r .Arn)
    echo "-- AWS アカウント: ${account}"
    echo "-- IAM: ${arn}"
    [[ -n "$PROFILE" ]] && echo "-- プロファイル: ${PROFILE}" || echo "-- プロファイル: (デフォルト)"
    echo ""

    # --- クラスタ選択 ---
    local clusters_json clusters
    clusters_json=$(aws_ecs list-clusters)
    mapfile -t clusters < <(
        echo "$clusters_json" | jq -r '.clusterArns[] | split("/")[-1]'
    )
    [[ ${#clusters[@]} -gt 0 ]] || die "クラスタが見つかりません"

    local cluster
    cluster=$(pick_one "クラスタ" "${clusters[@]}") || die "クラスタが選択されませんでした"
    echo "-- クラスタ: ${cluster}"

    # --- サービス選択 ---
    local services_json services
    services_json=$(aws_ecs list-services --cluster "$cluster")
    mapfile -t services < <(
        echo "$services_json" | jq -r '.serviceArns[] | split("/")[-1]'
    )
    [[ ${#services[@]} -gt 0 ]] || die "サービスが見つかりません (cluster: ${cluster})"

    local service
    service=$(pick_one "サービス" "${services[@]}") || die "サービスが選択されませんでした"
    echo "-- サービス: ${service}"

    # --- exec 有効チェック & CloudShell VPC 分岐 ---
    local exec_enabled
    exec_enabled=$(aws_ecs describe-services --cluster "$cluster" --services "$service" \
        | jq -r '.services[0].enableExecuteCommand // false')

    if [[ "$exec_enabled" != "true" ]]; then
        echo ""
        echo "!! このサービスは ECS Exec が無効化されています (enableExecuteCommand=false)"
        echo ""
        local action
        action=$(pick_one "代替手段を選択" \
            "CloudShell VPC でネットワーク調査" \
            "このまま ECS Exec を試す (失敗する可能性あり)" \
            "中止") \
            || die "選択されませんでした"

        case "$action" in
            "CloudShell VPC"*)
                launch_cloudshell_vpc "$cluster" "$service"
                return
                ;;
            "中止")
                echo "中止しました"
                exit 0
                ;;
        esac
    fi

    # --- タスク選択 ---
    local tasks_json tasks
    tasks_json=$(aws_ecs list-tasks --cluster "$cluster" --service-name "$service" --desired-status RUNNING)
    mapfile -t tasks < <(
        echo "$tasks_json" | jq -r '.taskArns[] | split("/")[-1]'
    )
    [[ ${#tasks[@]} -gt 0 ]] || die "実行中のタスクがありません (service: ${service})"

    local task
    task=$(pick_one "タスク" "${tasks[@]}") || die "タスクが選択されませんでした"
    echo "-- タスク: ${task}"

    # --- コンテナ選択 ---
    local describe_json containers
    describe_json=$(aws_ecs describe-tasks --cluster "$cluster" --tasks "$task")
    mapfile -t containers < <(
        echo "$describe_json" | jq -r '.tasks[0].containers[].name'
    )
    [[ ${#containers[@]} -gt 0 ]] || die "コンテナが見つかりません (task: ${task})"

    local container
    container=$(pick_one "コンテナ" "${containers[@]}") || die "コンテナが選択されませんでした"
    echo "-- コンテナ: ${container}"

    # --- 実行 ---
    # デフォルトは sh (bash 非搭載コンテナでの接続失敗タイムアウト回避)。
    # bash を使いたい場合は ECS_EXEC_SHELL=bash を指定。
    local shell_cmd="${ECS_EXEC_SHELL:-sh}"

    local cmd_display="aws ecs execute-command${PROFILE:+ --profile ${PROFILE}} --cluster ${cluster} --task ${task} --container ${container} --interactive --command ${shell_cmd}"
    echo ""
    echo "=== コマンド =================================="
    echo ""
    echo "${cmd_display}"
    echo ""
    echo "=============================================="
    echo ""

    aws ecs execute-command \
        ${PROFILE:+--profile "$PROFILE"} \
        --cluster "$cluster" \
        --task "$task" \
        --container "$container" \
        --interactive \
        --command "$shell_cmd"
}

main "$@"
