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
# 一時デバッグタスク フォールバック
# ---------------------------------------------------------------------------
#
# 既存サービスのアプリケーションコンテナを触らず、同じネットワーク設定で
# デバッグ用コンテナを --enable-execute-command 付きで run-task 起動する。
# IAM ロールとネットワーク設定のみ既存サービスから流用 (DB 接続情報などの
# アプリ設定は引き継がない)。セッション終了時に stop-task で自動停止。
#
# デフォルト: debian:stable-slim + 起動時に apt install で基本ネットワークツール
# 上書き: ECS_DEBUG_TASK_IMAGE=nicolaka/netshoot:v0.14 など
#         (カスタムイメージ指定時は install 処理はスキップ)
#
# 前提: 流用するタスクロールに SSM メッセージング権限
#       (ssmmessages:CreateControlChannel 等) があること。
# 上書きしたい場合は環境変数 ECS_DEBUG_TASK_ROLE_ARN を設定。
#
DEBUG_TASK_FAMILY="ecs-exec-debug"
DEBUG_TASK_CONTAINER="debug"
DEBUG_DEFAULT_IMAGE="public.ecr.aws/docker/library/debian:stable-slim"
DEBUG_DEFAULT_PACKAGES="dnsutils curl netcat-openbsd tcpdump traceroute iputils-ping iproute2"
# trap が失敗した場合の保険として、コンテナ自体に TTL を持たせる (秒)
DEBUG_TASK_TTL_SEC="${ECS_DEBUG_TASK_TTL_SEC:-14400}"  # デフォルト 4 時間

launch_debug_task() {
    local cluster="$1" service="$2"

    local svc_json
    svc_json=$(aws_ecs describe-services --cluster "$cluster" --services "$service")

    local svc_td_arn launch_type network_config cps_len
    svc_td_arn=$(echo "$svc_json" | jq -r '.services[0].taskDefinition')
    launch_type=$(echo "$svc_json" | jq -r '.services[0].launchType // "FARGATE"')
    network_config=$(echo "$svc_json" | jq -c '.services[0].networkConfiguration')
    cps_len=$(echo "$svc_json" | jq '.services[0].capacityProviderStrategy | length')

    [[ "$network_config" != "null" ]] \
        || die "サービスのネットワーク設定を取得できませんでした"

    # 既存サービスのロールを流用
    local td_json exec_role task_role
    td_json=$(aws_ecs describe-task-definition --task-definition "$svc_td_arn")
    exec_role=$(echo "$td_json" | jq -r '.taskDefinition.executionRoleArn // empty')
    task_role="${ECS_DEBUG_TASK_ROLE_ARN:-$(echo "$td_json" | jq -r '.taskDefinition.taskRoleArn // empty')}"

    [[ -n "$exec_role" ]] || die "実行ロール (executionRoleArn) が取得できませんでした"
    [[ -n "$task_role" ]] \
        || die "タスクロールが取得できませんでした。ECS_DEBUG_TASK_ROLE_ARN で SSM 権限のあるロールを指定してください"

    # コンテナ定義を組み立て
    # - カスタムイメージ指定時: そのまま sleep で起動 (ツールは事前installされている前提)
    # - デフォルト: debian-slim + 起動時 apt install → healthCheck で完了検知
    local custom_image="${ECS_DEBUG_TASK_IMAGE:-}"
    local container_def image_label
    if [[ -n "$custom_image" ]]; then
        image_label="$custom_image (カスタム)"
        container_def=$(jq -n \
            --arg name "$DEBUG_TASK_CONTAINER" \
            --arg image "$custom_image" \
            --arg ttl "$DEBUG_TASK_TTL_SEC" \
            '{
                name: $name,
                image: $image,
                command: ["sleep", $ttl],
                essential: true,
                linuxParameters: { initProcessEnabled: true }
            }')
    else
        image_label="${DEBUG_DEFAULT_IMAGE} + apt install ${DEBUG_DEFAULT_PACKAGES}"
        local install_cmd="apt-get update -q && DEBIAN_FRONTEND=noninteractive apt-get install -y -q --no-install-recommends ${DEBUG_DEFAULT_PACKAGES} >/dev/null && exec sleep ${DEBUG_TASK_TTL_SEC}"
        container_def=$(jq -n \
            --arg name "$DEBUG_TASK_CONTAINER" \
            --arg image "$DEBUG_DEFAULT_IMAGE" \
            --arg cmd "$install_cmd" \
            '{
                name: $name,
                image: $image,
                command: ["bash", "-c", $cmd],
                essential: true,
                linuxParameters: { initProcessEnabled: true },
                healthCheck: {
                    command: ["CMD-SHELL", "command -v dig >/dev/null && command -v tcpdump >/dev/null"],
                    interval: 10,
                    timeout: 5,
                    retries: 30,
                    startPeriod: 30
                }
            }')
    fi

    echo ""
    echo "-- デバッグ用タスク定義を登録します"
    echo "   family: ${DEBUG_TASK_FAMILY}"
    echo "   image : ${image_label}"

    local td_input new_td_arn
    td_input=$(jq -n \
        --arg family "$DEBUG_TASK_FAMILY" \
        --arg exec_role "$exec_role" \
        --arg task_role "$task_role" \
        --argjson container "$container_def" \
        '{
            family: $family,
            networkMode: "awsvpc",
            requiresCompatibilities: ["FARGATE"],
            cpu: "256",
            memory: "512",
            executionRoleArn: $exec_role,
            taskRoleArn: $task_role,
            containerDefinitions: [$container]
        }')

    new_td_arn=$(aws ecs register-task-definition \
        ${PROFILE:+--profile "$PROFILE"} \
        --cli-input-json "$td_input" \
        --output json \
        | jq -r '.taskDefinition.taskDefinitionArn')
    [[ -n "$new_td_arn" && "$new_td_arn" != "null" ]] || die "タスク定義の登録に失敗しました"
    echo "-- 登録: ${new_td_arn##*/}"

    # run-task 入力を JSON で組み立て (capacityProvider / launchType 両対応)
    local run_input
    run_input=$(jq -n \
        --arg cluster "$cluster" \
        --arg td "$new_td_arn" \
        --argjson network "$network_config" \
        '{
            cluster: $cluster,
            taskDefinition: $td,
            networkConfiguration: $network,
            enableExecuteCommand: true,
            count: 1,
            propagateTags: "TASK_DEFINITION"
        }')
    if [[ "$cps_len" -gt 0 ]]; then
        local cps_json
        cps_json=$(echo "$svc_json" | jq -c '.services[0].capacityProviderStrategy')
        run_input=$(echo "$run_input" | jq --argjson cps "$cps_json" '. + {capacityProviderStrategy: $cps}')
    else
        run_input=$(echo "$run_input" | jq --arg lt "$launch_type" '. + {launchType: $lt}')
    fi

    echo "-- デバッグタスクを起動..."
    local run_json task_arn task_id
    run_json=$(aws ecs run-task \
        ${PROFILE:+--profile "$PROFILE"} \
        --cli-input-json "$run_input" \
        --output json)
    task_arn=$(echo "$run_json" | jq -r '.tasks[0].taskArn // empty')
    if [[ -z "$task_arn" ]]; then
        echo "$run_json" | jq -r '.failures' >&2
        die "タスク起動に失敗しました"
    fi
    task_id="${task_arn##*/}"
    echo "-- 起動: ${task_id}"

    # 終了時にタスクを自動停止
    local DEBUG_TASK_CLEANED=0
    cleanup_debug_task() {
        [[ $DEBUG_TASK_CLEANED -eq 1 ]] && return
        DEBUG_TASK_CLEANED=1
        echo ""
        echo "-- デバッグタスクを停止 (${task_id})"
        aws ecs stop-task \
            ${PROFILE:+--profile "$PROFILE"} \
            --cluster "$cluster" \
            --task "$task_id" \
            --reason "ecs_exec debug session ended" >/dev/null 2>&1 || true
    }
    trap cleanup_debug_task EXIT INT TERM HUP

    echo "-- RUNNING 待機 (最大 5 分)..."
    aws ecs wait tasks-running \
        ${PROFILE:+--profile "$PROFILE"} \
        --cluster "$cluster" \
        --tasks "$task_id" \
        || die "タスクが RUNNING になりませんでした"

    # デフォルトイメージ時は install 完了 (HEALTHY) を待つ
    if [[ -z "$custom_image" ]]; then
        echo -n "-- ツールのインストール完了を待機 "
        local attempts=0 health_status="UNKNOWN"
        while [[ "$health_status" != "HEALTHY" ]] && (( attempts < 60 )); do
            sleep 5
            health_status=$(aws_ecs describe-tasks --cluster "$cluster" --tasks "$task_id" \
                | jq -r '.tasks[0].healthStatus // "UNKNOWN"')
            printf "."
            ((attempts++))
        done
        echo ""
        if [[ "$health_status" != "HEALTHY" ]]; then
            echo "!! HEALTHY になりませんでした (status=${health_status})。接続は試みますがツール未インストールの可能性あり"
        fi
    fi

    echo ""
    echo "=== デバッグシェル ===================================="
    echo "  利用可能 (デフォルト): dig, nslookup, curl, nc, tcpdump, traceroute, ping"
    echo "  exit でセッション終了 → タスクは自動停止されます"
    echo "======================================================="
    echo ""

    aws ecs execute-command \
        ${PROFILE:+--profile "$PROFILE"} \
        --cluster "$cluster" \
        --task "$task_id" \
        --container "$DEBUG_TASK_CONTAINER" \
        --interactive \
        --command bash
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
            "一時デバッグタスクを起動" \
            "CloudShell VPC でネットワーク調査" \
            "中止") \
            || die "選択されませんでした"

        case "$action" in
            "一時デバッグタスク"*)
                launch_debug_task "$cluster" "$service"
                return
                ;;
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
