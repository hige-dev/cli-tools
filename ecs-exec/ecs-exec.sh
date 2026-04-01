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

    # --- シェル選択 ---
    local shell
    shell=$(pick_one "シェル" "bash" "sh") || die "シェルが選択されませんでした"

    # --- 実行 ---
    local cmd="aws ecs execute-command${PROFILE:+ --profile ${PROFILE}} --cluster ${cluster} --task ${task} --container ${container} --interactive --command ${shell}"
    echo ""
    echo "=== コマンド =================================="
    echo ""
    echo "${cmd}"
    echo ""
    echo "=============================================="
    echo ""

    aws ecs execute-command \
        ${PROFILE:+--profile "$PROFILE"} \
        --cluster "$cluster" \
        --task "$task" \
        --container "$container" \
        --interactive \
        --command "$shell"
}

main "$@"
