#!/bin/bash
#
# db-connect.sh - SSM 経由の DB 接続ヘルパー
#
# 踏み台インスタンスへのSSMログイン、または
# SSM ポートフォワードによる RDS トンネルを対話的に確立する。
#
# Usage:
#   ./db-connect.sh [config-profile]
#
# 設定ファイル:
#   ~/.config/db-connect/config.json
#
# 必要なもの:
#   - AWS CLI v2 (session-manager-plugin 導入済み)
#   - jq
#   - fzf (任意: インストール済みなら自動で使用)
#

set -euo pipefail

CONFIG_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/db-connect"
CONFIG_FILE="${CONFIG_DIR}/config.json"

# ---------------------------------------------------------------------------
# ユーティリティ
# ---------------------------------------------------------------------------

die() { echo "エラー: $1" >&2; exit 1; }

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

aws_cmd() {
    aws "$@" ${AWS_PROFILE:+--profile "$AWS_PROFILE"} --output json
}

# ---------------------------------------------------------------------------
# 設定ファイル
# ---------------------------------------------------------------------------

init_config() {
    if [[ ! -f "$CONFIG_FILE" ]]; then
        echo "設定ファイルが見つかりません: ${CONFIG_FILE}"
        echo "サンプルを作成しますか？ [Y/n]"
        local answer
        read -r answer
        if [[ "$answer" =~ ^[Nn] ]]; then
            die "設定ファイルを作成してから再実行してください"
        fi
        mkdir -p "$CONFIG_DIR"
        cat > "$CONFIG_FILE" << 'SAMPLE'
{
  "profiles": {
    "dev": {
      "aws_profile": "dev",
      "bastion_instance_id": "i-xxxxxxxxxxxxxxxxx",
      "default_rds": "mydb.cluster-xxxx.ap-northeast-1.rds.amazonaws.com",
      "default_engine": "mysql",
      "default_local_port": 13306,
      "description": "開発環境"
    },
    "stg": {
      "aws_profile": "stg",
      "bastion_instance_id": "i-yyyyyyyyyyyyyyyyy",
      "default_rds": "",
      "default_engine": "postgresql",
      "default_local_port": 15432,
      "description": "ステージング環境"
    }
  }
}
SAMPLE
        echo ""
        echo "サンプル設定を作成しました: ${CONFIG_FILE}"
        echo "環境に合わせて編集してから再実行してください。"
        exit 0
    fi

    jq empty "$CONFIG_FILE" 2>/dev/null || die "設定ファイルの JSON が不正です: ${CONFIG_FILE}"
}

get_config() {
    local profile="$1" key="$2"
    jq -r ".profiles[\"${profile}\"].${key} // empty" "$CONFIG_FILE"
}

list_profiles() {
    jq -r '.profiles | to_entries[] | "\(.key)\t\(.value.description // "")"' "$CONFIG_FILE"
}

# ---------------------------------------------------------------------------
# 踏み台選択
# ---------------------------------------------------------------------------

select_bastion() {
    local default_bastion="$1"

    if [[ -n "$default_bastion" ]]; then
        echo "踏み台インスタンス: ${default_bastion} (設定ファイルのデフォルト)" >&2
        echo "このまま使用しますか？ [Y/n]" >&2
        local answer
        read -r answer
        if [[ ! "$answer" =~ ^[Nn] ]]; then
            echo "$default_bastion"
            return
        fi
    fi

    echo "踏み台インスタンスを取得中..." >&2
    local instances_json
    instances_json=$(aws_cmd ec2 describe-instances \
        --filters "Name=instance-state-name,Values=running" \
        --query 'Reservations[].Instances[]' 2>/dev/null) \
        || die "EC2 インスタンスの取得に失敗しました"

    local items
    mapfile -t items < <(
        echo "$instances_json" | jq -r '
            .[] |
            (.InstanceId) + "\t" +
            ((.Tags // [] | map(select(.Key == "Name")) | first // {}).Value // "(名前なし)")
        ' | sort -t$'\t' -k2
    )

    [[ ${#items[@]} -gt 0 ]] || die "実行中のインスタンスが見つかりません"

    local display=()
    for item in "${items[@]}"; do
        local id name
        id=$(echo "$item" | cut -f1)
        name=$(echo "$item" | cut -f2)
        display+=("${id}  ${name}")
    done

    local selected
    selected=$(pick_one "踏み台インスタンス" "${display[@]}") || die "インスタンスが選択されませんでした"
    echo "$selected" | awk '{print $1}'
}

# ---------------------------------------------------------------------------
# RDS 選択
# ---------------------------------------------------------------------------

select_rds() {
    local default_rds="$1" engine="$2"

    if [[ -n "$default_rds" ]]; then
        echo "RDS エンドポイント: ${default_rds} (設定ファイルのデフォルト)" >&2
        echo "このまま使用しますか？ [Y/n]" >&2
        local answer
        read -r answer
        if [[ ! "$answer" =~ ^[Nn] ]]; then
            echo "$default_rds"
            return
        fi
    fi

    echo "RDS インスタンスを取得中..." >&2
    local rds_json
    rds_json=$(aws_cmd rds describe-db-instances 2>/dev/null) \
        || die "RDS インスタンスの取得に失敗しました"

    local items
    mapfile -t items < <(
        echo "$rds_json" | jq -r '
            .DBInstances[] |
            (.Endpoint.Address // empty) + "\t" +
            .DBInstanceIdentifier + "\t" +
            .Engine + "\t" +
            (.Endpoint.Port | tostring)
        '
    )

    # Aurora クラスタエンドポイントも取得
    local cluster_json
    cluster_json=$(aws_cmd rds describe-db-clusters 2>/dev/null) || true
    if [[ -n "$cluster_json" ]]; then
        local cluster_items
        mapfile -t cluster_items < <(
            echo "$cluster_json" | jq -r '
                .DBClusters[] |
                (.Endpoint // empty) + "\t" +
                .DBClusterIdentifier + " (writer)\t" +
                .Engine + "\t" +
                (.Port | tostring)
            ' 2>/dev/null
        ) || true
        items+=("${cluster_items[@]}")
    fi

    [[ ${#items[@]} -gt 0 ]] || die "RDS インスタンスが見つかりません"

    local display=()
    for item in "${items[@]}"; do
        [[ -z "$item" ]] && continue
        local endpoint ident eng port
        endpoint=$(echo "$item" | cut -f1)
        ident=$(echo "$item" | cut -f2)
        eng=$(echo "$item" | cut -f3)
        port=$(echo "$item" | cut -f4)
        display+=("${endpoint}  [${ident}] (${eng}:${port})")
    done

    [[ ${#display[@]} -gt 0 ]] || die "RDS エンドポイントが見つかりません"

    local selected
    selected=$(pick_one "RDS" "${display[@]}") || die "RDS が選択されませんでした"
    echo "$selected" | awk '{print $1}'
}

detect_engine_port() {
    local rds_endpoint="$1"

    local rds_json
    rds_json=$(aws_cmd rds describe-db-instances 2>/dev/null) || true

    local info
    info=$(echo "$rds_json" | jq -r --arg ep "$rds_endpoint" '
        .DBInstances[] | select(.Endpoint.Address == $ep) |
        .Engine + "\t" + (.Endpoint.Port | tostring)
    ' 2>/dev/null) || true

    # クラスタエンドポイントの場合
    if [[ -z "$info" ]]; then
        local cluster_json
        cluster_json=$(aws_cmd rds describe-db-clusters 2>/dev/null) || true
        info=$(echo "$cluster_json" | jq -r --arg ep "$rds_endpoint" '
            .DBClusters[] | select(.Endpoint == $ep) |
            .Engine + "\t" + (.Port | tostring)
        ' 2>/dev/null) || true
    fi

    echo "$info"
}

# ---------------------------------------------------------------------------
# メイン
# ---------------------------------------------------------------------------

main() {
    command -v jq >/dev/null 2>&1 || die "jq がインストールされていません"
    command -v aws >/dev/null 2>&1 || die "AWS CLI がインストールされていません"

    init_config

    # --- プロファイル選択 ---
    local config_profile="${1:-}"
    if [[ -z "$config_profile" ]]; then
        local profile_names
        mapfile -t profile_names < <(jq -r '.profiles | keys[]' "$CONFIG_FILE")
        [[ ${#profile_names[@]} -gt 0 ]] || die "設定ファイルにプロファイルが定義されていません"

        local profile_display=()
        for p in "${profile_names[@]}"; do
            local desc
            desc=$(get_config "$p" "description")
            if [[ -n "$desc" ]]; then
                profile_display+=("${p}  (${desc})")
            else
                profile_display+=("${p}")
            fi
        done

        local selected
        selected=$(pick_one "接続プロファイル" "${profile_display[@]}") \
            || die "プロファイルが選択されませんでした"
        config_profile=$(echo "$selected" | awk '{print $1}')
    fi

    # プロファイルの存在確認
    jq -e ".profiles[\"${config_profile}\"]" "$CONFIG_FILE" >/dev/null 2>&1 \
        || die "プロファイルが見つかりません: ${config_profile}"

    # 設定値の読み込み
    AWS_PROFILE=$(get_config "$config_profile" "aws_profile")
    local default_bastion default_rds default_engine default_local_port
    default_bastion=$(get_config "$config_profile" "bastion_instance_id")
    default_rds=$(get_config "$config_profile" "default_rds")
    default_engine=$(get_config "$config_profile" "default_engine")
    default_local_port=$(get_config "$config_profile" "default_local_port")

    echo "=== DB Connect ==="
    echo "-- プロファイル: ${config_profile}"
    echo "-- AWS プロファイル: ${AWS_PROFILE:-(デフォルト)}"
    echo ""

    # AWS 認証確認
    local identity
    identity=$(aws sts get-caller-identity ${AWS_PROFILE:+--profile "$AWS_PROFILE"} --output json 2>/dev/null) \
        || die "AWS 認証に失敗しました${AWS_PROFILE:+ (profile: ${AWS_PROFILE})}"

    local account arn
    account=$(echo "$identity" | jq -r .Account)
    arn=$(echo "$identity" | jq -r .Arn)
    echo "-- AWS アカウント: ${account}"
    echo "-- IAM: ${arn}"
    echo ""

    # --- 接続モード選択 ---
    local mode
    mode=$(pick_one "接続モード" \
        "port-forward  RDS ポートフォワード" \
        "login         踏み台にログイン") \
        || die "接続モードが選択されませんでした"
    mode=$(echo "$mode" | awk '{print $1}')
    echo ""

    # --- 踏み台選択 ---
    local bastion
    bastion=$(select_bastion "$default_bastion")
    echo "-- 踏み台: ${bastion}"
    echo ""

    if [[ "$mode" == "login" ]]; then
        # --- 踏み台ログイン ---
        echo "=> aws ssm start-session --target ${bastion}"
        echo ""
        aws ssm start-session \
            ${AWS_PROFILE:+--profile "$AWS_PROFILE"} \
            --target "$bastion"
    else
        # --- ポートフォワード ---
        local rds_endpoint
        rds_endpoint=$(select_rds "$default_rds" "$default_engine")
        echo "-- RDS: ${rds_endpoint}"

        # エンジン・ポート自動検出
        local remote_port
        local engine_info
        engine_info=$(detect_engine_port "$rds_endpoint")

        if [[ -n "$engine_info" ]]; then
            local detected_engine detected_port
            detected_engine=$(echo "$engine_info" | cut -f1)
            detected_port=$(echo "$engine_info" | cut -f2)
            remote_port="${detected_port}"
            echo "-- エンジン: ${detected_engine} (自動検出)"
        else
            # 自動検出できない場合はデフォルトエンジンから推定
            case "${default_engine}" in
                mysql|aurora-mysql)    remote_port=3306 ;;
                postgres*|aurora-pos*) remote_port=5432 ;;
                *)
                    echo "リモートポートを入力してください (MySQL=3306, PostgreSQL=5432):" >&2
                    read -rp "> " remote_port
                    ;;
            esac
        fi

        local local_port="${default_local_port:-${remote_port}}"
        echo "-- リモートポート: ${remote_port}"
        echo "-- ローカルポート: ${local_port}"
        echo ""
        echo "=> ポートフォワードを開始します"
        echo "   localhost:${local_port} -> ${rds_endpoint}:${remote_port}"
        echo ""
        echo "   接続例:"
        case "${remote_port}" in
            3306)
                echo "     mysql -h 127.0.0.1 -P ${local_port} -u <USER> -p <DB_NAME>"
                ;;
            5432)
                echo "     psql -h 127.0.0.1 -p ${local_port} -U <USER> <DB_NAME>"
                ;;
            *)
                echo "     <client> -h 127.0.0.1 -p ${local_port}"
                ;;
        esac
        echo ""
        echo "   Ctrl+C で切断します"
        echo ""

        aws ssm start-session \
            ${AWS_PROFILE:+--profile "$AWS_PROFILE"} \
            --target "$bastion" \
            --document-name AWS-StartPortForwardingSessionToRemoteHost \
            --parameters "{\"host\":[\"${rds_endpoint}\"],\"portNumber\":[\"${remote_port}\"],\"localPortNumber\":[\"${local_port}\"]}"
    fi
}

main "$@"
