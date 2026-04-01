#!/bin/bash
#
# install.sh - ~/.local/bin にシンボリックリンクを作成する
#
# Usage:
#   ./install.sh          # リンク作成
#   ./install.sh --remove # リンク削除
#

set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN_DIR="$HOME/.local/bin"

# リンク名 → スクリプトの相対パス
declare -A TOOLS=(
    [nippou]="nippou/nippou.sh"
    [ecs_exec]="ecs-exec/ecs-exec.sh"
    [db_connect]="db-connect/db-connect.sh"
    [waf_logs]="waf-logs/waf_logs.py"
    [devops_agent]="devops-agent/devops-agent.sh"
)

install_links() {
    mkdir -p "$BIN_DIR"

    for name in "${!TOOLS[@]}"; do
        local src="${REPO_DIR}/${TOOLS[$name]}"
        local dest="${BIN_DIR}/${name}"

        if [[ ! -f "$src" ]]; then
            echo "スキップ: ${src} が見つかりません"
            continue
        fi

        chmod +x "$src"

        if [[ -L "$dest" ]]; then
            echo "更新: ${name} -> ${src}"
            ln -sf "$src" "$dest"
        elif [[ -e "$dest" ]]; then
            echo "スキップ: ${dest} は既に存在します（シンボリックリンクではありません）"
        else
            echo "作成: ${name} -> ${src}"
            ln -s "$src" "$dest"
        fi
    done

    # PATH チェック
    if [[ ":$PATH:" != *":$BIN_DIR:"* ]]; then
        echo ""
        echo "注意: ${BIN_DIR} が PATH に含まれていません。"
        echo "以下を .zshrc に追加してください:"
        echo ""
        echo '  export PATH="$HOME/.local/bin:$PATH"'
    fi

    echo ""
    echo "完了"
}

remove_links() {
    for name in "${!TOOLS[@]}"; do
        local dest="${BIN_DIR}/${name}"
        if [[ -L "$dest" ]]; then
            echo "削除: ${dest}"
            rm "$dest"
        fi
    done

    echo ""
    echo "完了"
}

case "${1:-}" in
    --remove) remove_links ;;
    *)        install_links ;;
esac
