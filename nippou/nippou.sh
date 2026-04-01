#!/bin/bash
#
# nippou.sh - 日次作業レポート生成ツール
#
# GitHub コミット、シェル履歴、Slack メッセージを収集し、
# Claude CLI で要約して Markdown ファイルを出力する。
#
# 設定:
#   .env.example を .env にコピーして編集する。
#   NIPPOU_CONF 環境変数で .env のパスを指定可能。
#
# Usage:
#   ./nippou.sh                        # 前日分を生成
#   ./nippou.sh 2025-12-01             # 指定日を生成
#   ./nippou.sh 2025-12-01 2025-12-05  # 期間指定で生成（5並列）
#   ./nippou.sh -j 3 2025-12-01 2025-12-05  # 並列数を指定
#   ./nippou.sh --monthly 2025-12      # 月報を生成（日報から集約）
#   ./nippou.sh --monthly 2025-12 -o ./output/  # 出力先を指定
#

set -euo pipefail

# ---------------------------------------------------------------------------
# 設定
# ---------------------------------------------------------------------------

# シンボリックリンク経由でも実体のディレクトリを取得
SCRIPT_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
load_config() {
    # .env の探索: NIPPOU_CONF 環境変数 > スクリプトと同じディレクトリ
    local conf=""
    if [[ -n "${NIPPOU_CONF:-}" && -f "$NIPPOU_CONF" ]]; then
        conf="$NIPPOU_CONF"
    elif [[ -f "${SCRIPT_DIR}/.env" ]]; then
        conf="${SCRIPT_DIR}/.env"
    fi

    if [[ -n "$conf" ]]; then
        local perms
        perms=$(stat -c %a "$conf")
        if (( perms > 600 )); then
            echo "エラー: 設定ファイルのパーミッションが緩すぎます (${perms}): ${conf}" >&2
            echo "  chmod 600 ${conf} を実行してください" >&2
            exit 1
        fi
        # shellcheck source=/dev/null
        source "$conf"
    fi

    # 環境変数 > .env > デフォルト の優先度で解決
    GH_AUTHOR="${GH_AUTHOR:-$(gh api user --jq .login 2>/dev/null || echo "")}"
    REPORT_DIR="${REPORT_DIR:-${SCRIPT_DIR}/logs}"
    HIST_FILE="${HIST_FILE:-$HOME/.zsh_history}"
    CLAUDE_HISTORY="${CLAUDE_HISTORY:-$HOME/.claude/history.jsonl}"
}

# ---------------------------------------------------------------------------
# ユーティリティ
# ---------------------------------------------------------------------------

check_date() {
    date -d "$1" +%Y-%m-%d >/dev/null 2>&1 || {
        echo "エラー: 無効な日付です: $1" >&2
        exit 1
    }
}

# 開始日〜終了日の日付を1行ずつ出力
enumerate_days() {
    local d="$1" last="$2"
    while [[ "$d" < "$last" || "$d" == "$last" ]]; do
        echo "$d"
        d=$(date -d "$d + 1 day" +%Y-%m-%d)
    done
}

log_step() { echo "-- $1"; }

MAX_JOBS=5

# ---------------------------------------------------------------------------
# データ収集: GitHub コミット
# ---------------------------------------------------------------------------

collect_commits() {
    local day="$1"
    if [[ -z "$GH_AUTHOR" ]]; then
        return
    fi

    gh api "search/commits?q=author:${GH_AUTHOR}+author-date:${day}&per_page=100" \
        --jq '
            .items
            | group_by(.repository.full_name)
            | map({
                repo: .[0].repository.full_name,
                msgs: map(.commit.message | split("\n")[0])
              })
            | map("### " + .repo + "\n" + (.msgs | map("- " + .) | join("\n")))
            | join("\n\n")
        ' 2>/dev/null || true
}

# ---------------------------------------------------------------------------
# データ収集: シェルコマンド履歴
# ---------------------------------------------------------------------------

collect_shell_history() {
    local day="$1"
    [[ -f "$HIST_FILE" ]] || return 0

    local ts_from ts_to
    ts_from=$(date -d "${day} 00:00:00" +%s)
    ts_to=$(date -d "${day} 23:59:59" +%s)

    # zsh はメタ文字 (0x83 + byte^0x20) でエスケープする場合があるので perl でデコード
    perl -pe 's/\x83(.)/chr(ord($1)^0x20)/ge' "$HIST_FILE" \
        | awk -F'[:;]' -v lo="$ts_from" -v hi="$ts_to" '
            /^: [0-9]+:[0-9]+;/ {
                t = $2
                if (t >= lo && t <= hi) {
                    c = substr($0, index($0, ";") + 1)
                    # 空行と本スクリプト自身の呼び出しを除外
                    if (c !~ /^[[:space:]]*$/ && c !~ /^claude -p/)
                        print c
                }
            }
        ' 2>/dev/null | head -100 || true
}

# ---------------------------------------------------------------------------
# データ収集: Slack メッセージ
# ---------------------------------------------------------------------------

collect_slack() {
    local day="$1"
    [[ -n "${SLACK_TOKEN:-}" ]] || return 0

    curl -sf \
        -H "Authorization: Bearer ${SLACK_TOKEN}" \
        --get \
        --data-urlencode "query=from:me on:${day}" \
        --data-urlencode "count=100" \
        "https://slack.com/api/search.messages" \
    | jq -r '
        .messages.matches // []
        | map(.text |= (
            gsub("<@[A-Z0-9]+\\|(?<n>[^>]+)>"; "\(.n)")
            | gsub("<@[A-Z0-9]+>"; "")
            | gsub("<(?<u>https?://[^|>]+)\\|(?<l>[^>]+)>"; "\(.l)")
            | gsub("<(?<u>https?://[^>]+)>"; "\(.u)")
            | gsub(":[a-z0-9_+-]+:"; "")
            | gsub("\\s+"; " ")
            | ltrimstr(" ") | rtrimstr(" ")
        ))
        | map(select(.text | length >= 15))
        | group_by(.channel.name)
        | map(
            "### #" + .[0].channel.name + "\n"
            + (map("- " + (.text | gsub("\n"; " ") | .[0:200])) | join("\n"))
        )
        | join("\n\n")
    ' 2>/dev/null || true
}

# ---------------------------------------------------------------------------
# データ収集: Claude Code セッション履歴
# ---------------------------------------------------------------------------

collect_claude_sessions() {
    local day="$1"
    [[ -f "$CLAUDE_HISTORY" ]] || return 0

    local ts_from ts_to
    ts_from=$(date -d "${day} 00:00:00" +%s)000
    ts_to=$(date -d "${day} 23:59:59" +%s)999

    jq -r --arg lo "$ts_from" --arg hi "$ts_to" '
        select(
            (.timestamp | tonumber) >= ($lo | tonumber)
            and (.timestamp | tonumber) <= ($hi | tonumber)
            and (.project | startswith("/tmp") | not)
            and (.display | length >= 5)
        )
        | "- [" + .project + "] " + (.display | split("\n")[0] | .[0:200])
    ' "$CLAUDE_HISTORY" 2>/dev/null || true
}

# ---------------------------------------------------------------------------
# 月報生成
# ---------------------------------------------------------------------------

MONTHLY_PROMPT_TEMPLATE='以下は %s の日報データをまとめたものです。

これを職務経歴書に転用しやすい月報として日本語で整理してください。

## フォーマット

プロジェクト（リポジトリ）単位でグルーピングし、その中を施策ごとに分けること。
**プロジェクトはビジネスインパクトの大きい順に並べること。**
上位5プロジェクトは詳細に、それ以降は簡潔にまとめる。

```
# 職務経歴サマリー（YYYY年MM月）

## 主な成果指標
- 成果1（最もインパクトの大きい定量成果）
- 成果2
- 成果3

## 担当プロジェクト・業務実績

### ビジネス価値で表現したプロジェクト名（リポジトリ名）
**役割**: ロール名 | **スコープ**: チーム人数、サービス規模など

#### 施策タイトル（動詞で始める：構築、改善、最適化、移行、対応 など）
- **背景/課題**: なぜやったのか（1-3行。障害、要請、技術的負債など動機を書く。数値や影響度を含める）
- **打ち手と判断**: 何をしたか＋なぜその方法を選んだか（他の選択肢との比較を含める）
- **成果（ビフォー/アフター）**: 改善前→改善後を対比で示す（削減率、工数、SLO改善、コスト、リスク排除など）
- **技術**: (使用技術をカンマ区切り)
```

## ルール
- 施策タイトルは動詞で始める（「構築」「改善」「対応」など）
- 各項目の記述もAction verbで始める（NG:「RemixからのAPI呼び出しをlocalhost経由に変更」→ OK:「同一ECSタスク内通信をlocalhost経由に最適化し、レイテンシをX ms削減」）
- 背景/課題は具体的に書く。「〜が課題だった」で終わらせず、数値や影響を含める
- 打ち手と判断では、技術選定の理由や他の選択肢との比較を必ず含める（例: 「CDKではなくecspressoを選択。理由: 既存のECSタスク定義との互換性」）
- 成果は必ずビフォー/アフターの対比で書く（例: 「60分→20分に短縮（67%%削減）」）
- 「見込み」「予定」など未確定の成果は【未確定】と明記し、実測値との区別を明確にする
- 同一プロジェクト内で関連する小さな作業は1つの施策にまとめてよい
- リポジトリが特定できない作業は具体的なカテゴリ名でまとめる（「通信最適化・品質改善」「セキュリティ対応」など。「その他」は使わない）
- 日報データから背景/課題が読み取れない場合は、コミット内容から推測して書く
- 見出しはリポジトリ名だけでなくビジネス価値で表現する（NG: `proni-masking-by-glue（データマスキングジョブ）` → OK: `開発環境データマスキング基盤の再設計・安定化`）
- Node.jsバージョン更新やREADME追記のみの小規模作業は独立セクションにせず「横断的活動」に統合する

---
%s'

build_monthly_report() {
    local year_month="$1"
    local out_dir="${2:-${REPORT_DIR}}"

    local year="${year_month:0:4}"
    local month="${year_month:5:2}"
    local report_src="${REPORT_DIR}/${year}/${month}"

    if [[ ! -d "$report_src" ]]; then
        echo "エラー: 日報ディレクトリが見つかりません: ${report_src}" >&2
        exit 1
    fi

    local daily_files
    daily_files=$(find "$report_src" -name '*.md' -type f | sort)
    if [[ -z "$daily_files" ]]; then
        echo "エラー: ${report_src} に日報ファイルがありません" >&2
        exit 1
    fi

    local combined=""
    while IFS= read -r f; do
        combined+=$'\n\n---\n'"$(cat "$f")"
    done <<< "$daily_files"

    log_step "月報生成: ${year_month} ($(echo "$daily_files" | wc -l) 日分)"

    local prompt_file
    prompt_file=$(mktemp)

    # shellcheck disable=SC2059
    printf "$MONTHLY_PROMPT_TEMPLATE" "$year_month" "$combined" > "$prompt_file"

    local dest_file="${out_dir}/${year}${month}.md"
    mkdir -p "$(dirname "$dest_file")"

    log_step "Claude で月報を生成中..."
    local summary
    summary=$(cd /tmp && claude -p < "$prompt_file" 2>/dev/null) \
        || { rm -f "$prompt_file"; echo "エラー: Claude による月報生成に失敗しました" >&2; exit 1; }

    rm -f "$prompt_file"
    echo "$summary" > "$dest_file"
    log_step "完了: ${dest_file}"
    echo "--- プレビュー ---"
    head -40 "$dest_file"
}

# ---------------------------------------------------------------------------
# Claude による要約生成
# ---------------------------------------------------------------------------

summarize_with_claude() {
    local day="$1"
    local raw_data="$2"

    local prompt_file
    prompt_file=$(mktemp)

    cat > "$prompt_file" <<PROMPT
以下は ${day} の作業履歴です。
提供されたデータはすべて ${day} のものとしてフィルタリング済みです。

この作業履歴を日本語で要約してください。

## フォーマット
リポジトリ（プロジェクト）単位で整理し、各項目に次の情報を含めること:
- **何をしたか**: 動詞で始める（構築、改善、修正、最適化、対応 など）
- **なぜやったか**: 背景・課題（障害、パフォーマンス問題、要件など。コミットメッセージから推測可）
- **ビフォー/アフター**: 改善前後の状態（数値があればなお良い。推測の場合は「推定」と明記）
- **技術判断**: 技術選定の理由があれば記載（なぜその方法を選んだか）
- **使用技術**: （言語、フレームワーク、クラウドサービス等をカッコ内に記載）

### 例
#### owner/repo-name
- **ECSタスク定義のメモリ上限を調整**（Terraform, AWS ECS）：OOMによる再起動が頻発 → メモリ上限を512MB→1024MBに変更し再起動を解消
- **APIエンドポイントに認証を追加**（Go, JWT）：未認証でアクセス可能だった管理APIにJWTベースの認証を導入し、不正アクセスを防止
- **N+1クエリをバッチ化で解消**（TypeScript, Prisma）：1リクエストあたり約2,100回のDBクエリが発生 → GROUP BYバッチ化で99.8%削減。findManyではなくrawQueryを選択した理由はGROUP BY集計の柔軟性

## ルール
- リポジトリが特定できない作業は具体的なカテゴリ名でまとめる（「通信最適化」「品質改善」「セキュリティ対応」など。「その他」は避ける）
- コミットメッセージから背景/意図が不明な場合は、変更内容をそのまま簡潔に記載する
- 作業した分だけ書く（項目数の制限なし）
- 「見込み」「予定」など未確定の成果は明確に区別する

---
${raw_data}
PROMPT

    local result
    result=$(cd /tmp && claude -p < "$prompt_file" 2>/dev/null) \
        || result="（要約の生成に失敗しました）"
    rm -f "$prompt_file"
    echo "$result"
}

# ---------------------------------------------------------------------------
# 1 日分のレポートを生成して Markdown ファイルに書き出す
# ---------------------------------------------------------------------------

build_daily_report() {
    local day="$1"

    local dest_dir dest_file
    dest_dir="${REPORT_DIR}/${day:0:4}/${day:5:2}"
    dest_file="${dest_dir}/${day}.md"

    log_step "対象日: ${day}  出力先: ${dest_file}"
    mkdir -p "$dest_dir"

    # --- 各データソースを収集 ---
    log_step "GitHub コミットを取得"
    local commits
    commits=$(collect_commits "$day")

    log_step "シェル履歴を取得"
    local shell_cmds
    shell_cmds=$(collect_shell_history "$day")

    log_step "Claude セッション履歴を取得"
    local claude_sessions
    claude_sessions=$(collect_claude_sessions "$day")

    local slack_msgs=""
    if [[ -n "${SLACK_TOKEN:-}" ]]; then
        log_step "Slack メッセージを取得"
        slack_msgs=$(collect_slack "$day")
    fi

    # --- データが何もなければ空レポート ---
    if [[ -z "$commits" && -z "$shell_cmds" && -z "$slack_msgs" && -z "$claude_sessions" ]]; then
        log_step "記録が見つかりませんでした"
        printf "# 日報 %s\n\n## 要約\n（記録なし）\n" "$day" > "$dest_file"
        return
    fi

    # --- 収集データを結合 ---
    local sections=""
    [[ -n "$commits" ]]         && sections+=$'\n## Gitコミット履歴\n'"${commits}"
    [[ -n "$shell_cmds" ]]      && sections+=$'\n\n## コマンド履歴\n'"${shell_cmds}"
    [[ -n "$claude_sessions" ]] && sections+=$'\n\n## Claude Codeセッション\n'"${claude_sessions}"
    [[ -n "$slack_msgs" ]]      && sections+=$'\n\n## Slackメッセージ\n'"${slack_msgs}"

    # --- Claude で要約 ---
    log_step "Claude で要約を生成"
    local summary
    summary=$(summarize_with_claude "$day" "$sections")

    # --- Markdown 出力 ---
    {
        printf "# 日報 %s\n\n" "$day"
        printf "## 要約\n%s\n\n" "$summary"

        if [[ -n "$commits" ]]; then
            printf "## Gitコミット\n%s\n\n" "$commits"
        fi
        if [[ -n "$shell_cmds" ]]; then
            printf "## コマンド履歴\n\`\`\`bash\n%s\n\`\`\`\n\n" "$shell_cmds"
        fi
        if [[ -n "$claude_sessions" ]]; then
            printf "## Claude Codeセッション\n%s\n\n" "$claude_sessions"
        fi
        if [[ -n "$slack_msgs" ]]; then
            printf "## Slackメッセージ\n%s\n\n" "$slack_msgs"
        fi
    } > "$dest_file"

    log_step "完了: ${dest_file}"
    echo "--- プレビュー ---"
    head -30 "$dest_file"
}

# ---------------------------------------------------------------------------
# エントリーポイント
# ---------------------------------------------------------------------------

main() {
    load_config

    # -j オプションの解析
    if [[ "${1:-}" == "-j" ]]; then
        MAX_JOBS="${2:?Usage: $0 -j N ...}"
        shift 2
    fi

    # --monthly オプションの処理
    if [[ "${1:-}" == "--monthly" ]]; then
        local ym="${2:-}"
        if [[ ! "$ym" =~ ^[0-9]{4}-(0[1-9]|1[0-2])$ ]]; then
            echo "Usage: $0 --monthly YYYY-MM [-o OUTPUT_DIR]" >&2
            exit 1
        fi
        local out_dir="${REPORT_DIR}"
        if [[ "${3:-}" == "-o" && -n "${4:-}" ]]; then
            out_dir="$4"
        fi
        build_monthly_report "$ym" "$out_dir"
        return
    fi

    local from to
    case $# in
        0) from=$(date -d yesterday +%Y-%m-%d); to="$from" ;;
        1) from="$1"; to="$1" ;;
        *) from="$1"; to="$2" ;;
    esac

    check_date "$from"
    check_date "$to"

    local days num_days
    days=$(enumerate_days "$from" "$to")
    num_days=$(echo "$days" | wc -l)

    if (( num_days <= 1 )); then
        # 1日だけなら逐次実行
        build_daily_report "$from"
        return
    fi

    echo "期間: ${from} - ${to} (${num_days} 日間, 最大${MAX_JOBS}並列)"
    echo ""

    # 並列実行: 各日のログを一時ファイルに書き出し、完了順に表示
    _NIPPOU_TMPDIR=$(mktemp -d)
    trap 'rm -rf "$_NIPPOU_TMPDIR"' EXIT
    local tmpdir="$_NIPPOU_TMPDIR"

    local running=0
    local pids=()
    local pid_to_day=()

    while IFS= read -r d; do
        (
            build_daily_report "$d" > "${tmpdir}/${d}.log" 2>&1
            echo $? > "${tmpdir}/${d}.rc"
        ) &
        pids+=($!)
        pid_to_day+=("$d")
        running=$((running + 1))

        # 同時実行数が上限に達したら1つ完了を待つ
        if (( running >= MAX_JOBS )); then
            wait -n 2>/dev/null || true
            running=$((running - 1))
        fi
    done <<< "$days"

    # 残りのジョブを全て待つ
    wait

    # 日付順にログを表示
    local failed=0
    while IFS= read -r d; do
        local rc=0
        [[ -f "${tmpdir}/${d}.rc" ]] && rc=$(cat "${tmpdir}/${d}.rc")
        if [[ "$rc" -ne 0 ]]; then
            echo "✗ ${d}: 生成に失敗しました"
            failed=$((failed + 1))
        else
            echo "✓ ${d}"
        fi
        [[ -f "${tmpdir}/${d}.log" ]] && cat "${tmpdir}/${d}.log"
        echo ""
    done <<< "$days"

    echo "${num_days} 日分のレポートを生成しました (成功: $((num_days - failed)), 失敗: ${failed})"
}

main "$@"
