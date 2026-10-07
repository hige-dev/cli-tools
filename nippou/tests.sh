#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# 実際の設定・履歴・ホームを参照せず、外部サービスをシェル関数で代替する。
test_tmp=$(mktemp -d)
fixture=$(mktemp -d "$SCRIPT_DIR/.nippou-test.XXXXXX")
trap 'rm -rf "$test_tmp" "$fixture"' EXIT
export HOME="$fixture/home" TZ=Asia/Tokyo
mkdir -p "$HOME" "$fixture/docs" "$test_tmp/projects"
export NIPPOU_CONF="$test_tmp/config"
: > "$NIPPOU_CONF"
chmod 600 "$NIPPOU_CONF"
export GH_AUTHOR=me REPORT_DIR="$test_tmp/reports" HIST_FILE="$test_tmp/history"
export CLAUDE_HISTORY="$test_tmp/claude-history" CLAUDE_PROJECTS_DIR="$test_tmp/projects"
export DOC_ROOTS="$fixture/docs" SLACK_TOKEN="" SLACK_DRAFT_CHANNEL=""
export CLAUDE_MAX_ATTEMPTS=3 CLAUDE_RETRY_WAIT=0 DOC_MAX_FILES=20 DOC_MAX_LINES=60 PR_BODY_MAX_CHARS=800
source "$SCRIPT_DIR/nippou.sh"
load_config

fail() { echo "失敗: $*" >&2; exit 1; }
contains() { grep -Fq -- "$2" "$1" || fail "$1 に $2 がありません"; }
absent() { if grep -Fq -- "$2" "$1"; then fail "$1 に $2 が混入しています"; fi; }
equal() { [[ "$1" == "$2" ]] || fail "期待値: $2 / 実際: $1"; }

create_notes 2026-10-08
note=$(notes_path 2026-10-08)
printf '\n本人が確認した結果\n' >> "$note"
cp "$note" "$test_tmp/note-before"
create_notes 2026-10-08 2> "$test_tmp/notes-log"
cmp "$note" "$test_tmp/note-before" || fail '補足が上書きされました'
contains "$test_tmp/notes-log" '既存の補足を保持'

# 両方の検索・複数ページに同じ PR を返す。
gh() {
    printf '%s\n' "$*" >> "$test_tmp/gh-calls"
    case "$*" in
        *search/issues*)
            printf '%s\n' '{"items":[{"repository_url":"https://api.github.com/repos/owner/repo","number":1}]}'
            printf '%s\n' '{"items":[{"repository_url":"https://api.github.com/repos/owner/repo","number":1}]}' ;;
        *pulls/1/reviews*)
            printf '%s\n' '[{"user":{"login":"me"},"submitted_at":"2026-10-07T08:00:00Z","state":"APPROVED","body":"本人の判断","html_url":"https://github.com/owner/repo/pull/1#pullrequestreview-1"},{"user":{"login":"other"},"state":"APPROVED","body":"他者のレビュー"}]' ;;
        *pulls/1*)
            printf '%s\n' '{"html_url":"https://github.com/owner/repo/pull/1","title":"検証対象のPR","user":{"login":"me"},"state":"open","created_at":"2026-10-07T00:00:00Z","updated_at":"2026-10-08T00:00:00Z","merged_at":null,"body":"月報用の本文abcdefghij"}' ;;
        *) fail "想定外の gh 呼び出し: $*" ;;
    esac
}
collect_pull_requests 2026-10-08 > "$test_tmp/prs"
equal "$(grep -c '^### https:' "$test_tmp/prs")" 1
equal "$(grep -c '^レビュー日時:' "$test_tmp/prs")" 1
equal "$(grep -c 'api repos/owner/repo/pulls/1$' "$test_tmp/gh-calls")" 1
equal "$(grep -c 'reviews --paginate' "$test_tmp/gh-calls")" 1
contains "$test_tmp/gh-calls" 'is:pr author:me updated:2026-10-08 -f per_page=100 --paginate'
contains "$test_tmp/gh-calls" 'is:pr reviewed-by:me updated:2026-10-08 -f per_page=100 --paginate'
contains "$test_tmp/prs" '2026-10-07T08:00:00Z'
absent "$test_tmp/prs" '月報用の本文'
absent "$test_tmp/prs" '他者のレビュー'
(GH_AUTHOR=""; collect_pull_requests 2026-10-08) > "$test_tmp/no-author"
equal "$(cat "$test_tmp/no-author")" ''

# API の失敗は警告を出し、収集全体を中断しない。
saved_gh=$(declare -f gh)
gh() {
    if [[ "$*" == *'author:me'* ]]; then echo '検索エラー' >&2; return 1; fi
    case "$*" in
        *search/issues*) echo '{"items":[{"repository_url":"https://api.github.com/repos/owner/repo","number":1}]}' ;;
        *reviews*) echo 'レビューエラー' >&2; return 1 ;;
        *) echo '{"html_url":"https://github.com/owner/repo/pull/1","title":"残ったPR","user":{"login":"me"},"state":"open","created_at":"日時","updated_at":"日時","merged_at":null}' ;;
    esac
}
collect_pull_requests 2026-10-08 > "$test_tmp/partial-prs" 2> "$test_tmp/api-errors"
contains "$test_tmp/partial-prs" '残ったPR'
contains "$test_tmp/api-errors" '警告: PR 検索に失敗'
contains "$test_tmp/api-errors" '警告: レビューの取得に失敗'
gh() { return 1; }
printf 'https://github.com/owner/repo/pull/1\n' | collect_pr_bodies > "$test_tmp/failed-bodies" 2> "$test_tmp/body-errors"
contains "$test_tmp/body-errors" '警告: PR 本文の取得に失敗'
eval "$saved_gh"

# コミットの 1 行目に URL が付き、Slack は短文除外・文字数制限の後に URL が付く。
# 改行を含む JSON は jq で構築する。
gh() {
    local filter=""
    while (( $# )); do
        if [[ "$1" == --jq ]]; then filter="$2"; shift 2; else shift; fi
    done
    jq -nc '{items:[{repository:{full_name:"owner/repo"},commit:{message:"実装を検証\n本文"},html_url:"https://github.com/owner/repo/commit/abc"}]}' | jq -r "$filter"
}
collect_commits 2026-10-08 > "$test_tmp/commits"
contains "$test_tmp/commits" '- 実装を検証 https://github.com/owner/repo/commit/abc'
absent "$test_tmp/commits" '本文'
eval "$saved_gh"
curl() {
    jq -nc '{messages:{matches:[{text:"短文",channel:{name:"repo"},permalink:"短文URL"},{text:("あ" * 210),channel:{name:"repo"},permalink:"https://slack.example/message"}]}}'
}
(SLACK_TOKEN=mock; collect_slack 2026-10-08) > "$test_tmp/slack"
contains "$test_tmp/slack" 'https://slack.example/message'
absent "$test_tmp/slack" '短文URL'
equal "$(sed -n '2p' "$test_tmp/slack")" "- $(printf '%200s' '' | tr ' ' 'x' | sed 's/x/あ/g') https://slack.example/message"

mkdir -p "$DOC_ROOTS/node_modules" "$DOC_ROOTS/.git"
git -C "$DOC_ROOTS" init -q
printf '追跡資料\n' > "$DOC_ROOTS/tracked.md"
git -C "$DOC_ROOTS" add tracked.md
printf 'ignored.md\n' > "$DOC_ROOTS/.gitignore"
cat > "$DOC_ROOTS/ignored.md" <<'DOC'
---
秘密のfrontmatter: 非表示
---
# 調査資料

確認した背景と判断
---
後半の調査ログ
DOC
printf '別の日の資料\n' > "$DOC_ROOTS/old.md"
printf '依存資料\n' > "$DOC_ROOTS/node_modules/dep.md"
touch -d '2026-10-08 00:00:00' "$DOC_ROOTS/ignored.md" "$DOC_ROOTS/tracked.md" "$DOC_ROOTS/node_modules/dep.md"
touch -d '2026-10-07 12:00:00' "$DOC_ROOTS/old.md"
# 経路 B は mtime が変わっても当日の UTC 記録から採用する。
mkdir -p "$fixture/transcript"
printf '会話から得た判断\n' > "$fixture/transcript/edited.md"
printf '対象日外の編集\n' > "$fixture/transcript/outside.md"
printf 'Read のみ\n' > "$fixture/transcript/read.md"
touch -d '2026-10-09' "$fixture/transcript/edited.md"
jq -nc --arg file "$fixture/transcript/edited.md" '{timestamp:"2026-10-07T15:00:00.000Z",message:{content:[{type:"tool_use",name:"Edit",input:{file_path:$file}}]}}' > "$CLAUDE_PROJECTS_DIR/session.jsonl"
jq -nc --arg file "$fixture/transcript/outside.md" '{timestamp:"2026-10-08T15:00:00.000Z",message:{content:[{type:"tool_use",name:"Write",input:{file_path:$file}}]}}' >> "$CLAUDE_PROJECTS_DIR/session.jsonl"
jq -nc --arg file "$fixture/transcript/read.md" '{timestamp:"2026-10-08T00:00:00Z",message:{content:[{type:"tool_use",name:"Read",input:{file_path:$file}}]}}' >> "$CLAUDE_PROJECTS_DIR/session.jsonl"
collect_local_docs 2026-10-08 > "$test_tmp/docs"
contains "$test_tmp/docs" '確認した背景と判断'
contains "$test_tmp/docs" '会話から得た判断'
absent "$test_tmp/docs" 'frontmatter'
absent "$test_tmp/docs" '後半の調査ログ'
absent "$test_tmp/docs" '追跡資料'
absent "$test_tmp/docs" '別の日の資料'
absent "$test_tmp/docs" '対象日外の編集'
absent "$test_tmp/docs" '依存資料'
absent "$test_tmp/docs" 'Read のみ'
(DOC_MAX_FILES=1; collect_local_docs 2026-10-08) > "$test_tmp/limited-docs"
equal "$(grep -c '^### FILE:' "$test_tmp/limited-docs")" 1
(DOC_MAX_LINES=1; collect_local_docs 2026-10-08) > "$test_tmp/limited-lines"
absent "$test_tmp/limited-lines" '確認した背景と判断'

# 要約を一度だけ呼び、一覧見出しの表記揺れを受け入れる。
claude() {
    cat > "$test_tmp/prompt"
    printf '%s\n' "$*" >> "$test_tmp/claude-calls"
    cat <<'SUMMARY'
施策の要約本文

## やったこと（Slack 用）
■ repo
  - 判断を記録

## 今日やること
■ repo
  - 未確認事項を検証
SUMMARY
}
collect_commits() { :; }
collect_shell_history() { :; }
collect_claude_sessions() { :; }
build_daily_report 2026-10-08 > "$test_tmp/daily-stdout"
daily=$(daily_path 2026-10-08)
contains "$daily" '本人が確認した結果'
contains "$daily" 'https://github.com/owner/repo/pull/1'
contains "$daily" "$DOC_ROOTS/ignored.md"
absent "$daily" '確認した背景と判断'
contains "$test_tmp/prompt" '確認した背景と判断'
equal "$(wc -l < "$test_tmp/claude-calls" | tr -d ' ')" 1
awk '/^## 要約$/ {s=1;next} s && /^## / {exit} s {print}' "$daily" > "$test_tmp/summary"
absent "$test_tmp/summary" '判断を記録'
cat > "$test_tmp/expected-tail" <<'TAIL'
やったこと
■ repo
  - 判断を記録

明日やること
■ repo
  - 未確認事項を検証
TAIL
tail -7 "$test_tmp/daily-stdout" > "$test_tmp/tail"
cmp "$test_tmp/expected-tail" "$test_tmp/tail" || fail '標準出力末尾の一覧が不正です'
contains "$daily" '## 明日やること（Slack 用）'
for headings in 'やったこと|明日やること' '## やったこと|## 明日やること（Slack用）'; do
    printf '%s\n' "$headings" | tr '|' '\n' | extract_summary_section body > "$test_tmp/heading-body"
    equal "$(cat "$test_tmp/heading-body")" ''
done

collect_pull_requests() { :; }
collect_local_docs() { :; }
create_notes 2026-10-09
build_daily_report 2026-10-09 > /dev/null
contains "$(daily_path 2026-10-09)" '## 本人補足（生成時点）'
build_daily_report 2026-10-10 > /dev/null
contains "$(daily_path 2026-10-10)" '（記録なし）'
# 見出しのない返答でも追加のモデル呼び出しをしない。
claude() { cat > "$test_tmp/prompt"; echo '一覧なしの要約'; }
build_daily_report 2026-10-09 > "$test_tmp/no-lists"
contains "$(daily_path 2026-10-09)" '一覧なしの要約'

printf '\n生成後の最新補足\n' >> "$note"
printf '対象外の月報\n' > "$REPORT_DIR/2026/10/202610.md"
printf '別の月の日報\n' > "$REPORT_DIR/2026/10/2026-09-01.md"
mkdir -p "$REPORT_DIR/2026/10/nested"
printf '入れ子の日報\n' > "$REPORT_DIR/2026/10/nested/2026-10-11.md"
: > "$test_tmp/gh-calls"
PR_BODY_MAX_CHARS=8
build_monthly_report 2026-10 > /dev/null
contains "$test_tmp/prompt" '本人補足（最新）'
contains "$test_tmp/prompt" '生成後の最新補足'
contains "$test_tmp/prompt" 'PR本文（取得時点）'
contains "$test_tmp/prompt" '月報用の本文ab（切り詰め）'
absent "$test_tmp/prompt" '対象外の月報'
absent "$test_tmp/prompt" '別の月の日報'
absent "$test_tmp/prompt" '入れ子の日報'
equal "$(wc -l < "$test_tmp/gh-calls" | tr -d ' ')" 1

# 再試行の待機時間と、失敗出力が成功結果に混ざらないことを確認する。
printf '試験プロンプト\n' > "$test_tmp/retry-prompt"
claude() {
    cat > /dev/null
    local n=0
    [[ ! -f "$test_tmp/attempts" ]] || n=$(cat "$test_tmp/attempts")
    n=$((n + 1)); echo "$n" > "$test_tmp/attempts"
    if (( n < 3 )); then echo '一時的な過負荷' >&2; return 7; fi
    echo '成功結果'
}
sleep() { echo "$1" >> "$test_tmp/waits"; }
CLAUDE_RETRY_WAIT=2
run_claude "$test_tmp/retry-prompt" > "$test_tmp/retry-out" 2> "$test_tmp/retry-err"
equal "$(cat "$test_tmp/retry-out")" '成功結果'
equal "$(cat "$test_tmp/waits")" $'2\n4'
contains "$test_tmp/retry-err" '一時的な過負荷'
claude() { cat >/dev/null; echo '標準出力にあるエラー'; return 9; }
if run_claude "$test_tmp/retry-prompt" > "$test_tmp/retry-out" 2> "$test_tmp/retry-err"; then fail '全試行失敗を成功扱いしました'; else equal "$?" 9; fi
contains "$test_tmp/retry-err" '標準出力にあるエラー'
equal "$(cat "$test_tmp/retry-out")" ''

# 主処理の既定日と日付正規化を確認する。
build_daily_report() { echo "$1"; }
equal "$(main)" "$(date +%Y-%m-%d)"
equal "$(main 2026-10-8)" '2026-10-08'
if (main -j 0 2026-10-08 2026-10-09) > /dev/null 2>&1; then fail '並列数 0 を受け入れました'; fi
if (main 2026-10-09 2026-10-08) > /dev/null 2>&1; then fail '逆順の期間を受け入れました'; fi

(main -j 2 2026-10-08 2026-10-09) > "$test_tmp/parallel-success"
contains "$test_tmp/parallel-success" '成功: 2, 失敗: 0'
build_daily_report() { if [[ "$1" == 2026-10-09 ]]; then return 1; fi; echo "$1"; }
if (main -j 2 2026-10-08 2026-10-09) > "$test_tmp/parallel-failure"; then fail '並列処理の失敗を成功扱いしました'; fi
contains "$test_tmp/parallel-failure" '成功: 1, 失敗: 1'

printf 'PASS: 日報・月報・収集・補足・一覧・リトライの検証に成功しました\n'
