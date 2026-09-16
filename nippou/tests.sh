#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/nippou.sh"
test_dir=$(mktemp -d)
trap 'rm -rf "$test_dir"' EXIT
REPORT_DIR="$test_dir/reports"
create_notes 2026-09-16 >/dev/null
note=$(notes_path 2026-09-16)
printf '\n担当範囲のテスト記録\n' >> "$note"
cp "$note" "$test_dir/original"
create_notes 2026-09-16 >/dev/null
cmp "$note" "$test_dir/original"
# Both searches return the same PR; fetch it only once.
GH_AUTHOR=test-user
gh() {
    case "$*" in
        *search/issues*) printf 'https://api.github.com/repos/org/repo\t42\n' ;;
        *reviews*) printf 'レビューの根拠\n' ;;
        *) printf 'PR詳細の根拠\n' ;;
    esac
}
prs=$(collect_pull_requests 2026-09-16)
[[ $(printf '%s\n' "$prs" | grep -c 'PR詳細の根拠') == 1 ]]
[[ $(printf '%s\n' "$prs" | grep -c 'レビューの根拠') == 1 ]]
unset -f gh
collect_commits() { :; }
collect_pull_requests() { printf 'PRの根拠 https://example.test/pr/1'; }
collect_shell_history() { :; }
collect_claude_sessions() { :; }
# Capture prompts instead of invoking the model.
claude() { cat; }
SLACK_TOKEN=""
build_daily_report 2026-09-16 >/dev/null
daily="$REPORT_DIR/2026/09/2026-09-16.md"
grep -q '担当範囲のテスト記録' "$daily"
grep -q 'https://example.test/pr/1' "$daily"
printf '\n後から追記した確認結果\n' >> "$note"
printf '混入してはいけない月報' > "$REPORT_DIR/2026/09/202609.md"
printf '混入してはいけない別月' > "$REPORT_DIR/2026/09/2026-08-16.md"
build_monthly_report 2026-09 >/dev/null
monthly="$REPORT_DIR/202609.md"
grep -q '後から追記した確認結果' "$monthly"
if grep -q '混入してはいけない' "$monthly"; then
    echo 'FAIL: monthly input contains unrelated files' >&2
    exit 1
fi
build_daily_report 2026-09-16 >/dev/null
grep -q '後から追記した確認結果' "$note"
# Notes alone must be sufficient input for a daily report.
collect_pull_requests() { :; }
build_daily_report 2026-09-16 >/dev/null
grep -q '担当範囲のテスト記録' "$daily"
# Empty days still get a report.
build_daily_report 2026-09-17 >/dev/null
grep -q '（記録なし）' "$REPORT_DIR/2026/09/2026-09-17.md"
echo 'PASS: notes preserved; daily and monthly inputs verified'
