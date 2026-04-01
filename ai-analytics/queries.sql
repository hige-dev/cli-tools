-- Claude Code 利用分析クエリ集
-- 実行: uv run python -c "import duckdb; con = duckdb.connect('claude_usage.duckdb'); print(con.sql(open('queries.sql').read().split('---')[N]).fetchdf())"
-- または DuckDB CLI: duckdb claude_usage.duckdb < queries.sql

-- ■ 基本統計: イベント種別ごとの件数
SELECT
    hook_event_name,
    count(*) AS cnt
FROM events
GROUP BY hook_event_name
ORDER BY cnt DESC;

--- ■ 日別セッション数
SELECT
    timestamp::DATE AS day,
    count(DISTINCT session_id) AS sessions
FROM events
GROUP BY day
ORDER BY day DESC
LIMIT 30;

--- ■ ツール利用ランキング
SELECT
    tool_name,
    count(*) AS cnt
FROM events
WHERE tool_name IS NOT NULL
GROUP BY tool_name
ORDER BY cnt DESC;

--- ■ セッションごとのプロンプト数
SELECT
    session_id,
    count(*) AS prompts,
    min(timestamp) AS started,
    max(timestamp) AS ended,
    age(max(timestamp), min(timestamp)) AS duration
FROM events
WHERE hook_event_name = 'UserPromptSubmit'
GROUP BY session_id
ORDER BY started DESC
LIMIT 20;

--- ■ プロジェクト別（cwd）利用頻度
SELECT
    cwd,
    count(DISTINCT session_id) AS sessions,
    count(*) AS events
FROM events
WHERE cwd IS NOT NULL
GROUP BY cwd
ORDER BY sessions DESC;

--- ■ 時間帯別の利用パターン
SELECT
    hour(timestamp) AS hour_of_day,
    count(DISTINCT session_id) AS sessions,
    count(*) FILTER (WHERE hook_event_name = 'UserPromptSubmit') AS prompts
FROM events
GROUP BY hour_of_day
ORDER BY hour_of_day;
