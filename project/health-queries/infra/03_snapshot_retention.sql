-- Silver/Gold 스냅샷 보존 현황 (주간 수동 실행)
-- 실행: aws athena start-query-execution --query-string "$(cat infra/03_snapshot_retention.sql)"
-- 비용: $snapshots 메타테이블 — 사실상 무료
--
-- 해석:
--   expire_snapshots 후 old 스냅샷 실제 제거 확인
--   설정 기준: Silver previous-versions-max=21 (7일×3커밋), Gold=7 (7일×1커밋)
--   retention_days = time travel 가용 범위 (oldest → latest 스냅샷 간격)

SELECT
    'silver'           AS layer,
    COUNT(*)           AS snapshot_count,
    MIN(committed_at)  AS oldest_snapshot,
    MAX(committed_at)  AS latest_snapshot,
    date_diff(
        'day',
        MIN(committed_at),
        MAX(committed_at)
    )                  AS retention_days
FROM silver."processed_events$snapshots"
UNION ALL
SELECT
    'gold',
    COUNT(*),
    MIN(committed_at),
    MAX(committed_at),
    date_diff('day', MIN(committed_at), MAX(committed_at))
FROM gold."campaign_summary$snapshots";
