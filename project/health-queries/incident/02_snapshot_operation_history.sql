-- 장애 드릴다운: Silver/Gold 최근 스냅샷 작업 이력
-- 실행: aws athena start-query-execution --query-string "$(cat incident/02_snapshot_operation_history.sql)"
-- 비용: $snapshots 메타테이블 — 사실상 무료
--
-- 활용:
--   마지막 append 시각 → 배치가 언제 성공했는지 파악
--   rollback 필요 시 parent_id로 직전 스냅샷 snapshot_id 확인
--   operation 종류: append(정상 배치), overwrite(full-refresh), delete(expire_snapshots)

SELECT *
FROM (
    SELECT
        'silver'       AS layer,
        snapshot_id,
        parent_id,
        committed_at,
        operation
    FROM silver."processed_events$snapshots"
    UNION ALL
    SELECT
        'gold',
        snapshot_id,
        parent_id,
        committed_at,
        operation
    FROM gold."campaign_summary$snapshots"
) t
ORDER BY committed_at DESC
LIMIT 20;
