-- 파이프라인 스냅샷 신선도 알림 (0행=정상 / 1행+=Alert)
-- 실행: Airflow AthenaOperator → PythonOperator len(rows) > 0 시 AirflowException
-- 비용: $snapshots 메타테이블 — 사실상 무료
--
-- 감지: Silver 또는 Gold에 오늘 Iceberg 스냅샷이 없음
-- Airflow gold_batch SUCCESS여도 Spark 조용한 실패로 Iceberg 커밋이 없는 경우를 잡아냄

SELECT
    layer,
    'no_snapshot_today'                AS alert_type,
    CAST(latest_committed AS VARCHAR)  AS latest_committed_at
FROM (
    SELECT
        'silver'             AS layer,
        MAX(committed_at)    AS latest_committed
    FROM silver."processed_events$snapshots"
    UNION ALL
    SELECT
        'gold',
        MAX(committed_at)
    FROM gold."campaign_summary$snapshots"
) t
WHERE DATE(latest_committed) < current_date;
