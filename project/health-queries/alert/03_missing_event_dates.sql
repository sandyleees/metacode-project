-- Silver 결측 날짜 알림 (0행=정상 / 1행+=Alert)
-- 실행: Airflow AthenaOperator → PythonOperator len(rows) > 0 시 AirflowException
-- 비용: Silver 30파티션 COUNT(*) (파티션 프루닝 적용)
--
-- 감지: 최근 30일 중 Silver에 event_date 파티션이 없는 날짜
-- conversion attribution window가 30일이므로, 30일 이내 결측 파티션은
-- Gold 집계에 조용히 영향 — 배치 스케줄 누락·S3 쓰기 실패 조기 탐지

WITH date_range AS (
    SELECT CAST(dt AS DATE) AS expected_date
    FROM UNNEST(SEQUENCE(
        CAST(date_add('day', -30, current_date) AS TIMESTAMP),
        CAST(date_add('day', -1, current_date) AS TIMESTAMP),
        INTERVAL '1' DAY
    )) AS t(dt)
),
existing_dates AS (
    SELECT DISTINCT event_date
    FROM silver.processed_events
    WHERE event_date >= date_add('day', -30, current_date)
      AND event_date <  current_date
)
SELECT
    d.expected_date              AS missing_date,
    'event_date_missing_in_silver' AS alert_type
FROM date_range d
LEFT JOIN existing_dates e ON d.expected_date = e.event_date
WHERE e.event_date IS NULL;
