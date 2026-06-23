-- 14일 일별 트래픽 볼륨 추이 (Superset 운영 탭 Dataset, 24h 캐시)
-- 비용: 💰 Silver 14파티션 COUNT()
--
-- Silver 기준으로 집계하는 이유:
--   Gold는 attribution 업데이트로 과거 파티션이 재기록되므로 순수 수집 볼륨에 부적합
--   Silver impression row count = 실제 수집 건수

SELECT
    event_date,
    COUNT(*)             AS impressions,
    SUM(click)           AS clicks,
    SUM(conversion)      AS conversions,
    COUNT(DISTINCT uid)  AS unique_users
FROM silver.processed_events
WHERE event_date >= date_add('day', -14, current_date)
  AND event_date <  current_date
GROUP BY event_date
ORDER BY event_date;
