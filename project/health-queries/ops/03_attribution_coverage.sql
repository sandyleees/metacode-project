-- 7일 일별 click/conversion 귀속 비율 (Superset 운영 탭 Dataset, 24h 캐시)
-- 비용: Silver 7파티션 SUM (비교적 저렴)
--
-- 파이프라인 Stage2(attribution MERGE) 정상 동작 확인용
--   click_rate_pct 또는 conv_rate_pct가 0%로 떨어지면 Stage2 장애 가능성
--   Gold CVR(conversions/clicks)과 함께 보면 집계 단계 문제 여부도 구분 가능
--
-- 캠페인 성과 판단(CTR/CVR)이 목적이 아닌 파이프라인 체크 전용

SELECT
    event_date,
    COUNT(*)                                                         AS impressions,
    SUM(click)                                                       AS clicks,
    SUM(conversion)                                                  AS conversions,
    ROUND(CAST(SUM(click) AS DOUBLE)      / COUNT(*) * 100, 4)      AS click_rate_pct,
    ROUND(CAST(SUM(conversion) AS DOUBLE) / COUNT(*) * 100, 4)      AS conv_rate_pct
FROM silver.processed_events
WHERE event_date >= date_add('day', -7, current_date)
  AND event_date <  current_date
GROUP BY event_date
ORDER BY event_date;
