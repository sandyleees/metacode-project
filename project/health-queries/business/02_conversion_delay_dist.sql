-- conversion 지연 분포 (Superset 비즈니스 탭 Dataset, 주간 refresh)
-- 비용: 💰 Silver 30파티션 APPROX_PERCENTILE
--
-- attribution window(현재 30일) 적정성 검증 (단위: 초):
--   p95 ≤ 432,000초(5일)    → 30일 window는 과대 설정, 단축 고려
--   p50 ≥ 2,160,000초(25일) → window 확장 필요 신호
--
-- 주간 refresh로 내린 이유:
--   전환 지연 분포는 일별 변동이 의미 없고 주간 누적으로 봐야 패턴이 보임

SELECT
    approx_percentile(conversion_delay_sec, 0.25)   AS p25_sec,
    approx_percentile(conversion_delay_sec, 0.5)    AS p50_sec,
    approx_percentile(conversion_delay_sec, 0.75)   AS p75_sec,
    approx_percentile(conversion_delay_sec, 0.9)    AS p90_sec,
    approx_percentile(conversion_delay_sec, 0.95)   AS p95_sec,
    COUNT(*)                                        AS conversion_count
FROM silver.processed_events
WHERE conversion          =  1
  AND conversion_delay_sec >= 0
  AND event_date >= date_add('day', -30, current_date)
  AND event_date <  current_date;
