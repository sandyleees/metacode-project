-- 14일 일별 단계별 파이프라인 지연 p50 추이 (Superset 운영 탭 Dataset, 24h 캐시)
-- 비용: 💰 Silver 14파티션 APPROX_PERCENTILE
--
-- 단계 분리 설계 이유:
--   end_to_end만 보면 원인 단계를 알 수 없음
--   p50_p2b 증가 → Kafka 네트워크·프로듀서 지연
--   p50_b2i 증가 → Spark 소비자 처리 지연 (backpressure, 파티션 불균형 등)

SELECT
    event_date,
    approx_percentile(producer_to_broker_sec, 0.5)   AS p50_p2b_sec,
    approx_percentile(broker_to_ingest_sec,  0.5)    AS p50_b2i_sec,
    approx_percentile(end_to_end_latency_sec, 0.5)   AS p50_e2e_sec
FROM silver.processed_events
WHERE event_date >= date_add('day', -14, current_date)
  AND event_date <  current_date
GROUP BY event_date
ORDER BY event_date;
