-- 파이프라인 지연 임계치 알림 (0행=정상 / 1행+=Alert)
-- 실행: Airflow AthenaOperator → PythonOperator len(rows) > 0 시 AirflowException
-- 비용: Silver 어제 1파티션
--
-- 감지: 어제 end_to_end_latency_sec p50 > 300초(5분)
-- p50(중앙값) 기준으로 이상치에 강건 — 소수 이벤트의 극단값에 흔들리지 않음

SELECT
    event_date,
    approx_percentile(end_to_end_latency_sec, 0.5)  AS p50_e2e_sec,
    '5min_latency_threshold_breached'                AS alert_type
FROM silver.processed_events
WHERE event_date = current_date - interval '1' day
GROUP BY event_date
HAVING approx_percentile(end_to_end_latency_sec, 0.5) > 300;
