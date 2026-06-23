-- 장애 드릴다운: 지정 날짜 Silver 수집 지표 확인
-- 실행: aws athena start-query-execution --query-string "$(cat incident/01_bronze_silver_volume_diff.sql)"
-- 비용: Silver 1파티션
--
-- :run_date 파라미터: 아래 DATE '2026-06-22'를 조사할 날짜 (YYYY-MM-DD)로 수정 후 실행
--
-- 활용:
--   silver_rows = 0 → 배치 미실행 또는 S3 쓰기 실패
--   click_rate_pct ≈ 0 → Stage2 attribution MERGE 실패 (click 귀속 안됨)
--   avg_e2e_sec 급등 → 수집 당시 파이프라인 지연 장애
--   Bronze raw 행 수는 Athena 외부 테이블 없이 직접 집계 불가 → Silver 수치로 배치 정상 여부 판단

SELECT
    event_date,
    COUNT(*)                                                         AS silver_rows,
    SUM(click)                                                       AS attributed_clicks,
    SUM(conversion)                                                  AS attributed_conversions,
    ROUND(CAST(SUM(click) AS DOUBLE)      / COUNT(*) * 100, 4)      AS click_rate_pct,
    ROUND(CAST(SUM(conversion) AS DOUBLE) / COUNT(*) * 100, 4)      AS conv_rate_pct,
    ROUND(AVG(CAST(end_to_end_latency_sec AS DOUBLE)), 2)           AS avg_e2e_sec,
    approx_percentile(end_to_end_latency_sec, 0.5)                  AS p50_e2e_sec,
    approx_percentile(end_to_end_latency_sec, 0.95)                 AS p95_e2e_sec
FROM silver.processed_events
WHERE event_date = DATE '2026-06-22'
GROUP BY event_date;
