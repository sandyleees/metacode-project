-- Silver-Gold 일관성 알림 (0행=정상 / 1행+=Alert)
-- 실행: Airflow AthenaOperator → PythonOperator len(rows) > 0 시 AirflowException
-- 비용: Silver 어제 1파티션 + Gold 어제 1파티션
--
-- 감지:
--   (1) Gold 어제 파티션 자체가 없음 (배치 미실행 또는 집계 실패)
--   (2) Silver COUNT(*) vs Gold SUM(impressions) 차이가 10% 초과
-- 10% 임계치 이유: attribution 업데이트로 Gold는 과거 파티션도 재기록되므로 미세 시간차 허용
-- LEFT JOIN 이유: INNER JOIN 시 Gold 파티션 없으면 0행 반환 → 조용한 오판(false normal)

SELECT
    s.event_date,
    s.silver_rows,
    COALESCE(g.gold_impressions, 0)     AS gold_impressions,
    CASE
        WHEN g.summary_date IS NULL THEN 100.0
        ELSE ROUND(
            ABS(CAST(s.silver_rows - g.gold_impressions AS DOUBLE))
            / NULLIF(s.silver_rows, 0) * 100,
            2
        )
    END                                  AS diff_pct,
    CASE
        WHEN g.summary_date IS NULL THEN 'gold_partition_missing'
        ELSE 'silver_gold_count_mismatch'
    END                                  AS alert_type
FROM (
    SELECT
        event_date,
        COUNT(*) AS silver_rows
    FROM silver.processed_events
    WHERE event_date = current_date - interval '1' day
    GROUP BY event_date
) s
LEFT JOIN (
    SELECT
        summary_date,
        SUM(impressions) AS gold_impressions
    FROM gold.campaign_summary
    WHERE summary_date = current_date - interval '1' day
    GROUP BY summary_date
) g ON s.event_date = g.summary_date
WHERE g.summary_date IS NULL
   OR ABS(CAST(s.silver_rows - g.gold_impressions AS DOUBLE))
      / NULLIF(s.silver_rows, 0) > 0.1;
