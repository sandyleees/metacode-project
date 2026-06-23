-- 7일 볼륨 이상 탐지: 이번 주 vs 전주 동일 요일 행 수 비교 (Superset 운영 탭 Dataset, 주간 refresh)
-- 비용: 💰 Silver 14파티션 COUNT (7일 × 2주)
--
-- 구조적 전제:
--   Silver는 MERGE ON event_id(UPSERT) 구조 → COUNT(*) = COUNT(DISTINCT event_id) 항상 성립
--   → Silver 내부 dup_rate는 설계상 항상 0% (Kafka 중복은 MERGE에서 흡수됨)
--   → 중복 모니터링 대신 주간 볼륨 이상 탐지로 전환
--
-- wow_change_pct 해석:
--   ±30% 이상 이탈 → 수집 이상(급증: Kafka 재전송 폭주 / 급감: Bronze 배치 누락) 의심
--   NULL prev_week → 전주 데이터 없음 (파이프라인 초기 운영 중)
--
-- 주간 refresh로 내린 이유:
--   일별 볼륨 추이는 ops/01에서 확인 — 이 쿼리는 주간 기준 이상 신호에 집중

WITH this_week AS (
    SELECT
        event_date,
        COUNT(*) AS rows_this_week
    FROM silver.processed_events
    WHERE event_date >= date_add('day', -7, current_date)
      AND event_date <  current_date
    GROUP BY event_date
),
prev_week AS (
    SELECT
        date_add('day', 7, event_date) AS event_date,  -- 전주 날짜를 이번 주 기준으로 정렬
        COUNT(*) AS rows_prev_week
    FROM silver.processed_events
    WHERE event_date >= date_add('day', -14, current_date)
      AND event_date <  date_add('day', -7, current_date)
    GROUP BY event_date
)
SELECT
    t.event_date,
    t.rows_this_week,
    p.rows_prev_week,
    ROUND(
        (CAST(t.rows_this_week AS DOUBLE) / NULLIF(p.rows_prev_week, 0) - 1) * 100,
        2
    ) AS wow_change_pct
FROM this_week t
LEFT JOIN prev_week p ON t.event_date = p.event_date
ORDER BY t.event_date;
