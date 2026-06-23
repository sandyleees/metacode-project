-- 30일 일별·캠페인별 KPI 추이 (Superset 비즈니스 탭 Dataset, 24h 캐시)
-- 비용: Gold 30파티션 — 집계 테이블이라 Silver 대비 스캔 비용 낮음
--
-- BI팀·Campaign팀 전용 — CTR/CVR/CPC/CPM/CPA 성과 판단
-- DE가 보는 attribution_coverage(ops/03)와 달리 비즈니스 지표 중심

SELECT
    summary_date,
    campaign,
    impressions,
    clicks,
    conversions,
    unique_users,
    converting_users,
    total_cost,
    ROUND(ctr,       4)  AS ctr,
    ROUND(cvr,       4)  AS cvr,
    ROUND(cpc,       4)  AS cpc,
    ROUND(cpa,       4)  AS cpa,
    ROUND(cpm,       4)  AS cpm,
    click_through_conversions,
    view_through_conversions,
    ROUND(frequency, 4)  AS frequency
FROM gold.campaign_summary
WHERE summary_date >= date_add('day', -30, current_date)
  AND summary_date <  current_date
ORDER BY summary_date DESC, campaign;
