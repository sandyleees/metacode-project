-- Gold: campaign_summary 테이블 정의
-- health-queries/*.sql 작성 시 컬럼 참조용, Superset 대시보드 컬럼 확인용
-- 설계 배경: JOBS_GUIDE.md §3-3, §3-4

CREATE DATABASE IF NOT EXISTS glue_catalog.gold;

CREATE TABLE IF NOT EXISTS glue_catalog.gold.campaign_summary (
    summary_date                DATE      NOT NULL COMMENT 'event_date 기준 집계 날짜; 파티션 키',
    campaign                    INT       NOT NULL COMMENT '캠페인 식별자',

    impressions                 BIGINT             COMMENT 'COUNT(*)',
    clicks                      BIGINT             COMMENT 'SUM(click)',
    conversions                 BIGINT             COMMENT 'SUM(conversion)',
    unique_users                BIGINT             COMMENT 'COUNT(DISTINCT uid)',
    converting_users            BIGINT             COMMENT 'COUNT(DISTINCT uid WHERE conversion=1)',
    total_cost                  DOUBLE             COMMENT 'SUM(cost)',

    ctr                         DOUBLE             COMMENT 'clicks / impressions × 100',
    cvr                         DOUBLE             COMMENT 'conversions / clicks × 100; clicks=0이면 NULL',
    cpc                         DOUBLE             COMMENT 'total_cost / clicks; clicks=0이면 NULL',
    cpa                         DOUBLE             COMMENT 'total_cost / conversions; conversions=0이면 NULL',
    cpm                         DOUBLE             COMMENT 'total_cost / impressions × 1000',

    click_through_conversions   BIGINT             COMMENT 'COUNT(click=1 AND conversion=1)',
    view_through_conversions    BIGINT             COMMENT 'COUNT(click=0 AND conversion=1)',
    avg_conversion_delay_sec    DOUBLE             COMMENT 'AVG(conversion_delay_sec >= 0); -1 sentinel 제외',

    frequency                   DOUBLE             COMMENT 'impressions / unique_users',

    updated_at                  TIMESTAMP          COMMENT 'MERGE 실행 시각'
)
USING iceberg
PARTITIONED BY (summary_date)
TBLPROPERTIES (
    'format-version'                              = '2',
    'write.merge.mode'                            = 'copy-on-write',
    'write.target-file-size-bytes'                = '134217728',
    'write.metadata.previous-versions-max'        = '7',
    'write.metadata.delete-after-commit.enabled'  = 'true'
);
