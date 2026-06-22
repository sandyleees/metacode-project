-- Silver: processed_events 테이블 정의
-- health-queries/*.sql 작성 시 컬럼 참조용
-- 설계 배경: JOBS_GUIDE.md §2-4, §2-5

CREATE DATABASE IF NOT EXISTS glue_catalog.silver;

CREATE TABLE IF NOT EXISTS glue_catalog.silver.processed_events (
    event_id                STRING    NOT NULL COMMENT 'impression eid — 세 이벤트 타입 간 공통 join 키',
    event_date              DATE      NOT NULL COMMENT '파티션 키; impression unix ts 기준 일자',
    event_hour              INT                COMMENT '시계열 분석용; impression 발생 시(0~23)',
    event_time              TIMESTAMP          COMMENT 'impression 발생 절대시각',
    uid                     STRING    NOT NULL COMMENT '유저 식별자',
    campaign                INT                COMMENT '캠페인 식별자',
    click                   INT                COMMENT '클릭 발생 1, 없으면 0',
    conversion              INT                COMMENT '전환 발생 1, 없으면 0',
    conversion_timestamp    BIGINT             COMMENT '전환 unix ts(초); 전환 없으면 -1',
    conversion_delay_sec    BIGINT             COMMENT 'conversion_ts - impression_ts(초); 전환 없으면 -1',
    cost                    DOUBLE             COMMENT 'impression 비용 (transformed)',
    producer_to_broker_sec  BIGINT             COMMENT '브로커 수신 - 이벤트 발생(초); 프로듀서·네트워크 지연',
    broker_to_ingest_sec    BIGINT             COMMENT 'Spark ingest - 브로커 수신(초); 소비자 처리 지연',
    end_to_end_latency_sec  BIGINT             COMMENT 'Spark ingest - 이벤트 발생(초); 전체 파이프라인 지연',
    updated_at              TIMESTAMP          COMMENT 'click/conversion MERGE 반영 시각'
)
USING iceberg
PARTITIONED BY (event_date)
TBLPROPERTIES (
    'format-version'                              = '2',
    'write.merge.mode'                            = 'merge-on-read',
    'write.target-file-size-bytes'                = '134217728',
    'write.metadata.previous-versions-max'        = '21',
    'write.metadata.delete-after-commit.enabled'  = 'true'
);
