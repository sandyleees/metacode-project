"""
Criteo Medallion 일배치 DAG
  Silver: raw_to_processed_iceberg.py   (Bronze raw → processed_events Iceberg)
  Gold  : processed_to_campaign_summary.py (processed_events → campaign_summary Iceberg)

Bronze(kafka_to_raw.py)는 Spark Structured Streaming — 장시간 실행 서비스이므로
Airflow가 직접 트리거하지 않고 docker-compose restart:on-failure 로 별도 관리.
이 DAG는 Bronze 파티션 존재를 확인한 뒤 Silver → Gold 순서를 보장한다.

스케줄 타이밍 (Airflow 3.x):
  UTC 02:00 실행 → {{ macros.ds_add(ds, -1) }} = 전날 날짜 = 처리 대상 Bronze 파티션
  예) 2026-06-24 02:00 실행 → ds=2026-06-24, ds-1=2026-06-23 → raw_date=2026-06-23 처리
  ※ Airflow 3.x에서 {{ ds }} = logical_date.date() = 트리거 당일이므로 ds-1을 데이터 날짜로 사용

멱등성:
  Silver MERGE ON (event_id, event_date) — 동일 날짜 재실행 안전
  Gold   MERGE ON (summary_date, campaign) — 동일 날짜 재실행 안전
  max_active_runs=1 — 동시 MERGE 충돌 차단
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from dag_config import PROJECT_DIR, S3_RAW_BUCKET, SPARK_CONF, spark_env_vars
from dag_utils import make_athena_gate_task

_ENV_VARS = spark_env_vars(include_raw_base=True)

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=2),
    # TODO [Alert]: 알람 연동 후 아래 줄 활성화
    # "email": ["data-team@company.com"],
    # "email_on_failure": True,
    # "on_failure_callback": slack_alert,
}

with DAG(
    dag_id="criteo_medallion_daily",
    description="Bronze 확인 → Silver → Gold 일배치 (Iceberg Medallion)",
    schedule="0 2 * * *",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    max_active_runs=1,  # Silver/Gold MERGE 동시 실행 방지
    default_args=default_args,
    tags=["criteo", "iceberg", "medallion"],
    # TODO [Backfill]: DAG Run Conf 백필 지원 미구현
    #   PythonOperator로 application_args 동적 생성 후 SparkSubmitOperator에 전달하거나
    #   trigger_dag_id로 별도 백필 DAG 분리
) as dag:

    # Bronze는 Streaming 서비스 — 완료 신호 없음.
    # S3 파티션 파일 존재 확인으로 Silver가 빈 배치로 실행되는 것을 막는다.
    check_bronze_impressions = S3KeySensor(
        task_id="check_bronze_impressions",
        bucket_name=S3_RAW_BUCKET,
        bucket_key="raw/impressions/raw_date={{ macros.ds_add(ds, -1) }}/raw_hour=*/part-*.parquet",
        wildcard_match=True,
        aws_conn_id="aws_default",
        poke_interval=300,
        timeout=3600,        # 1시간 내 파일 없으면 FAILED → Bronze 장애 의심
        mode="reschedule",   # 대기 중 Worker 슬롯 반납
        soft_fail=False,
    )

    silver_batch = SparkSubmitOperator(
        task_id="silver_batch",
        application=f"{PROJECT_DIR}/jobs/raw_to_processed_iceberg.py",
        conn_id="spark_default",
        conf=SPARK_CONF,
        env_vars=_ENV_VARS,
        application_args=["--run-date-start", "{{ macros.ds_add(ds, -1) }}", "--run-date-end", "{{ macros.ds_add(ds, -1) }}"],
        execution_timeout=timedelta(hours=2),
    )

    gold_batch = SparkSubmitOperator(
        task_id="gold_batch",
        application=f"{PROJECT_DIR}/jobs/processed_to_campaign_summary.py",
        conn_id="spark_default",
        conf=SPARK_CONF,
        env_vars=_ENV_VARS,
        application_args=["--run-date-end", "{{ macros.ds_add(ds, -1) }}"],
        execution_timeout=timedelta(hours=1),
    )

    # ── Silver 검증 ─────────────────────────────────────────────────────────
    # silver_batch가 성공했어도 Iceberg 커밋이 없는 조용한 실패를 잡아냄
    verify_silver_snapshot = make_athena_gate_task(
        "verify_silver_snapshot",
        """\
SELECT 'silver_snapshot_missing' AS alert
FROM silver."processed_events$snapshots"
HAVING MAX(committed_at) < current_date""",
    )

    # ds-1(처리 대상 날짜) 파티션에 데이터가 0건이면 Gold로 진입 차단
    verify_silver_rows = make_athena_gate_task(
        "verify_silver_rows",
        """\
SELECT COUNT(*) AS silver_rows
FROM silver.processed_events
WHERE event_date = DATE '{ds}'
HAVING COUNT(*) = 0""",
    )

    # ── Gold 검증 ────────────────────────────────────────────────────────────
    verify_gold_snapshot = make_athena_gate_task(
        "verify_gold_snapshot",
        """\
SELECT 'gold_snapshot_missing' AS alert
FROM gold."campaign_summary$snapshots"
HAVING MAX(committed_at) < current_date""",
        database="gold",
    )

    # Silver COUNT(*) vs Gold SUM(impressions) 차이가 10% 초과하거나
    # Gold 파티션 자체가 없으면 집계 이상 — CROSS JOIN 후 NULL 조건 명시
    verify_silver_gold_consistency = make_athena_gate_task(
        "verify_silver_gold_consistency",
        """\
SELECT
    s.silver_rows,
    COALESCE(g.gold_impressions, 0)  AS gold_impressions,
    CASE
        WHEN g.gold_impressions IS NULL THEN 100.0
        ELSE ROUND(
            ABS(CAST(s.silver_rows - g.gold_impressions AS DOUBLE))
            / NULLIF(s.silver_rows, 0) * 100,
            2
        )
    END                              AS diff_pct
FROM (
    SELECT COUNT(*) AS silver_rows
    FROM silver.processed_events
    WHERE event_date = DATE '{ds}'
) s
CROSS JOIN (
    SELECT SUM(impressions) AS gold_impressions
    FROM gold.campaign_summary
    WHERE summary_date = DATE '{ds}'
) g
WHERE g.gold_impressions IS NULL
   OR ABS(CAST(s.silver_rows - g.gold_impressions AS DOUBLE))
      / NULLIF(s.silver_rows, 0) > 0.1""",
    )

    # ── 의존 관계 ────────────────────────────────────────────────────────────
    # Bronze → Silver → [snapshot 확인, 행수 확인] → Gold → [snapshot 확인, 일관성 확인]
    check_bronze_impressions >> silver_batch
    silver_batch >> [verify_silver_snapshot, verify_silver_rows]
    [verify_silver_snapshot, verify_silver_rows] >> gold_batch
    gold_batch >> [verify_gold_snapshot, verify_silver_gold_consistency]
