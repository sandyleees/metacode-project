"""
Criteo Medallion 일배치 DAG
  Silver: raw_to_processed_iceberg.py   (Bronze raw → processed_events Iceberg)
  Gold  : processed_to_campaign_summary.py (processed_events → campaign_summary Iceberg)

Bronze(kafka_to_raw.py)는 Spark Structured Streaming — 장시간 실행 서비스이므로
Airflow가 직접 트리거하지 않고 docker-compose restart:on-failure 로 별도 관리.
이 DAG는 Bronze 파티션 존재를 확인한 뒤 Silver → Gold 순서를 보장한다.

스케줄 타이밍:
  UTC 02:00 실행 → {{ ds }} = 전날 날짜
  예) 2026-06-19 02:00 실행 → {{ ds }} = 2026-06-18 → Bronze raw_date=2026-06-18 처리

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
        bucket_key="raw/impressions/raw_date={{ ds }}/raw_hour=*/part-*.parquet",
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
        application_args=["--run-date-start", "{{ ds }}", "--run-date-end", "{{ ds }}"],
        execution_timeout=timedelta(hours=2),
    )

    gold_batch = SparkSubmitOperator(
        task_id="gold_batch",
        application=f"{PROJECT_DIR}/jobs/processed_to_campaign_summary.py",
        conn_id="spark_default",
        conf=SPARK_CONF,
        env_vars=_ENV_VARS,
        application_args=["--run-date-end", "{{ ds }}"],
        execution_timeout=timedelta(hours=1),
    )

    check_bronze_impressions >> silver_batch >> gold_batch
