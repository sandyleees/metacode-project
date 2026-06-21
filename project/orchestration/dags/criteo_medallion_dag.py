"""
Criteo Medallion 일배치 DAG
  Silver: raw_to_processed_iceberg.py   (Bronze raw → processed_events Iceberg)
  Gold  : processed_to_campaign_summary.py (processed_events → campaign_summary Iceberg)

Bronze(kafka_to_raw.py)는 Spark Structured Streaming — 장시간 실행 서비스이므로
Airflow가 직접 트리거하지 않고 docker-compose restart:on-failure 로 별도 관리.
이 DAG는 Bronze 파티션 존재를 확인한 뒤 Silver → Gold 순서를 보장한다.

스케줄 타이밍:
  UTC 02:00 실행 → {{ ds }} = 전날 날짜 (= 처리 대상 data interval start)
  예) 2026-06-19 02:00 실행 → {{ ds }} = 2026-06-18 → Bronze raw_date=2026-06-18 처리

파라미터 주입:
  일배치: --run-date-end {{ ds }}  (Silver/Gold 기본 동작 그대로)
  백필  : DAG Run Conf { "run_date_start": "2026-06-01", "run_date_end": "2026-06-07" }
          → Silver/Gold 명시 재처리 모드 전환

멱등성:
  Silver MERGE ON (event_id, event_date) — 동일 날짜 재실행 안전
  Gold   MERGE ON (summary_date, campaign) — 동일 날짜 재실행 안전
  max_active_runs=1 — 동시 MERGE 충돌 차단
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# ── 환경 설정 ────────────────────────────────────────────────────────────────

S3_RAW_BUCKET = os.environ.get("S3_RAW_BUCKET", "metacode-criteo-project")

# Airflow 컨테이너에 마운트된 프로젝트 루트 경로
# KubernetesPodOperator / ECSOperator 사용 시 이 경로는 컨테이너 마운트 경로로 대체
PROJECT_DIR = "/home/sandy/metacode-project/project"

SPARK_CONF = {"spark.cores.max": "2", "spark.executor.memory": "1g"}

# SparkSubmitOperator는 서브프로세스로 spark-submit을 실행하므로
# Airflow 컨테이너의 환경변수가 자동 상속되지 않음 — env_vars로 명시 전달 필요
SPARK_ENV_VARS = {
    "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID", ""),
    "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
    "AWS_DEFAULT_REGION": os.environ.get("AWS_DEFAULT_REGION", "ap-northeast-2"),
    "AWS_REGION": os.environ.get("AWS_DEFAULT_REGION", "ap-northeast-2"),
    "S3_RAW_BASE": f"s3a://{S3_RAW_BUCKET}/raw",
    "GLUE_WAREHOUSE": f"s3a://{S3_RAW_BUCKET}/warehouse",
    "PYSPARK_PYTHON": "python3",
    "PYSPARK_DRIVER_PYTHON": "python3",
}

# ── 공통 기본값 ───────────────────────────────────────────────────────────────

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    # 태스크 하나가 2시간 넘으면 이상 상황으로 판단
    "execution_timeout": timedelta(hours=2),
    # TODO [Alert]: 이메일 알람 — Airflow SMTP 연동 후 아래 두 줄 활성화
    # "email": ["data-team@company.com"],
    # "email_on_failure": True,
    # TODO [Alert]: Slack 알람 — on_failure_callback=slack_alert 함수 구현 후 여기 추가
    # "on_failure_callback": slack_alert,
}

# ── DAG 정의 ─────────────────────────────────────────────────────────────────

with DAG(
    dag_id="criteo_medallion_daily",
    description="Bronze 확인 → Silver → Gold 일배치 (Iceberg Medallion)",
    # UTC 02:00 실행: Bronze Streaming이 전날 데이터를 충분히 적재한 시점 이후
    schedule="0 2 * * *",
    start_date=datetime(2026, 6, 1),
    # catchup=False: 과거 missed run 자동 실행 안 함. 백필은 DAG Conf run_date_start/end로 수동 지정
    catchup=False,
    max_active_runs=1,  # Silver/Gold MERGE 동시 실행 방지
    default_args=default_args,
    tags=["criteo", "iceberg", "medallion"],
    # DAG Run Conf로 백필 날짜 범위 주입 가능
    # 예) {"run_date_start": "2026-06-01", "run_date_end": "2026-06-07"}
    params={
        "run_date_start": None,  # 미지정 시 일배치 모드
    },
) as dag:

    # ── Task 1: Bronze 파티션 존재 확인 ──────────────────────────────────────
    # Bronze는 Streaming 서비스 — Airflow가 완료 신호를 받을 수 없으므로
    # S3 파티션에 실제 파일이 있는지 확인해 Silver가 빈 배치로 실행되는 것을 막는다.
    # poke_interval=300s: 5분마다 확인, timeout=3600s: 1시간 내 데이터 없으면 실패
    check_bronze_impressions = S3KeySensor(
        task_id="check_bronze_impressions",
        bucket_name=S3_RAW_BUCKET,
        # impression 파티션 임의 파일 존재 확인 (wildcard 지원)
        bucket_key="raw/impressions/raw_date={{ ds }}/raw_hour=*/part-*.parquet",
        wildcard_match=True,
        aws_conn_id="aws_default",
        poke_interval=300,   # 5분마다 S3에 파일 존재 확인 (poke = 찌르다)
        timeout=3600,        # 1시간 내 파일 없으면 Task FAILED → Bronze 장애 의심
        mode="reschedule",   # 대기 중 Worker 슬롯 반납 (mode="poke"면 슬롯 점유한 채 sleep)
        soft_fail=False,
        # TODO [Alert]: timeout 초과 시 Bronze Streaming 컨테이너 상태 확인 알람
        # on_failure_callback=slack_alert  (Bronze가 죽었을 가능성 높음)
    )

    # ── Task 2: Silver 배치 ───────────────────────────────────────────────────
    # --run-date-end {{ ds }}: Airflow execution date = 처리 대상 날짜 (전날)
    # DAG Conf run_date_start 지정 시 명시 재처리 모드 전환
    # TODO [Backfill]: run_date_start 백필 지원 — PythonOperator로 application_args 동적 생성 후
    #   SparkSubmitOperator에 전달하거나, trigger_dag_id로 별도 백필 DAG 분리
    silver_batch = SparkSubmitOperator(
        task_id="silver_batch",
        application=f"{PROJECT_DIR}/jobs/raw_to_processed_iceberg.py",
        conn_id="spark_default",
        conf=SPARK_CONF,
        env_vars=SPARK_ENV_VARS,
        application_args=["--run-date-start", "{{ ds }}", "--run-date-end", "{{ ds }}"],
        execution_timeout=timedelta(hours=2),
        # TODO [Alert]: Stage 2 RuntimeError(impression-click 0 매칭) 발생 시
        # exit code != 0 → retries=2 소진 후 Task FAILED.
        # on_failure_callback=slack_alert 으로 "Silver attribution 이상 감지" 알람 추가
    )

    # ── Task 3: Gold 배치 ─────────────────────────────────────────────────────
    # Silver 완료 이후 실행 — Airflow 의존성으로 보장
    #
    # 일배치 모드 (--run-date-start 미지정):
    #   get_changed_event_dates()가 오늘 Silver 커밋 diff를 읽어 변경된 event_date만 재집계.
    #   "오늘 Silver에 커밋된 것 = 방금 완료된 silver_batch Task"이므로 타이밍 보장됨.
    #
    # 명시 재처리 모드 (--run-date-start 지정):
    #   Silver 현재 상태 기준으로 지정 범위 재집계 — snapshot diff 불필요.
    gold_batch = SparkSubmitOperator(
        task_id="gold_batch",
        application=f"{PROJECT_DIR}/jobs/processed_to_campaign_summary.py",
        conn_id="spark_default",
        conf=SPARK_CONF,
        env_vars=SPARK_ENV_VARS,
        application_args=["--run-date-start", "{{ ds }}", "--run-date-end", "{{ ds }}"],
        execution_timeout=timedelta(hours=1),
        # TODO [Alert]: Gold MERGE 실패 시 KPI 대시보드 데이터 미갱신 상태
        # Silver는 성공했으므로 Gold 단독 재처리로 복구 가능
        # on_failure_callback=slack_alert 으로 "Gold KPI 집계 실패" 알람 추가
    )

    # ── 의존성 체인 ───────────────────────────────────────────────────────────
    # Bronze 데이터 확인 → Silver MERGE → Gold snapshot diff
    check_bronze_impressions >> silver_batch >> gold_batch
