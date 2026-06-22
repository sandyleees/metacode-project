"""
Iceberg 월간 유지보수 DAG — manifest 재편성 (rewrite_manifests)

[실행 주기: 매월 1일 02:00]
daily maintenance DAG(criteo_iceberg_maintenance)는 데이터 파일·스냅샷·고아 파일을
매일 정리하지만, manifest list 자체의 장기 누적은 막지 못한다.
이 DAG이 월 1회 manifest를 재편성한다.

[현재 규모 기준 주기 판단]
Silver/Gold 모두 일(日) 단위 파티션이므로 manifest list는 하루 ~1개 증가.
1년 누적 ~330개, 3년 ~1,100개 — Iceberg는 수백만 개 설계라 즉각적 장애는 없음.
단, 범위 스캔 시 manifest 파일을 열어 통계를 확인하는 횟수가 늘어 쿼리 플래닝 시간이
누적 증가하므로 월 1회 정리.

데이터 규모 10배 이상 증가 또는 파티션 단위를 시간(hourly)으로 세분화하면
manifest 증가 속도가 하루 24개+로 가속 → weekly 또는 daily로 주기 단축 필요.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor

from dag_config import spark_env_vars
from dag_utils import make_sql_task

_ENV_VARS = spark_env_vars()

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="criteo_iceberg_maintenance_monthly",
    description="Iceberg manifest 재편성 — 월간 (매월 1일)",
    schedule="0 2 1 * *",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["criteo", "iceberg", "maintenance", "monthly"],
) as dag:

    # 두 DAG 모두 schedule="0 2 1 * *" 동일 시각 — 같은 logical_date로 매칭
    # rewrite_manifests는 새 스냅샷 생성 → MERGE·compaction과 겹치면 낙관적 잠금 충돌
    wait_for_daily_maintenance = ExternalTaskSensor(
        task_id="wait_for_daily_maintenance",
        external_dag_id="criteo_iceberg_maintenance",
        external_task_id="maintenance_done",
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        mode="reschedule",
        poke_interval=300,
        timeout=7200,
    )

    # compact_silver(daily)는 최근 35일만 대상 → window 밖 파티션 manifest가 매일 1개씩 누적
    # rewrite_manifests는 현재 스냅샷의 manifest list 전체를 소수의 큰 파일로 재편성
    # data file은 건드리지 않으므로 compaction보다 빠르고 가볍다
    rewrite_manifests_silver = make_sql_task(
        "rewrite_silver_manifests",
        "CALL glue_catalog.system.rewrite_manifests("
        "table => 'silver.processed_events')",
        _ENV_VARS,
    )

    # Gold는 compact_silver 같은 부수 정리 효과 없음 → Silver보다 약간 빠르게 누적
    rewrite_manifests_gold = make_sql_task(
        "rewrite_gold_manifests",
        "CALL glue_catalog.system.rewrite_manifests("
        "table => 'gold.campaign_summary')",
        _ENV_VARS,
    )

    # Silver와 Gold는 서로 독립 — 병렬 실행
    wait_for_daily_maintenance >> [rewrite_manifests_silver, rewrite_manifests_gold]
