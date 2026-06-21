"""
Iceberg 월간 유지보수 DAG — manifest 재편성 (rewrite_manifests)

[실행 주기: 매월 1일 04:00]
daily maintenance DAG(criteo_iceberg_maintenance)는 데이터 파일·스냅샷·고아 파일을
매일 정리하지만, manifest list 자체의 장기 누적은 막지 못한다.
이 DAG이 월 1회 manifest를 재편성한다.

[현재 규모 기준 주기 판단]
Silver/Gold 모두 일(日) 단위 파티션이므로 manifest list는 하루 ~1개 증가.
1년 누적 ~330개, 3년 ~1,100개 — Iceberg는 수백만 개 설계라 즉각적 장애는 없음.
단, 범위 스캔(Superset 다중 날짜 집계, health-queries 전체 집계) 시 manifest 파일을
열어 통계를 확인하는 횟수가 늘어 쿼리 플래닝 시간이 누적 증가하므로 월 1회 정리.

데이터 규모 10배 이상 증가 또는 파티션 단위를 시간(hourly)으로 세분화하면
manifest 증가 속도가 하루 24개+ 로 가속 → weekly 또는 daily로 주기 단축 필요.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor

COMPOSE_DIR = os.environ.get("COMPOSE_DIR", "/home/sandy/metacode-project/project")

SPARK_SUBMIT_BASE = (
    "/opt/spark/bin/spark-submit "
    "--master spark://spark-master:7077 "
    "--conf spark.cores.max=2 "
)

RUN_SQL = f"{SPARK_SUBMIT_BASE} /app/analytics/run_sql.py"

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="criteo_iceberg_maintenance_monthly",
    description="Iceberg manifest 재편성 — 월간 (매월 1일)",
    schedule="0 2 1 * *",   # 매월 1일 02:00 — daily maintenance와 동일 시각, ExternalTaskSensor가 완료 보장
    start_date=datetime(2026, 7, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["criteo", "iceberg", "maintenance", "monthly"],
) as dag:

    # ── Task 0: daily maintenance 완료 대기 ──────────────────────────────────
    # monthly DAG(04:00)는 daily maintenance DAG(02:00)가 끝난 뒤 실행해야 한다.
    # rewrite_manifests는 새 스냅샷을 생성하므로, 진행 중인 MERGE·compaction과
    # 겹치면 Iceberg 낙관적 잠금 충돌이 발생한다.
    # execution_date_fn: monthly DAG의 실행 시각(04:00)을 daily DAG 실행 시각(02:00)으로 맞춤
    wait_for_daily_maintenance = ExternalTaskSensor(
        task_id="wait_for_daily_maintenance",
        external_dag_id="criteo_iceberg_maintenance",
        external_task_id="maintenance_done",   # Silver/Gold 두 chain이 모두 완료된 합류 지점
        execution_date_fn=lambda dt: dt.replace(hour=2, minute=0, second=0, microsecond=0),
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        mode="reschedule",
        poke_interval=300,
        timeout=7200,
    )

    def _make_sql_task(task_id: str, sql: str) -> BashOperator:
        return BashOperator(
            task_id=task_id,
            bash_command=(
                f"cd {COMPOSE_DIR} && "
                "docker compose --profile batch run --rm raw-to-processed "
                f'{RUN_SQL} --sql "{sql}"'
            ),
        )

    # ── Silver manifest 재편성 ────────────────────────────────────────────────
    # [누적 원인]
    # compact_silver(daily DAG)는 최근 35일 파티션만 대상으로 한다.
    # 35일 window 밖으로 밀린 파티션은 이후 MERGE 대상이 아니므로
    # manifest가 갱신되지 않고 manifest list에 1개씩 고정 잔류.
    # compact_silver가 최근 35일을 manifest 1개로 통합하는 부수효과가 있지만
    # window 밖 파티션은 하루 1개씩 증가해 1년 ~330개 누적.
    #
    # [이 작업이 하는 일]
    # 현재 스냅샷의 manifest list 전체(수백 개)를 소수의 큰 manifest file로 재편성.
    # data file은 건드리지 않으므로 compaction보다 빠르고 가볍다.
    rewrite_manifests_silver = _make_sql_task(
        "rewrite_silver_manifests",
        "CALL glue_catalog.system.rewrite_manifests("
        "table => 'silver.processed_events')",
    )

    # ── Gold manifest 재편성 ──────────────────────────────────────────────────
    # [누적 원인]
    # Gold는 COW MERGE가 파티션당 파일 1개로 자체 유지하므로 data compaction이 없다.
    # attribution window(30일) 안에서 매일 MERGE가 일어나 manifest가 갱신되지만
    # window 밖 파티션은 갱신 중단 → manifest list에 고정 잔류.
    # compact_silver 같은 부수 정리 효과도 없어 Silver보다 약간 빠르게 누적.
    #
    # [이 작업이 하는 일]
    # Silver와 동일: manifest list 재편성만 수행, data file 변경 없음.
    rewrite_manifests_gold = _make_sql_task(
        "rewrite_gold_manifests",
        "CALL glue_catalog.system.rewrite_manifests("
        "table => 'gold.campaign_summary')",
    )

    # ── 의존성 ────────────────────────────────────────────────────────────────
    # Silver와 Gold는 서로 독립 — 병렬 실행
    wait_for_daily_maintenance >> [rewrite_manifests_silver, rewrite_manifests_gold]
