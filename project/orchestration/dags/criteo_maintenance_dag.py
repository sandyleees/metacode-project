"""
Iceberg 유지보수 DAG — Compaction / Expire Snapshots / Orphan 제거

ExternalTaskSensor로 criteo_medallion_daily의 gold_batch Task SUCCESS를 확인한 뒤
Compaction을 시작한다. 시간 기반 스케줄(03:00)이 아니라 medallion 완료를 실제로 기다린다.

MERGE 진행 중 Compaction이 겹치면 Iceberg 낙관적 잠금 충돌이 발생하므로
medallion DAG 완료 보장은 필수다.

Silver/Gold 테이블의 파일 단편화 해소 및 스냅샷 정리.
백필 진행 중에는 expire_snapshots older_than 기간을 연장해야 time-travel 롤백이 가능.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

COMPOSE_DIR = os.environ.get("COMPOSE_DIR", "/home/sandy/metacode-project/project")

SPARK_SUBMIT_BASE = (
    "/opt/spark/bin/spark-submit "
    "--master spark://spark-master:7077 "
    "--conf spark.cores.max=2 "
)

# run_sql.py를 재사용해 Iceberg CALL PROCEDURE 실행
RUN_SQL = f"{SPARK_SUBMIT_BASE} /app/run_sql.py"

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=1),
}

with DAG(
    dag_id="criteo_iceberg_maintenance",
    description="Iceberg Compaction / Expire Snapshots / Orphan 제거",
    # medallion DAG와 동일한 스케줄 — ExternalTaskSensor가 실제 완료를 기다림
    # 03:00으로 잡으면 medallion이 02:00~03:00 사이에 끝나지 않아도 센서가 계속 대기
    schedule="0 2 * * *",
    start_date=datetime(2026, 6, 1),
    catchup=False,           # 유지보수는 최신 실행만 의미 있음
    max_active_runs=1,
    default_args=default_args,
    tags=["criteo", "iceberg", "maintenance"],
) as dag:

    # ── Task 0: medallion DAG gold_batch 완료 대기 ───────────────────────────
    # 같은 execution_date의 criteo_medallion_daily >> gold_batch가 SUCCESS일 때만 진행.
    # Silver/Gold MERGE가 완전히 끝난 뒤 Compaction을 시작해 충돌을 방지한다.
    # timeout=7200: 2시간 내 medallion이 끝나지 않으면 이 DAG도 실패 처리
    wait_for_medallion = ExternalTaskSensor(
        task_id="wait_for_medallion_gold",
        external_dag_id="criteo_medallion_daily",
        external_task_id="gold_batch",      # 마지막 Task — 이게 SUCCESS면 전체 완료
        allowed_states=["success"],
        failed_states=["failed", "skipped"],  # medallion 실패 시 유지보수도 즉시 중단
        mode="reschedule",                  # Worker 슬롯 점유 방지
        poke_interval=120,                  # 2분마다 확인
        timeout=7200,                       # 최대 2시간 대기
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

    # ── Silver ───────────────────────────────────────────────────────────────

    # Silver Compaction: MOR delete file을 data file에 흡수 → 읽기 시 delta merge 오버헤드 제거
    # where: attribution window(30일) + Bronze 수집 지연 여유(5일) = 35일 이내 파티션만 대상
    #   → 35일 초과 파티션은 오늘 MERGE의 delete file 대상이 될 가능성이 극히 낮음
    #   → 전체 파티션 manifest 스캔을 최근 35일로 축소
    #   → 극단적 수집 지연(>5일)으로 누락된 파티션의 delete file은 드물고 성능 영향 미미
    compact_silver = _make_sql_task(
        "compact_silver",
        "CALL glue_catalog.system.rewrite_data_files("
        "table => 'silver.processed_events', "
        "where => 'event_date >= DATE_SUB(CURRENT_DATE(), 35)', "
        "options => map('target-file-size-bytes', '134217728'))",
    )

    # metadata.json 보존 기간 ≤ 스냅샷 보존 기간
    # Silver Expire Snapshots (30일 이전)
    # 스냅샷 엔트리 + manifest list + manifest file 제거
    # metadata.json 자체는 ensure_table의 write.metadata.previous-versions-max=10이 쓰기 시점에 정리
    # 주의: 백필 진행 중에는 이 태스크를 skip하거나 older_than을 연장할 것
    expire_silver = _make_sql_task(
        "expire_silver_snapshots",
        "CALL glue_catalog.system.expire_snapshots("
        "table => 'silver.processed_events', "
        "older_than => TIMESTAMP '{{ macros.ds_add(ds, -30) }} 00:00:00')",
    )

    # Silver Orphan 제거 (3일 이상 고아 파일)
    # Compaction이 새 파일로 교체한 구 data/delete file, expire가 참조 끊은 파일 삭제
    orphan_silver = _make_sql_task(
        "remove_silver_orphans",
        "CALL glue_catalog.system.remove_orphan_files("
        "table => 'silver.processed_events', "
        "older_than => TIMESTAMP '{{ macros.ds_add(ds, -3) }} 00:00:00')",
    )

    # ── Gold ─────────────────────────────────────────────────────────────────

    # Gold Expire Snapshots (30일 이전)
    # Gold는 하루 1 스냅샷 → 30일이면 30개 누적. expire는 메타데이터 연산만이므로 매일 실행 무방.
    # COW MERGE가 교체한 구 data file의 참조를 끊어 orphan 판별 가능하게 함.
    # compact_gold 없이도 독립 실행 가능 — expire는 compaction에 의존하지 않음.
    expire_gold = _make_sql_task(
        "expire_gold_snapshots",
        "CALL glue_catalog.system.expire_snapshots("
        "table => 'gold.campaign_summary', "
        "older_than => TIMESTAMP '{{ macros.ds_add(ds, -30) }} 00:00:00')",
    )

    # Gold Orphan 제거: COW MERGE마다 교체된 구 data file이 S3에 남음 → expire 후 실제 삭제
    orphan_gold = _make_sql_task(
        "remove_gold_orphans",
        "CALL glue_catalog.system.remove_orphan_files("
        "table => 'gold.campaign_summary', "
        "older_than => TIMESTAMP '{{ macros.ds_add(ds, -3) }} 00:00:00')",
    )

    # ── 의존성 ────────────────────────────────────────────────────────────────
    # Silver: compact(매일) → expire → orphan
    #   MOR delete file이 매일 누적 → Compaction 필수
    # Gold:   expire → orphan (매일, Compaction 없음)
    #   COW MERGE가 파티션당 파일 1개로 재작성 → 합칠 대상 없음 → Compaction 낭비
    # rewrite_manifests 생략: rewrite_data_files + expire_snapshots이 구 manifest를 정리함
    wait_for_medallion >> compact_silver >> expire_silver >> orphan_silver
    wait_for_medallion >> expire_gold >> orphan_gold
