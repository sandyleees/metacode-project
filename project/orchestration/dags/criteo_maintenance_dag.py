"""
Iceberg 유지보수 DAG — Compaction / Expire Snapshots / Orphan 제거

ExternalTaskSensor로 criteo_medallion_daily의 gold_batch Task SUCCESS를 확인한 뒤
Compaction을 시작한다.

MERGE 진행 중 Compaction이 겹치면 Iceberg 낙관적 잠금 충돌이 발생하므로
medallion DAG 완료 보장은 필수다.

백필 진행 중에는 expire_snapshots older_than 기간을 연장해야 time-travel 롤백이 가능하다.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.utils.trigger_rule import TriggerRule

from dag_config import spark_env_vars
from dag_utils import make_sql_task

_ENV_VARS = spark_env_vars()

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=1),
}

with DAG(
    dag_id="criteo_iceberg_maintenance",
    description="Iceberg Compaction / Expire Snapshots / Orphan 제거",
    schedule="0 2 * * *",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["criteo", "iceberg", "maintenance"],
) as dag:

    # Silver/Gold MERGE가 완전히 끝난 뒤 Compaction 시작 — Iceberg 낙관적 잠금 충돌 방지
    wait_for_medallion = ExternalTaskSensor(
        task_id="wait_for_medallion_gold",
        external_dag_id="criteo_medallion_daily",
        external_task_id="gold_batch",
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        mode="reschedule",
        poke_interval=120,
        timeout=7200,
    )

    # MOR delete file을 data file에 흡수 → 읽기 시 delta merge 오버헤드 제거
    # where 35일: attribution window(30일) + Bronze 수집 지연 여유(5일)
    compact_silver = make_sql_task(
        "compact_silver",
        "CALL glue_catalog.system.rewrite_data_files("
        "table => 'silver.processed_events', "
        "where => 'event_date >= DATE_SUB(CURRENT_DATE(), 35)', "
        "options => map('target-file-size-bytes', '134217728'))",
        _ENV_VARS,
    )

    # 스냅샷 엔트리 + manifest list + manifest file 제거
    # 백필 진행 중에는 older_than을 연장할 것
    expire_silver = make_sql_task(
        "expire_silver_snapshots",
        "CALL glue_catalog.system.expire_snapshots("
        "table => 'silver.processed_events', "
        "older_than => TIMESTAMP '{{ macros.ds_add(ds, -31) }} 00:00:00')",
        _ENV_VARS,
    )

    # Compaction이 교체한 구 data/delete file, expire가 참조 끊은 파일 삭제
    orphan_silver = make_sql_task(
        "remove_silver_orphans",
        "CALL glue_catalog.system.remove_orphan_files("
        "table => 'silver.processed_events', "
        "older_than => TIMESTAMP '{{ macros.ds_add(ds, -4) }} 00:00:00')",
        _ENV_VARS,
    )

    # Gold는 COW MERGE가 파티션당 파일 1개로 재작성 → Compaction 불필요, Expire만 실행
    expire_gold = make_sql_task(
        "expire_gold_snapshots",
        "CALL glue_catalog.system.expire_snapshots("
        "table => 'gold.campaign_summary', "
        "older_than => TIMESTAMP '{{ macros.ds_add(ds, -31) }} 00:00:00')",
        _ENV_VARS,
    )

    # COW MERGE마다 교체된 구 data file → expire 후 실제 삭제
    orphan_gold = make_sql_task(
        "remove_gold_orphans",
        "CALL glue_catalog.system.remove_orphan_files("
        "table => 'gold.campaign_summary', "
        "older_than => TIMESTAMP '{{ macros.ds_add(ds, -4) }} 00:00:00')",
        _ENV_VARS,
    )

    # ExternalTaskSensor는 단일 task_id만 지원 — Silver/Gold 두 chain 합류 지점
    maintenance_done = EmptyOperator(
        task_id="maintenance_done",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # Silver: compact → expire → orphan  (MOR delete file 매일 누적)
    # Gold  : expire → orphan            (COW MERGE가 파일 1개 유지, compact 불필요)
    wait_for_medallion >> compact_silver >> expire_silver >> orphan_silver
    wait_for_medallion >> expire_gold >> orphan_gold
    [orphan_silver, orphan_gold] >> maintenance_done
