"""DAG 공통 유틸리티 — SparkSubmitOperator 팩토리."""
from __future__ import annotations

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from dag_config import PROJECT_DIR, SPARK_CONF

# task마다 JVM 기동 ~45s 오버헤드 — task 수 증가 시 jobs/iceberg_maintenance.py 교체 검토
RUN_SQL_APP = f"{PROJECT_DIR}/analytics/run_sql.py"


def make_sql_task(task_id: str, sql: str, env_vars: dict[str, str]) -> SparkSubmitOperator:
    return SparkSubmitOperator(
        task_id=task_id,
        application=RUN_SQL_APP,
        conn_id="spark_default",
        conf=SPARK_CONF,
        env_vars=env_vars,
        application_args=["--sql", sql],
    )
