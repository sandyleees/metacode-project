"""DAG 공통 유틸리티 — SparkSubmitOperator / Athena 검증 게이트 팩토리."""
from __future__ import annotations

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from dag_config import ATHENA_RESULTS_S3, PROJECT_DIR, SPARK_CONF

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


def make_athena_gate_task(
    task_id: str,
    sql_template: str,
    database: str = "silver",
) -> "PythonOperator":
    """0행=정상 구조의 Athena 검증 게이트를 PythonOperator로 생성한다.

    sql_template 내 {ds}는 ds-1로 치환된다. silver_batch가 --run-date-start=ds-1로
    실행되므로 Silver event_date=ds-1, Gold summary_date=ds-1이 검증 대상 파티션이다.
    쿼리 결과가 1행 이상이면 AirflowException — Gold batch 진입 차단 또는 경보 발생.
    """
    from airflow.operators.python import PythonOperator

    def _run(**context: object) -> None:
        from datetime import datetime, timedelta
        run_date = (datetime.strptime(str(context["ds"]), "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
        sql = sql_template.replace("{ds}", run_date)
        _athena_gate(task_id=task_id, sql=sql, database=database)

    return PythonOperator(task_id=task_id, python_callable=_run)


def _athena_gate(task_id: str, sql: str, database: str) -> None:
    """Athena 쿼리를 실행하고 결과 행이 있으면 AirflowException을 발생시킨다."""
    from airflow.exceptions import AirflowException
    from airflow.providers.amazon.aws.hooks.athena import AthenaHook

    hook = AthenaHook(aws_conn_id="aws_default")
    execution_id = hook.run_query(
        query=sql,
        query_context={"Database": database},
        result_configuration={"OutputLocation": ATHENA_RESULTS_S3},
    )
    final_state = hook.poll_query_status(execution_id)
    if final_state != "SUCCEEDED":
        raise AirflowException(
            f"[{task_id}] Athena 쿼리 실패 (state={final_state})"
        )
    response = hook.get_query_results(execution_id)
    if not response:
        raise AirflowException(
            f"[{task_id}] Athena 결과 조회 실패 (execution_id={execution_id})"
        )
    data_rows = response["ResultSet"]["Rows"][1:]  # 첫 행은 컬럼 헤더
    if data_rows:
        raise AirflowException(
            f"[{task_id}] 검증 실패 — 이상 데이터 감지: {data_rows}"
        )
