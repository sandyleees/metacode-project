"""DAG 공통 설정 — 버킷·경로·Spark 설정을 한 곳에서 관리한다."""
from __future__ import annotations

import os

S3_RAW_BUCKET: str = os.environ.get("S3_RAW_BUCKET", "metacode-criteo-project")
PROJECT_DIR: str = "/home/sandy/metacode-project/project"
SPARK_CONF: dict[str, str] = {"spark.cores.max": "2", "spark.executor.memory": "1g"}


def spark_env_vars(*, include_raw_base: bool = False) -> dict[str, str]:
    # SparkSubmitOperator는 서브프로세스로 spark-submit 실행 — 컨테이너 환경변수가 자동 상속 안 됨
    region = os.environ.get("AWS_DEFAULT_REGION", "ap-northeast-2")
    env: dict[str, str] = {
        "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID", ""),
        "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
        "AWS_DEFAULT_REGION": region,
        "AWS_REGION": region,
        "GLUE_WAREHOUSE": f"s3a://{S3_RAW_BUCKET}/warehouse",
        "PYSPARK_PYTHON": "python3",
        "PYSPARK_DRIVER_PYTHON": "python3",
    }
    if include_raw_base:
        env["S3_RAW_BASE"] = f"s3a://{S3_RAW_BUCKET}/raw"
    return env
