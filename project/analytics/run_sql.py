"""범용 Iceberg/Glue ad-hoc SQL 실행기.

health-queries/*.sql 실행, snapshot/history 메타테이블 조회,
time travel(VERSION AS OF / TIMESTAMP AS OF), CALL system.* 프로시저(rollback_to_snapshot,
rewrite_data_files, expire_snapshots 등)를 운영 배치 스크립트 재빌드 없이 즉석 실행한다.

사용 예:
  spark-submit run_sql.py --sql "SELECT * FROM glue_catalog.gold.campaign_summary.history"
  spark-submit run_sql.py --sql-file health-queries/01_snapshot_freshness.sql
  spark-submit run_sql.py --sql "CALL glue_catalog.system.rollback_to_snapshot('gold.campaign_summary', 123456789)"

용도 범위:
  이 스크립트는 로컬 ad-hoc 조회 전용이다. criteo_maintenance_dag.py가 현재 이 스크립트를
  SparkSubmitOperator로 호출하고 있으나, task마다 JVM이 새로 기동되는 구조(~45s × task 수)다.
  유지보수 task 수 증가 또는 기동 오버헤드가 문제가 될 경우 jobs/iceberg_maintenance.py
  (전용 스크립트, SparkSession 1회)로 교체 검토 — 이 시점에 이 스크립트는 ad-hoc 전용으로 복귀.
"""

import argparse
import logging
import os
from typing import List

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

CATALOG = "glue_catalog"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Iceberg/Glue ad-hoc SQL 실행기")
    parser.add_argument(
        "--glue-warehouse",
        default=os.environ.get("GLUE_WAREHOUSE", "s3a://your-bucket/warehouse"),
        help="Iceberg warehouse 경로",
    )
    parser.add_argument("--sql", help="실행할 단일 SQL/CALL 문")
    parser.add_argument(
        "--sql-file",
        help="세미콜론(;)으로 구분된 다중 SQL 문 파일 경로 (-- 로 시작하는 줄은 주석으로 무시)",
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=100,
        help="결과 출력 행 수 (기본값: 100)",
    )
    parser.add_argument(
        "--no-truncate",
        action="store_true",
        default=False,
        help="지정 시 컬럼 값을 자르지 않고 전체 출력",
    )
    return parser.parse_args()


def build_spark(glue_warehouse: str) -> SparkSession:
    # jobs/spark_utils.build_spark()와 동일 설정 — 크로스 디렉토리 임포트 제약으로 별도 유지
    aws_key = os.environ.get("AWS_ACCESS_KEY_ID", "")
    aws_secret = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
    aws_region = os.environ.get("AWS_DEFAULT_REGION", "ap-northeast-2")

    logger.info("SparkSession 생성: catalog=%s", CATALOG)

    return (
        SparkSession.builder
        .appName("AdHocIcebergSQL")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.catalog-impl",
                "org.apache.iceberg.aws.glue.GlueCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", glue_warehouse)
        .config(f"spark.sql.catalog.{CATALOG}.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{CATALOG}.client.region", aws_region)
        .config(f"spark.sql.catalog.{CATALOG}.client.access-key-id", aws_key)
        .config(f"spark.sql.catalog.{CATALOG}.client.secret-access-key", aws_secret)
        .config("spark.executorEnv.AWS_ACCESS_KEY_ID", aws_key)
        .config("spark.executorEnv.AWS_SECRET_ACCESS_KEY", aws_secret)
        .config("spark.executorEnv.AWS_DEFAULT_REGION", aws_region)
        .config("spark.executorEnv.AWS_REGION", aws_region)
        .config("spark.hadoop.fs.s3a.access.key", aws_key)
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret)
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        .getOrCreate()
    )


def split_statements(text: str) -> List[str]:
    # 문자열 리터럴 내부 세미콜론은 분리 안 함 — ad-hoc 용도 한정
    lines = [ln for ln in text.splitlines() if not ln.strip().startswith("--")]
    return [s.strip() for s in "\n".join(lines).split(";") if s.strip()]


def run_statement(spark: SparkSession, stmt: str, rows: int, truncate: bool) -> None:
    print(f"\n{'=' * 100}\n-- SQL\n{stmt}\n{'-' * 100}")
    df = spark.sql(stmt)
    try:
        df.show(rows, truncate=truncate)
    except Exception as e:
        # CALL 프로시저 등 show() 불가한 결과 — collect로 출력 시도
        logger.debug("show() 실패 (%s), collect로 재시도", e)
        for row in df.collect():
            print(row)


def main() -> None:
    args = parse_args()

    if not args.sql and not args.sql_file:
        raise SystemExit("--sql 또는 --sql-file 중 하나는 필수")

    spark = build_spark(args.glue_warehouse)

    if args.sql:
        statements = [args.sql]
    else:
        with open(args.sql_file, encoding="utf-8") as f:
            statements = split_statements(f.read())

    for stmt in statements:
        run_statement(spark, stmt, args.rows, not args.no_truncate)

    spark.stop()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    main()
