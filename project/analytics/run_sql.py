"""
범용 Iceberg/Glue ad-hoc SQL 실행기.

목적: health-queries/*.sql 실행, snapshot/history 메타테이블 조회,
      time travel(VERSION AS OF / TIMESTAMP AS OF), CALL system.* 프로시저(rollback_to_snapshot,
      rewrite_data_files, expire_snapshots 등)를 운영 배치 스크립트 재빌드 없이 즉석 실행하기 위함.

build_spark()는 raw_to_processed_iceberg.py / processed_to_campaign_summary.py와 동일한
Iceberg + Glue Catalog 설정을 재사용한다 (상세 설명은 그쪽 주석 참고).

사용 예:
  spark-submit run_sql.py --sql "SELECT * FROM glue_catalog.gold.campaign_summary.history"
  spark-submit run_sql.py --sql-file health-queries/01_snapshot_freshness.sql
  spark-submit run_sql.py --sql "CALL glue_catalog.system.rollback_to_snapshot('gold.campaign_summary', 123456789)"
"""

import argparse
import os

from pyspark.sql import SparkSession


CATALOG = "glue_catalog"


def parse_args():
    parser = argparse.ArgumentParser(description="Iceberg/Glue ad-hoc SQL 실행기")
    parser.add_argument(
        "--glue-warehouse",
        default=os.environ.get("GLUE_WAREHOUSE", "s3a://your-bucket/warehouse"),
        help="Iceberg warehouse 경로",
    )
    parser.add_argument("--sql", help="실행할 단일 SQL/CALL 문")
    parser.add_argument(
        "--sql-file",
        help="세미콜론(;)으로 구분된 다중 SQL 문이 담긴 파일 경로 (-- 로 시작하는 줄은 주석으로 무시)",
    )
    parser.add_argument("--rows", type=int, default=100, help="결과 출력 행 수 (기본값: 100)")
    parser.add_argument(
        "--no-truncate", action="store_true", default=False,
        help="지정 시 컬럼 값을 자르지 않고 전체 출력",
    )
    return parser.parse_args()


def build_spark(glue_warehouse: str) -> SparkSession:
    # raw_to_processed_iceberg.py build_spark()와 동일한 설정 — 상세 주석은 그쪽 참고
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
        .config(f"spark.sql.catalog.{CATALOG}.client.region",
                os.environ.get("AWS_DEFAULT_REGION", "ap-northeast-2"))
        .config(f"spark.sql.catalog.{CATALOG}.client.access-key-id",
                os.environ.get("AWS_ACCESS_KEY_ID", ""))
        .config(f"spark.sql.catalog.{CATALOG}.client.secret-access-key",
                os.environ.get("AWS_SECRET_ACCESS_KEY", ""))
        .config("spark.executorEnv.AWS_ACCESS_KEY_ID",
                os.environ.get("AWS_ACCESS_KEY_ID", ""))
        .config("spark.executorEnv.AWS_SECRET_ACCESS_KEY",
                os.environ.get("AWS_SECRET_ACCESS_KEY", ""))
        .config("spark.executorEnv.AWS_DEFAULT_REGION",
                os.environ.get("AWS_DEFAULT_REGION", "ap-northeast-2"))
        .config("spark.executorEnv.AWS_REGION",
                os.environ.get("AWS_DEFAULT_REGION", "ap-northeast-2"))
        .config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID", ""))
        .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY", ""))
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        .getOrCreate()
    )


def split_statements(text: str):
    # health-queries/*.sql처럼 사람이 작성한 파일을 다루기 위한 단순 분리기
    # -- 주석 줄 제거 후 세미콜론 기준 분리 (문자열 리터럴 내부 세미콜론은 가정하지 않음 — ad-hoc 용도 한정)
    lines = [ln for ln in text.splitlines() if not ln.strip().startswith("--")]
    cleaned = "\n".join(lines)
    return [s.strip() for s in cleaned.split(";") if s.strip()]


def run_statement(spark: SparkSession, stmt: str, rows: int, truncate: bool) -> None:
    print(f"\n{'=' * 100}\n-- SQL\n{stmt}\n{'-' * 100}")
    df = spark.sql(stmt)
    try:
        df.show(rows, truncate=truncate)
    except Exception:
        # CALL 프로시저 등 show() 불가한 결과 — collect로 출력 시도
        for row in df.collect():
            print(row)


def main():
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
    main()
