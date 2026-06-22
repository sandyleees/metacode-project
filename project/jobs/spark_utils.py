"""Spark + Iceberg + Glue Catalog 공통 세션 빌더.

Driver/Executor 자격증명 분리 구조와 S3FileIO vs Hadoop S3A 차이는
JOBS_GUIDE.md §4 참고.
"""

import logging
import os

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def build_spark(app_name: str, catalog: str, glue_warehouse: str) -> SparkSession:
    """Iceberg + Glue Catalog SparkSession을 생성한다.

    Args:
        app_name: Spark UI / history server에 표시될 앱 이름
        catalog: Iceberg catalog 이름 (SQL 테이블 참조의 prefix로 사용됨)
        glue_warehouse: Iceberg 데이터 파일을 쓰는 S3 루트 경로

    Returns:
        설정이 완료된 SparkSession
    """
    aws_key = os.environ.get("AWS_ACCESS_KEY_ID", "")
    aws_secret = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
    aws_region = os.environ.get("AWS_DEFAULT_REGION", "ap-northeast-2")

    logger.info("SparkSession 생성: app=%s, catalog=%s", app_name, catalog)

    return (
        SparkSession.builder
        .appName(app_name)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog}.catalog-impl",
                "org.apache.iceberg.aws.glue.GlueCatalog")
        .config(f"spark.sql.catalog.{catalog}.warehouse", glue_warehouse)
        .config(f"spark.sql.catalog.{catalog}.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{catalog}.client.region", aws_region)
        .config(f"spark.sql.catalog.{catalog}.client.access-key-id", aws_key)
        .config(f"spark.sql.catalog.{catalog}.client.secret-access-key", aws_secret)
        # Executor JVM은 spark.sql.catalog.* 를 읽지 않으므로 환경변수로 별도 전달 (JOBS_GUIDE.md §4)
        .config("spark.executorEnv.AWS_ACCESS_KEY_ID", aws_key)
        .config("spark.executorEnv.AWS_SECRET_ACCESS_KEY", aws_secret)
        .config("spark.executorEnv.AWS_DEFAULT_REGION", aws_region)
        .config("spark.executorEnv.AWS_REGION", aws_region)
        .config("spark.hadoop.fs.s3a.access.key", aws_key)
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret)
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        .getOrCreate()
    )
