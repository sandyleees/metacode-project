"""S3 raw zone → processed_events Iceberg (Silver) 일단위 배치.

2-Stage 처리 구조, Attribution window 로직, MOR 선택 이유는 JOBS_GUIDE.md §2 참고.
"""

import argparse
import logging
import os
from datetime import date, timedelta
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import (
    coalesce, col, current_timestamp,
    hour, lit, row_number, to_date, to_timestamp, when,
)
from pyspark.sql.types import LongType

from spark_utils import build_spark, parse_sql

logger = logging.getLogger(__name__)

CATALOG = "glue_catalog"
DATABASE = "silver"
TABLE = "processed_events"
FULL_NAME = f"{CATALOG}.{DATABASE}.{TABLE}"

SECONDS_PER_DAY = 86_400


def parse_args() -> argparse.Namespace:
    """CLI 인자를 파싱하고 날짜 범위 유효성을 검사한다.

    Returns:
        파싱된 인자 Namespace

    Raises:
        SystemExit: --run-date-start > --run-date-end 인 경우
    """
    yesterday = (date.today() - timedelta(days=1)).isoformat()

    parser = argparse.ArgumentParser(
        description="S3 raw zone → processed_events Iceberg (Silver) 일단위 배치"
    )
    parser.add_argument(
        "--run-date-start",
        default=yesterday,
        help="처리 시작 날짜 (YYYY-MM-DD, 기본값: 어제)",
    )
    parser.add_argument(
        "--run-date-end",
        default=yesterday,
        help="처리 종료 날짜 (YYYY-MM-DD inclusive, 기본값: 어제)",
    )
    parser.add_argument(
        "--s3-raw-base",
        default=os.environ.get("S3_RAW_BASE", "s3a://your-bucket/raw"),
        help="S3 raw zone 기본 경로",
    )
    parser.add_argument(
        "--glue-warehouse",
        default=os.environ.get("GLUE_WAREHOUSE", "s3a://your-bucket/warehouse"),
        help="Iceberg warehouse S3 경로",
    )
    parser.add_argument(
        "--click-window-days",
        type=int,
        default=7,
        help="impression 기준 click attribution 최대 일수 (기본값: 7)",
    )
    parser.add_argument(
        "--conversion-window-days",
        type=int,
        default=30,
        help="impression 기준 conversion attribution 최대 일수 (기본값: 30)",
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        default=False,
        help="테이블 전체를 원자적으로 재구축 (기본값: False)",
    )

    args = parser.parse_args()

    if args.run_date_start > args.run_date_end:
        parser.error(
            f"--run-date-start ({args.run_date_start}) 가 "
            f"--run-date-end ({args.run_date_end}) 보다 늦습니다."
        )

    return args


def ensure_table(spark: SparkSession) -> None:
    """Silver 데이터베이스와 processed_events 테이블이 없으면 생성한다.

    스키마 정의는 jobs/ddl/silver_processed_events.sql 참고.
    """
    ddl_path = Path(__file__).parent / "ddl" / "silver_processed_events.sql"
    for stmt in parse_sql(ddl_path.read_text(encoding="utf-8")):
        spark.sql(stmt)


def read_raw(
    spark: SparkSession,
    base: str,
    event_type: str,
    date_start: str,
    date_end: str,
) -> DataFrame:
    """S3 raw zone에서 지정 날짜 범위의 Parquet 파일을 읽는다.

    Args:
        spark: SparkSession
        base: S3 raw zone 루트 경로 (e.g. "s3a://bucket/raw")
        event_type: "impression" | "click" | "conversion"
        date_start: raw_date 필터 시작 (YYYY-MM-DD, inclusive)
        date_end: raw_date 필터 종료 (YYYY-MM-DD, inclusive)

    Returns:
        raw_date 파티션 pruning이 적용된 DataFrame
    """
    return (
        spark.read
        .parquet(f"{base}/{event_type}s")
        .filter((col("raw_date") >= date_start) & (col("raw_date") <= date_end))
    )


def dedup(df: DataFrame, key_col: str) -> DataFrame:
    """key_col 기준 배치 내 중복 제거. Kafka at-least-once 중복 방어.

    Args:
        df: 중복이 포함된 원본 DataFrame
        key_col: 중복 판정 기준 컬럼명 (e.g. "eid")

    Returns:
        key_col당 ingest_ts 최솟값 행만 남긴 DataFrame

    Note:
        ingest_ts tie-breaking 비결정론 주의 — JOBS_GUIDE.md §5-E2 참고.
    """
    w = Window.partitionBy(key_col).orderBy("ingest_ts")
    return df.withColumn("_rn", row_number().over(w)).filter(col("_rn") == 1).drop("_rn")


def transform(
    imp_df: DataFrame,
    clk_df: DataFrame,
    cvt_df: DataFrame,
    click_window_sec: int,
    conv_window_sec: int,
) -> DataFrame:
    """full_refresh 전용: impression / click / conversion을 단일 DataFrame으로 조인한다.

    일반 배치에서는 사용하지 않는다. 설계 배경은 JOBS_GUIDE.md §2-1 참고.

    Args:
        imp_df: raw impression DataFrame
        clk_df: raw click DataFrame (raw_date 범위를 click window만큼 확장한 값 전달 전제)
        cvt_df: raw conversion DataFrame (raw_date 범위를 conversion window만큼 확장한 값 전달 전제)
        click_window_sec: click attribution window (초)
        conv_window_sec: conversion attribution window (초)

    Returns:
        Silver 스키마에 맞는 staged DataFrame
    """
    imp = dedup(imp_df, "eid")
    clk = dedup(clk_df, "eid")
    cvt = dedup(cvt_df, "eid")

    clk_slim = clk.select(col("eid"), col("timestamp").alias("click_ts"))
    cvt_slim = cvt.select(col("eid"), col("timestamp").alias("conv_ts"))

    base = (
        imp.alias("i")
        .join(
            clk_slim.alias("c"),
            on=(col("i.eid") == col("c.eid"))
               & (col("c.click_ts") - col("i.timestamp")).between(0, click_window_sec),
            how="left",
        )
        .join(
            cvt_slim.alias("cv"),
            on=(col("i.eid") == col("cv.eid"))
               & (col("cv.conv_ts") - col("i.timestamp")).between(0, conv_window_sec),
            how="left",
        )
    )

    return base.select(
        col("i.eid").alias("event_id"),
        to_date(col("i.timestamp").cast("timestamp")).alias("event_date"),
        hour(col("i.timestamp").cast("timestamp")).cast("int").alias("event_hour"),
        to_timestamp(col("i.event_time")).alias("event_time"),
        col("i.uid").alias("uid"),
        col("i.campaign").alias("campaign"),
        when(col("c.click_ts").isNotNull(), lit(1)).otherwise(lit(0)).cast("int").alias("click"),
        when(col("cv.conv_ts").isNotNull(), lit(1)).otherwise(lit(0)).cast("int").alias("conversion"),
        coalesce(col("cv.conv_ts").cast(LongType()), lit(-1)).alias("conversion_timestamp"),
        when(
            col("cv.conv_ts").isNotNull(),
            (col("cv.conv_ts") - col("i.timestamp")).cast(LongType()),
        ).otherwise(lit(-1)).alias("conversion_delay_sec"),
        coalesce(col("i.cost"), lit(0.0)).alias("cost"),
        (col("i.kafka_timestamp").cast("long") - col("i.timestamp")).cast(LongType())
            .alias("producer_to_broker_sec"),
        (col("i.ingest_ts").cast("long") - col("i.kafka_timestamp").cast("long")).cast(LongType())
            .alias("broker_to_ingest_sec"),
        (col("i.ingest_ts").cast("long") - col("i.timestamp")).cast(LongType())
            .alias("end_to_end_latency_sec"),
        current_timestamp().alias("updated_at"),
    )


def run_full_refresh(staged: DataFrame) -> None:
    """staged DataFrame으로 Silver 테이블 전체를 원자적으로 교체한다.

    Args:
        staged: Silver 스키마에 맞는 전체 데이터 DataFrame
    """
    staged.writeTo(FULL_NAME).createOrReplace()


def run_impression_stage(spark: SparkSession, imp_df: DataFrame) -> None:
    """Stage 1: impression을 Silver에 INSERT. click/conversion은 0/-1 초기값으로 채운다.

    왜 2-Stage인지, 왜 click/conversion을 분리했는지는 JOBS_GUIDE.md §2-1 참고.

    Args:
        spark: SparkSession
        imp_df: raw impression DataFrame (raw_date 필터 적용 전제)
    """
    imp = dedup(imp_df, "eid")

    staged = imp.select(
        col("eid").alias("event_id"),
        to_date(col("timestamp").cast("timestamp")).alias("event_date"),
        hour(col("timestamp").cast("timestamp")).cast("int").alias("event_hour"),
        to_timestamp(col("event_time")).alias("event_time"),
        col("uid"),
        col("campaign"),
        lit(0).cast("int").alias("click"),
        lit(0).cast("int").alias("conversion"),
        lit(-1).cast(LongType()).alias("conversion_timestamp"),
        lit(-1).cast(LongType()).alias("conversion_delay_sec"),
        coalesce(col("cost"), lit(0.0)).alias("cost"),
        (col("kafka_timestamp").cast("long") - col("timestamp")).cast(LongType())
            .alias("producer_to_broker_sec"),
        (col("ingest_ts").cast("long") - col("kafka_timestamp").cast("long")).cast(LongType())
            .alias("broker_to_ingest_sec"),
        (col("ingest_ts").cast("long") - col("timestamp")).cast(LongType())
            .alias("end_to_end_latency_sec"),
        current_timestamp().alias("updated_at"),
    )

    staged.createOrReplaceTempView("staged_impressions")
    spark.sql(f"""
        MERGE INTO {FULL_NAME} t
        USING staged_impressions s
        ON t.event_id = s.event_id
        WHEN NOT MATCHED THEN INSERT *
    """)
    logger.info("Stage 1 완료: impression MERGE INSERT")


def _assert_attribution_matched(
    event_type: str,
    bronze_count: int,
    matched_count: int,
) -> None:
    """Attribution 매칭 결과를 검증한다.

    Soft failure 설계 배경(왜 0건만 감지하는가, 임계값 한계)은 JOBS_GUIDE.md §2-6, §5-E3 참고.

    Args:
        event_type: "click" | "conversion" (에러 메시지 식별용)
        bronze_count: Bronze에서 읽은 이벤트 수
        matched_count: Silver impression과 매칭된 수

    Raises:
        RuntimeError: Bronze 데이터가 있는데 Silver 매칭이 0건인 경우
    """
    if bronze_count > 0 and matched_count == 0:
        raise RuntimeError(
            f"{event_type} attribution Stage 2 anomaly: "
            f"Bronze {event_type} {bronze_count}건 중 Silver impression 매칭 0건. "
            f"Stage 1 미완료 또는 impression 누락 의심. "
            f"Attribution window 내 impression 부재 가능성 확인 필요."
        )


def run_attribution_stage(
    spark: SparkSession,
    clk_df: DataFrame,
    cvt_df: DataFrame,
    click_window_sec: int,
    conv_window_sec: int,
) -> None:
    """Stage 2: 오늘 수집된 click/conversion을 Silver impression에 MERGE UPDATE한다.

    Attribution window 판정 로직, partition pruning 힌트 설계는 JOBS_GUIDE.md §2-2 참고.

    Args:
        spark: SparkSession
        clk_df: raw click DataFrame (오늘 raw_date 기준)
        cvt_df: raw conversion DataFrame (오늘 raw_date 기준)
        click_window_sec: click attribution window (초)
        conv_window_sec: conversion attribution window (초)

    Raises:
        RuntimeError: Bronze에 데이터가 있는데 Silver 매칭이 0건인 경우
    """
    clk = dedup(clk_df, "eid")
    cvt = dedup(cvt_df, "eid")
    silver = spark.table(FULL_NAME)

    # --- click attribution ---
    clk.cache()
    click_updates = (
        clk.alias("c")
        .join(
            silver.alias("s"),
            (col("c.eid") == col("s.event_id"))
            & (col("c.timestamp") - col("s.event_time").cast("long")).between(0, click_window_sec)
            # Silver event_date 파티션 pruning 힌트 — JOBS_GUIDE.md §2-2 참고
            & (col("s.event_date") >= to_date((col("c.timestamp") - click_window_sec).cast("timestamp"))),
            how="inner",
        )
        .select(col("s.event_id"), col("s.event_date"))
    )
    _assert_attribution_matched("click", clk.count(), click_updates.count())

    click_updates.createOrReplaceTempView("click_updates")
    spark.sql(f"""
        MERGE INTO {FULL_NAME} t
        USING click_updates s
        ON t.event_id = s.event_id AND t.event_date = s.event_date
        WHEN MATCHED AND t.click = 0
        THEN UPDATE SET t.click = 1, t.updated_at = current_timestamp()
    """)
    clk.unpersist()
    logger.info("Stage 2 완료: click attribution MERGE")

    # --- conversion attribution ---
    cvt.cache()
    conv_updates = (
        cvt.alias("cv")
        .join(
            silver.alias("s"),
            (col("cv.eid") == col("s.event_id"))
            & (col("cv.timestamp") - col("s.event_time").cast("long")).between(0, conv_window_sec)
            # Silver event_date 파티션 pruning 힌트 — JOBS_GUIDE.md §2-2 참고
            & (col("s.event_date") >= to_date((col("cv.timestamp") - conv_window_sec).cast("timestamp"))),
            how="inner",
        )
        .select(
            col("s.event_id"),
            col("s.event_date"),
            col("cv.timestamp").cast(LongType()).alias("conversion_timestamp"),
            (col("cv.timestamp") - col("s.event_time").cast("long")).cast(LongType())
                .alias("conversion_delay_sec"),
        )
    )
    _assert_attribution_matched("conversion", cvt.count(), conv_updates.count())

    conv_updates.createOrReplaceTempView("conv_updates")
    spark.sql(f"""
        MERGE INTO {FULL_NAME} t
        USING conv_updates s
        ON t.event_id = s.event_id AND t.event_date = s.event_date
        WHEN MATCHED AND t.conversion = 0
        THEN UPDATE SET
            t.conversion           = 1,
            t.conversion_timestamp = s.conversion_timestamp,
            t.conversion_delay_sec = s.conversion_delay_sec,
            t.updated_at           = current_timestamp()
    """)
    cvt.unpersist()
    logger.info("Stage 2 완료: conversion attribution MERGE")


def main() -> None:
    args = parse_args()

    click_window_sec = args.click_window_days * SECONDS_PER_DAY
    conv_window_sec = args.conversion_window_days * SECONDS_PER_DAY

    spark = build_spark("RawToProcessedIceberg", CATALOG, args.glue_warehouse)
    ensure_table(spark)

    imp_df = read_raw(
        spark, args.s3_raw_base, "impression", args.run_date_start, args.run_date_end
    )

    if args.full_refresh:
        # full_refresh 시 raw_date 범위 확장 이유 — JOBS_GUIDE.md §2-3 참고
        click_raw_end = (
            date.fromisoformat(args.run_date_end) + timedelta(days=args.click_window_days)
        ).isoformat()
        conv_raw_end = (
            date.fromisoformat(args.run_date_end) + timedelta(days=args.conversion_window_days)
        ).isoformat()
        clk_df = read_raw(spark, args.s3_raw_base, "click", args.run_date_start, click_raw_end)
        cvt_df = read_raw(spark, args.s3_raw_base, "conversion", args.run_date_start, conv_raw_end)
        staged = transform(imp_df, clk_df, cvt_df, click_window_sec, conv_window_sec)
        run_full_refresh(staged)
    else:
        clk_df = read_raw(
            spark, args.s3_raw_base, "click", args.run_date_start, args.run_date_end
        )
        cvt_df = read_raw(
            spark, args.s3_raw_base, "conversion", args.run_date_start, args.run_date_end
        )
        run_impression_stage(spark, imp_df)
        run_attribution_stage(spark, clk_df, cvt_df, click_window_sec, conv_window_sec)

    spark.stop()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    main()
