"""
raw files -> processed_events Iceberg (silver), 일단위 배치
Airflow 자동화 가능: run_date_start/end 를 {{ ds }} 로 주입하면 일배치 스케줄링 됨

raw zone(append-only parquet)에서 impression/click/conversion을 읽어
시간차 이벤트를 하나의 silver 테이블로 조인/변환 후 Iceberg로 MERGE.
"""

import argparse
import os
from datetime import date, timedelta

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    coalesce, col, current_timestamp,
    hour, lit, row_number, to_date, to_timestamp,
    when,
)
from pyspark.sql.types import LongType


CATALOG   = "glue_catalog"
DATABASE  = "silver"
TABLE     = "processed_events"
FULL_NAME = f"{CATALOG}.{DATABASE}.{TABLE}"


def parse_args():
    # run_date_start / run_date_end 범위 지정 → 특정 날짜 재처리 시 그 범위만 실행 가능
    # 기본값은 어제 하루 — 일배치이므로 매일 전날치를 처리하는 것이 기본 동작
    yesterday = (date.today() - timedelta(days=1)).isoformat()

    parser = argparse.ArgumentParser(
        description="S3 raw zone → processed_events Iceberg (silver) 일단위 배치"
    )
    parser.add_argument(
        "--run-date-start",
        default=yesterday,
        help="처리 시작 날짜 (YYYY-MM-DD, 기본값: 어제). raw_date 파티션 기준으로 이 날짜 이상 읽음",
    )
    parser.add_argument(
        "--run-date-end",
        default=yesterday,
        help="처리 종료 날짜 (YYYY-MM-DD inclusive, 기본값: 어제). 단일 날짜면 start == end",
    )
    parser.add_argument(
        "--s3-raw-base",
        default=os.environ.get("S3_RAW_BASE", "s3a://your-bucket/raw"),
        help="S3 raw zone 기본 경로",
    )
    parser.add_argument(
        "--glue-warehouse",
        default=os.environ.get("GLUE_WAREHOUSE", "s3a://your-bucket/warehouse"),
        help="Iceberg warehouse 경로 (Glue catalog 메타스토어가 실제 데이터를 쓰는 S3 경로)",
    )
    parser.add_argument(
        "--click-window-days",
        type=int,
        default=7,
        help="impression 기준 click 매칭 최대 일수 (업계 표준: 7일)",
    )
    parser.add_argument(
        "--conversion-window-days",
        type=int,
        default=30,
        help="impression 기준 conversion 매칭 최대 일수 (업계 표준: 30일)",
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        default=False,
        help="지정 시 테이블 DROP 후 전체 재적재 (기본값: False, 일반 운영 시 불필요)",
    )

    return parser.parse_args()


def build_spark(glue_warehouse: str) -> SparkSession:
    # Iceberg + Glue Catalog SparkSession 설정:
    #   spark.sql.extensions: Iceberg SQL 확장 등록
    #     → CREATE TABLE USING iceberg, MERGE INTO 구문 활성화됨
    #   spark.sql.catalog.glue_catalog: "glue_catalog" 이름으로 Iceberg catalog 등록
    #     (이 이름이 SQL에서 테이블 참조 시 prefix 로 사용됨: glue_catalog.silver.processed_events)
    #   catalog-impl: GlueCatalog → AWS Glue Data Catalog 를 Iceberg 메타스토어로 사용
    #     Glue 는 테이블 스키마·파티션 메타만 저장, 실제 데이터 파일은 S3 warehouse 에 기록
    #   warehouse: Iceberg 가 실제 데이터를 쓰는 S3 루트 경로
    #     테이블 경로는 warehouse/{database}/{table} 로 자동 결정됨
    #   io-impl: S3FileIO → Iceberg 가 S3 데이터 파일을 읽고 쓸 때 사용하는 구현체
    return (
        SparkSession.builder
        .appName("RawToProcessedIceberg")
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
        
        # GlueCatalog(Driver)용 자격증명 — Glue API 호출은 Driver에서만 발생
        .config(f"spark.sql.catalog.{CATALOG}.client.region",
                os.environ.get("AWS_DEFAULT_REGION", "ap-northeast-2"))
        .config(f"spark.sql.catalog.{CATALOG}.client.access-key-id",
                os.environ.get("AWS_ACCESS_KEY_ID", ""))
        .config(f"spark.sql.catalog.{CATALOG}.client.secret-access-key",
                os.environ.get("AWS_SECRET_ACCESS_KEY", ""))
        # Iceberg S3FileIO는 Executor JVM에서 실행되며 spark.sql.catalog.* 를 읽지 않는다.
        # spark.executorEnv.* 로 전달해야 Executor의 AWS 자격증명 체인(EnvironmentVariableCredentialsProvider)이 인식한다.
        # (spark.hadoop.fs.s3a.* 는 Hadoop S3A 전용이고 S3FileIO 와 무관)
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


def ensure_table(spark: SparkSession) -> None:
    # Airflow 자동화 시: DAG 첫 task 로 ensure_table 을 호출하면 테이블 존재를 사전 보장할 수 있음
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG}.{DATABASE}")

    # event_date: 파티션 키 — 일단위 배치이므로 DATE 로 충분, 시/분/초는 event_time 컬럼에 유지
    # click / conversion: 없으면 0 (원본 Criteo 데이터셋의 0/1 인코딩과 통일)
    # conversion_timestamp / conversion_delay_sec: 전환 없으면 -1 (0 은 "즉시 전환"을 의미할 수 있어 -1 을 sentinel 로 사용)
    # event_hour: impression 발생 시각의 시(0~23) — 시계열 분석(시간대별 CTR/CVR 등)용 파생 컬럼
    # updated_at: click/conversion 을 구분해 별도로 기록할 필요 없음
    #   MERGE 실행 시마다 current_timestamp() 로 갱신하면 "마지막으로 이 행이 업데이트된 시각" 을 충분히 추적 가능
    #   어떤 이벤트가 트리거했는지까지 추적이 필요하다면 별도 audit 테이블 고려
    # 지연 측정 파생 컬럼 — kafka_to_raw.py 에서 raw 에 저장되는 컬럼 활용:
    #   kafka_timestamp : Kafka 브로커 수신 시각 (SparkType: Timestamp)
    #   ingest_ts       : Spark structured streaming 처리 시각 (current_timestamp)
    #   timestamp       : producer 가 설정한 이벤트 발생 unix ts (초, LongType)
    #
    #   producer_to_broker_sec  = kafka_timestamp(초) - timestamp(초)  : 네트워크·프로듀서 지연
    #   broker_to_ingest_sec    = ingest_ts(초) - kafka_timestamp(초)  : 소비자·Spark 처리 지연
    #   end_to_end_latency_sec  = ingest_ts(초) - timestamp(초)        : 전체 파이프라인 지연
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {FULL_NAME} (
            event_id                STRING    NOT NULL COMMENT 'impression eid — impression/click/conversion 간 공통 join 키',
            event_date              DATE      NOT NULL COMMENT '파티션 키; impression unix ts 기준 일자',
            event_hour              INT                COMMENT '시계열 분석용; impression 발생 시(0~23)',
            event_time              TIMESTAMP          COMMENT 'impression 발생 절대시각',
            uid                     STRING    NOT NULL COMMENT '유저 식별자',
            campaign                INT                COMMENT '캠페인 식별자',
            click                   INT                COMMENT '클릭 발생 1, 없으면 0',
            conversion              INT                COMMENT '전환 발생 1, 없으면 0',
            conversion_timestamp    BIGINT             COMMENT '전환 unix ts(초); 전환 없으면 -1',
            conversion_delay_sec    BIGINT             COMMENT 'conversion_ts - impression_ts(초); 전환 없으면 -1',
            cost                    DOUBLE             COMMENT 'impression 비용 (transformed)',
            producer_to_broker_sec  BIGINT             COMMENT 'Kafka 브로커 수신 - 이벤트 발생(초); 프로듀서·네트워크 지연',
            broker_to_ingest_sec    BIGINT             COMMENT 'Spark ingest - Kafka 브로커 수신(초); 소비자 처리 지연',
            end_to_end_latency_sec  BIGINT             COMMENT 'Spark ingest - 이벤트 발생(초); 전체 파이프라인 지연',
            updated_at              TIMESTAMP          COMMENT 'click/conversion MERGE 반영 시 갱신'
        )
        USING iceberg
        PARTITIONED BY (event_date)
        TBLPROPERTIES (
            'format-version'                = '2',
            'write.update.mode'             = 'copy-on-write',
            'write.merge.mode'              = 'copy-on-write',
            'write.delete.mode'             = 'copy-on-write',
            'write.target-file-size-bytes'  = '134217728'
        )
    """)


def read_raw(spark: SparkSession, base: str, event_type: str,
             date_start: str, date_end: str):
    # raw_date 파티션 pruning → 지정 날짜 범위 데이터만 읽어 풀스캔 방지
    return (
        spark.read
        .parquet(f"{base}/{event_type}s")
        .filter(
            (col("raw_date") >= date_start) & (col("raw_date") <= date_end)
        )
    )


def dedup(df, key_col: str):
    # Kafka at-least-once 보장으로 동일 이벤트가 raw 에 중복 저장될 수 있음
    # key_col 기준 중복 제거 — 동일 key 중 ingest_ts 가 가장 이른 행(최초 수집분)만 유지
    w = Window.partitionBy(key_col).orderBy("ingest_ts")
    return df.withColumn("_rn", row_number().over(w)).filter(col("_rn") == 1).drop("_rn")


def transform(imp_df, clk_df, cvt_df, click_window_sec: int, conv_window_sec: int):
    # 중복 제거: eid 기준 - why? kafka : at-least-once 보장
    imp = dedup(imp_df, "eid")
    clk = dedup(clk_df, "eid")
    cvt = dedup(cvt_df, "eid")

    # click: eid 로 1:1 join — 해당 impression 이 클릭됐는지 여부
    clk_slim = clk.select(
        col("eid"),
        col("timestamp").alias("click_ts"),
        col("ingest_ts").alias("click_ingest_ts"),
    )

    # conversion: eid 로 1:1 join
    # producer.py 에서 conversion_id 기준 중복 제거 후 첫 번째로 본 impression 의 eid 로 발행
    # → 동일 전환을 공유하는 다른 impression 들은 이 파이프라인에서 conversion=0 으로 표시됨
    #   (원본 Criteo 데이터셋과의 차이 — 파이프라인 설계상 특성)
    cvt_slim = cvt.select(
        col("eid"),
        col("timestamp").alias("conv_ts"),
        col("ingest_ts").alias("conv_ingest_ts"),
    )

    # impression 기준 left join — window 조건을 ON 절에 포함
    # click/conversion이 window 밖이면 NULL로 처리(impression 행은 유지)
    # click 7일, conversion 30일 — 디지털 광고 업계 표준 (Google, Meta 등 동일 기준)
    # producer가 real timestamp(unix 초)를 메시지에 기록하므로 차이값이 실제 경과 초와 일치
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
        # 지연 측정 컬럼 — kafka_to_raw.py 에서 raw 에 저장된 kafka_timestamp / ingest_ts 활용
        # Timestamp → cast("long") 은 초 단위 unix epoch 반환 (Spark 3.x 기준)
        # timestamp 는 producer 가 설정한 이벤트 발생 unix ts(초) 로 단위 동일
        (col("i.kafka_timestamp").cast("long") - col("i.timestamp")).cast(LongType())
        .alias("producer_to_broker_sec"),
        (col("i.ingest_ts").cast("long") - col("i.kafka_timestamp").cast("long")).cast(LongType())
        .alias("broker_to_ingest_sec"),
        (col("i.ingest_ts").cast("long") - col("i.timestamp")).cast(LongType())
        .alias("end_to_end_latency_sec"),
        current_timestamp().alias("updated_at"),
    )


def run_full_refresh(spark: SparkSession, staged) -> None:
    # overwritePartitions() 대신 DROP TABLE + append 사용:
    # raw 파티션 키(raw_date, ingest 기준)와 processed 파티션 키(event_date, 이벤트 발생 기준)가 달라
    # raw_date=N 하나를 처리해도 결과 event_date는 지연 이벤트로 인해 여러 날짜에 걸침.
    # overwritePartitions()는 결과 DataFrame에 포함된 event_date 파티션을 통째로 덮어쓰므로
    # 이전 run에서 정확히 채워둔 다른 날짜 파티션까지 삭제될 수 있음.
    # full_refresh는 전체 재적재가 목적이므로 테이블 자체를 DROP 후 재생성하는 것이 안전하고 명확함.
    spark.sql(f"DROP TABLE IF EXISTS {FULL_NAME}")
    ensure_table(spark)
    staged.writeTo(FULL_NAME).append()


def run_impression_stage(spark: SparkSession, imp_df) -> None:
    # Stage 1: impression만 먼저 Silver에 INSERT (click=0, conversion=0 초기값)
    # click/conversion과 분리하는 이유: click이 3일 후에 수집되면 raw_date가 달라
    # 같은 배치에서 join할 수 없으므로, impression을 먼저 Silver에 올려두고
    # click/conversion은 Stage 2에서 Silver를 직접 조회해 attribution 처리
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
        (col("kafka_timestamp").cast("long") - col("timestamp")).cast(LongType()).alias("producer_to_broker_sec"),
        (col("ingest_ts").cast("long") - col("kafka_timestamp").cast("long")).cast(LongType()).alias("broker_to_ingest_sec"),
        (col("ingest_ts").cast("long") - col("timestamp")).cast(LongType()).alias("end_to_end_latency_sec"),
        current_timestamp().alias("updated_at"),
    )
    staged.createOrReplaceTempView("staged_impressions")
    spark.sql(f"""
        MERGE INTO {FULL_NAME} t
        USING staged_impressions s
        ON t.event_id = s.event_id
        WHEN NOT MATCHED THEN INSERT *
    """)


def run_attribution_stage(
    spark: SparkSession,
    clk_df,
    cvt_df,
    click_window_sec: int,
    conv_window_sec: int,
    run_date_end: str,
) -> None:
    # Stage 2: 오늘 raw에 도착한 click/conversion → Silver에서 매칭 impression 찾아 MERGE UPDATE
    # impression이 며칠 전 raw_date에 있어도 이미 Stage 1에서 Silver에 올라가 있으므로 join 가능
    clk = dedup(clk_df, "eid")
    cvt = dedup(cvt_df, "eid")

    # [파티션 pruning] Silver에서 읽을 파티션 범위 제한 — 성능 최적화
    # 오늘 수집된 click이 매칭할 수 있는 impression은 최대 click_window_days(7일) 전까지.
    # 그 이전 event_date 파티션은 어차피 join에서 걸러지므로 Iceberg 파티션 자체를 열지 않음.
    # producer가 real timestamp를 사용하므로 Silver event_date와 run_date_end가 같은 시간축(2026년대).
    #
    # [주의] run_date_end는 click 수집 날짜 기준이고 click.timestamp는 이벤트 발생 날짜 기준.
    # 수집 지연이 크면(click이 며칠 뒤에 수집) 파티션 필터가 유효 impression 파티션을 제외할 수 있음.
    # 현재 producer는 발행 즉시 수집되므로 click.event_time ≈ raw_date, 실질적 누락 없음.
    # 수집 지연이 발생하는 환경에서는 click.agg(min("timestamp"))로 lookback을 정확히 계산해야 함.
    click_lookback = (
        date.fromisoformat(run_date_end) - timedelta(seconds=click_window_sec)
    ).isoformat()
    conv_lookback = (
        date.fromisoformat(run_date_end) - timedelta(seconds=conv_window_sec)
    ).isoformat()

    silver_for_click = spark.table(FULL_NAME).filter(col("event_date") >= click_lookback)
    silver_for_conv  = spark.table(FULL_NAME).filter(col("event_date") >= conv_lookback)

    # [attribution 비즈니스 로직] click이 impression 발생 후 window 내에 일어났는지 초 단위 판정
    # click 7일, conversion 30일 — 디지털 광고 업계 표준 (Google, Meta 등 동일 기준)
    # 파티션 pruning(위)이 날짜 단위 사전 필터라면, 이 조건이 실제 attribution 판정 기준.
    # ≥ 0: click이 impression보다 나중 발생 (순서 보장)
    # ≤ window_sec: impression 발생 후 window 이내
    # event_time.cast("long") → unix 초, clk.timestamp도 unix 초 → 단위 동일
    click_updates = (
        clk.alias("c")
        .join(
            silver_for_click.alias("s"),
            (col("c.eid") == col("s.event_id"))
            & (col("c.timestamp") - col("s.event_time").cast("long")).between(0, click_window_sec),
            how="inner",
        )
        .select(col("s.event_id"))
    )
    click_updates.createOrReplaceTempView("click_updates")
    spark.sql(f"""
        MERGE INTO {FULL_NAME} t
        USING click_updates s
        ON t.event_id = s.event_id
        WHEN MATCHED AND t.click = 0
        THEN UPDATE SET t.click = 1, t.updated_at = current_timestamp()
    """)

    # Conversion attribution: window 내 매칭 impression에 conversion=1 및 지연 정보 업데이트
    conv_updates = (
        cvt.alias("cv")
        .join(
            silver_for_conv.alias("s"),
            (col("cv.eid") == col("s.event_id"))
            & (col("cv.timestamp") - col("s.event_time").cast("long")).between(0, conv_window_sec),
            how="inner",
        )
        .select(
            col("s.event_id"),
            col("cv.timestamp").cast(LongType()).alias("conversion_timestamp"),
            (col("cv.timestamp") - col("s.event_time").cast("long")).cast(LongType()).alias("conversion_delay_sec"),
        )
    )
    conv_updates.createOrReplaceTempView("conv_updates")
    spark.sql(f"""
        MERGE INTO {FULL_NAME} t
        USING conv_updates s
        ON t.event_id = s.event_id
        WHEN MATCHED AND t.conversion = 0
        THEN UPDATE SET
            t.conversion           = 1,
            t.conversion_timestamp = s.conversion_timestamp,
            t.conversion_delay_sec = s.conversion_delay_sec,
            t.updated_at           = current_timestamp()
    """)


def main():
    args = parse_args()

    # attribution window: 메시지 event timestamp 기준 원본 window 크기 적용
    # producer 가 전송 타이밍만 압축할 뿐 메시지 timestamp는 Criteo 원본 상대시간 그대로이므로
    # ingest_ts / kafka_timestamp 기반 압축 window 불필요
    click_window_sec = args.click_window_days * 24 * 3600
    conv_window_sec  = args.conversion_window_days * 24 * 3600

    spark = build_spark(args.glue_warehouse)
    ensure_table(spark)

    imp_df = read_raw(spark, args.s3_raw_base, "impression", args.run_date_start, args.run_date_end)

    if args.full_refresh:
        # full_refresh에서 attribution window와 raw_date 범위는 축이 다름:
        #   attribution window = event_time 기준 (join 조건이 보장)
        #   raw_date 범위      = 수집 날짜 기준 (운영 결정)
        # impression 범위(run_date_end)와 click/conversion 범위를 맞추면
        # run_date_end 이후에 수집된 window 내 click/conversion을 놓침.
        # → click은 impression 기준 최대 7일, conversion은 30일 뒤까지 수집될 수 있으므로
        #   raw_date 읽기 범위를 window만큼 확장해 포함 보장.
        click_raw_end = (date.fromisoformat(args.run_date_end) + timedelta(days=args.click_window_days)).isoformat()
        conv_raw_end  = (date.fromisoformat(args.run_date_end) + timedelta(days=args.conversion_window_days)).isoformat()
        clk_df = read_raw(spark, args.s3_raw_base, "click",      args.run_date_start, click_raw_end)
        cvt_df = read_raw(spark, args.s3_raw_base, "conversion", args.run_date_start, conv_raw_end)
        staged = transform(imp_df, clk_df, cvt_df, click_window_sec, conv_window_sec)
        run_full_refresh(spark, staged)
    else:
        clk_df = read_raw(spark, args.s3_raw_base, "click",      args.run_date_start, args.run_date_end)
        cvt_df = read_raw(spark, args.s3_raw_base, "conversion", args.run_date_start, args.run_date_end)
        run_impression_stage(spark, imp_df)
        run_attribution_stage(spark, clk_df, cvt_df, click_window_sec, conv_window_sec, args.run_date_end)

    spark.stop() # 리소스 정리 - 배치 작업


if __name__ == "__main__":
    main()
