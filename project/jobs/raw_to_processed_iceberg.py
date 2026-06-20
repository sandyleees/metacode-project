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
            'format-version'                         = '2',
            'write.merge.mode'                       = 'merge-on-read',
            'write.target-file-size-bytes'           = '134217728',
            'write.metadata.previous-versions-max'   = '21',
            'write.metadata.delete-after-commit.enabled' = 'true'
        )
    """)
    # write.merge.mode MOR: MERGE INTO 구문 전체에 적용.
    # write.metadata.previous-versions-max=21 + delete-after-commit.enabled:
    #   Silver는 하루 3커밋(impression/click/conversion) → 21 = 7일치 metadata.json 보존.
    #   Gold는 하루 1커밋 → previous-versions-max=7로 동일 7일 커버 (캘린더 기준 통일).
    #   expire_snapshots(30일) > metadata.json 보존(7일) → 복구 시 스냅샷 데이터 파일 생존 보장.
        # metadata.json 보존 기간 ≤ 스냅샷 보존 기간
        # metadata.json 보존 기간 = previous-versions-max ÷ (하루 커밋 수)
        # Silver: 21 ÷ 3회/일 = 7일  <  expire 30일  ✓
    #   WHEN NOT MATCHED INSERT → 항상 append, COW/MOR 무관.
    #   WHEN MATCHED UPDATE    → MOR는 delta 파일만 append (COW는 파일 전체 재작성).
    #   광고 데이터 특성상 UPDATE 대상(click/conversion)은 전체 impression의 <2% → MOR 효과 큼.
    #   읽기 시 base+delta 병합 오버헤드는 daily compaction(maintenance.sh)이 흡수.
    # write.update.mode / write.delete.mode 미설정:
    #   standalone UPDATE/DELETE 구문이 없으므로 불필요.


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


def run_full_refresh(staged) -> None:
    # createOrReplace(): Iceberg 원자적 테이블 교체
    #   - Glue catalog 엔트리 유지 (DROP TABLE은 Athena/Superset 참조 단절, 스냅샷 이력 소멸 위험)
    #   - 교체 전 스냅샷은 expire 전까지 time travel 가능
    #   - 실패 시 이전 상태 보존 (원자성 보장)
    staged.writeTo(FULL_NAME).createOrReplace()


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
) -> None:
    # Stage 2: 오늘 raw에 도착한 click/conversion → Silver에서 eid로 매칭되는 impression 행에 MERGE UPDATE
    # impression은 Stage 1에서 이미 Silver에 올라가 있으므로 raw_date와 무관하게 join 가능.
    # lookback 기준은 각 행의 event_time(c.timestamp / cv.timestamp) — ingest_ts(run_date_end) 축이 아님.
    # 수집 지연이 있어도 실제 발생 시각 기준 window를 정확히 판정하기 위해 per-row로 계산.
    clk = dedup(clk_df, "eid")
    cvt = dedup(cvt_df, "eid")

    silver = spark.table(FULL_NAME)

    # --- click attribution ---
    click_updates = (
        clk.alias("c")
        .join(
            silver.alias("s"),
            # 조건 1: eid 일치 — 이 click이 어느 impression에서 발생했는지 특정
            (col("c.eid") == col("s.event_id"))
            # 조건 2: attribution window 판정 (비즈니스 로직)
            #   c.timestamp - s.event_time: click이 impression보다 얼마나 나중인지 (초)
            #   >= 0: click은 반드시 impression 이후 발생 (순서 보장)
            #   <= click_window_sec: impression 후 7일 이내 — 디지털 광고 업계 표준
            #   양쪽 모두 unix 초 단위: c.timestamp(LongType), event_time.cast("long")
            & (col("c.timestamp") - col("s.event_time").cast("long")).between(0, click_window_sec)
            # 조건 3: Silver event_date 파티션 pruning 힌트 (성능)
            #   조건 2에서 c.timestamp - s.event_time <= click_window_sec 이면 조건 3도 항상 성립 — 논리적 중복.
            #   단, 조건 2는 두 동적 컬럼의 연산이라 Iceberg가 스캔 전 파티션 제거에 쓰기 어려움.
            #   조건 3은 파티션 키(event_date)를 직접 비교하므로 Spark DFP가 인식해 불필요한 파티션 열기를 차단.
            #   (c.timestamp - click_window_sec).cast("timestamp"): click 기준 window 하한을 unix 초 → Timestamp
            #   to_date(...): Timestamp → DATE (Silver event_date 파티션 키 타입과 일치)
            & (col("s.event_date") >= to_date((col("c.timestamp") - click_window_sec).cast("timestamp"))),
            how="inner",
        )
        # event_date를 함께 전달 → MERGE ON 절에서 파티션 키로 타겟 pruning
        .select(col("s.event_id"), col("s.event_date"))
    )
    # Soft failure 감지: Bronze에 click이 있는데 Silver impression 매칭이 0건이면
    # Stage 1 미완료 또는 Silver 데이터 누락 의심 → 조용히 지나가지 않고 명시적으로 실패
    # Hard failure(MERGE exception)는 Airflow가 잡지만, 0건 업데이트는 정상 종료처럼 보임
    bronze_click_count = clk.count()
    matched_click_count = click_updates.count()
    if bronze_click_count > 0 and matched_click_count == 0:
        raise RuntimeError(
            f"Click attribution Stage 2 anomaly: "
            f"Bronze click {bronze_click_count}건 중 Silver impression 매칭 0건 — "
            f"Stage 1 실패 또는 impression 누락 의심"
            f"Attribution window 내 impression 부재"
        )
    
    click_updates.createOrReplaceTempView("click_updates")
    spark.sql(f"""
        MERGE INTO {FULL_NAME} t
        USING click_updates s
        ON t.event_id = s.event_id AND t.event_date = s.event_date
        WHEN MATCHED AND t.click = 0
        THEN UPDATE SET t.click = 1, t.updated_at = current_timestamp()
    """)

    # --- conversion attribution ---
    conv_updates = (
        cvt.alias("cv")
        .join(
            silver.alias("s"),
            # 조건 1: eid 일치
            (col("cv.eid") == col("s.event_id"))
            # 조건 2: attribution window 판정 — conversion은 30일 window
            & (col("cv.timestamp") - col("s.event_time").cast("long")).between(0, conv_window_sec)
            # 조건 3: Silver event_date 파티션 pruning 힌트 — click과 동일한 원리, window만 30일
            & (col("s.event_date") >= to_date((col("cv.timestamp") - conv_window_sec).cast("timestamp"))),
            how="inner",
        )
        .select(
            col("s.event_id"),
            col("s.event_date"),
            col("cv.timestamp").cast(LongType()).alias("conversion_timestamp"),
            (col("cv.timestamp") - col("s.event_time").cast("long")).cast(LongType()).alias("conversion_delay_sec"),
        )
    )

    bronze_conv_count = cvt.count()
    matched_conv_count = conv_updates.count()
    if bronze_conv_count > 0 and matched_conv_count == 0:
        raise RuntimeError(
            f"Conversion attribution Stage 2 anomaly: "
            f"Bronze conversion {bronze_conv_count}건 중 Silver impression 매칭 0건 — "
            f"Stage 1 실패 또는 impression 누락 의심"
            f"Attribution window 내 impression 부재"
        )
    
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
        run_full_refresh(staged)
    else:
        clk_df = read_raw(spark, args.s3_raw_base, "click",      args.run_date_start, args.run_date_end)
        cvt_df = read_raw(spark, args.s3_raw_base, "conversion", args.run_date_start, args.run_date_end)
        run_impression_stage(spark, imp_df)
        run_attribution_stage(spark, clk_df, cvt_df, click_window_sec, conv_window_sec)

    spark.stop() # 리소스 정리 - 배치 작업


if __name__ == "__main__":
    main()
