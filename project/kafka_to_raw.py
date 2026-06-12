'''
kafka 브로커에 있는 impression/click/conversion 토픽을
Spark structured streaming으로 S3 raw에 저장 (Bronze)
raw는 Iceberg 테이블이 아니라 append-only 파일 zone으로 둔다.
기본 포맷은 parquet이며 spark 수집시간 기준 raw_date / raw_hour 단위로 파티션 저장한다.
'''
# 멱등성, 예외처리 고려 필요
# logging? 문제 발생 시 슬랙 등 알림 처리?

import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json, hour, to_date
from pyspark.sql.types import (
    DoubleType, IntegerType, LongType, StringType, StructField, StructType
)


def parse_args():
    # ── argparse 파라미터 선정 기준 ──────────────────────────────────────────
    # "환경(dev/staging/prod)이나 실행 목적(live vs replay)에 따라 바뀌는 값"만 파라미터로
    # 스키마 정의 / outputMode / partitionBy / format 은 데이터 설계 결정 → 코드에 고정
    parser = argparse.ArgumentParser(
        description="Kafka 토픽 3개를 Spark Structured Streaming으로 S3 raw zone에 적재"
    )

    # ── 인프라 연결 정보 ───────────────────────────────────────────────────
    # 환경마다 Kafka 주소가 다름 (로컬: localhost:9092 / 컨테이너: kafka:29092 / 운영: 브로커 DNS)
    # → 환경변수 폴백: docker-compose에서 BOOTSTRAP_SERVERS 주입, 코드 무수정
    parser.add_argument(
        "--bootstrap-servers",
        default=os.environ.get("BOOTSTRAP_SERVERS", "localhost:9092"),
        help="Kafka bootstrap server 주소 (기본값: BOOTSTRAP_SERVERS 환경변수 또는 localhost:9092)",
    )

    # ── 토픽명 ────────────────────────────────────────────────────────────
    # 스테이징/프로덕션 간 토픽 네이밍 구분 (예: prod-ad-impressions vs ad-impressions)
    # → 하드코딩 기본값: 토픽명은 파이프라인 설계 시 고정되는 경향 → 환경변수 폴백 불필요
    parser.add_argument("--topic-impression", default="ad-impressions",
                        help="노출 이벤트 토픽명 (기본값: ad-impressions)")
    parser.add_argument("--topic-click",      default="ad-clicks",
                        help="클릭 이벤트 토픽명 (기본값: ad-clicks)")
    parser.add_argument("--topic-conversion", default="ad-conversions",
                        help="전환 이벤트 토픽명 (기본값: ad-conversions)")

    # ── S3 경로 ───────────────────────────────────────────────────────────
    # 버킷명이 환경마다 다름 (dev-bucket / prod-bucket)
    # → 환경변수 폴백: docker-compose에서 S3_RAW_BASE / CHECKPOINT_BASE 주입
    parser.add_argument(
        "--s3-raw-base",
        default=os.environ.get("S3_RAW_BASE", "s3a://your-bucket/raw"),
        help="S3 raw zone 기본 경로 (기본값: S3_RAW_BASE 환경변수 또는 s3a://your-bucket/raw)",
    )
    parser.add_argument(
        "--checkpoint-base",
        default=os.environ.get("CHECKPOINT_BASE", "s3a://your-bucket/checkpoints/kafka_to_raw"),
        help="Spark 체크포인트 기본 경로 (기본값: CHECKPOINT_BASE 환경변수)",
    )

    # ── 스트리밍 동작 제어 ─────────────────────────────────────────────────
    # 실행 목적에 따라 바뀌는 파라미터
    # - latest  (기본): 정상 운영 — 새로 들어오는 메시지만 처리
    # - earliest:       장애 후 재처리(replay) — retention 범위 내 전체 메시지 재소비
    # → 코드 수정 없이 CLI 인자로 전환 가능한 게 핵심 이유
    parser.add_argument(
        "--starting-offsets",
        default="latest",
        choices=["latest", "earliest"],
        help="스트리밍 시작 offset (기본값: latest / replay 시: earliest)",
    )

    # ── 단일 토픽 처리 모드 (3-process 구조용) ────────────────────────────
    # 미지정 시 3개 토픽을 1개 프로세스에서 처리 (하위 호환)
    parser.add_argument(
        "--topic-type",
        choices=["impression", "click", "conversion"],
        default=None,
        help="처리할 토픽 종류 (미지정 시 3개 토픽 동시 처리)",
    )

    return parser.parse_args()


# ── 스키마 정의 ────────────────────────────────────────────────────────────
# 스키마는 데이터 설계 결정 → 환경/목적에 따라 바뀌지 않음 → 코드에 고정
# (argparse 파라미터로 만들지 않는 이유)

basic_schema = StructType([
    StructField("eid",        StringType(),  nullable=False),
    StructField("uid",        StringType(),  nullable=False),
    StructField("timestamp",  LongType(),    nullable=False),  # 이벤트 발생 unix timestamp (초)
    StructField("event_time", StringType(),  nullable=True),   # 이벤트 발생 절대시간 (ISO 문자열)
    StructField("event_type", StringType(),  nullable=False),
    StructField("campaign",   IntegerType(), nullable=True),
])

# impression만 cost 필드 추가
# StructType([basic_schema, cost]) 구문은 Python 문법 오류 (StructType은 StructField 리스트를 받음)
# → basic_schema.fields (StructField 리스트) + 새 StructField 리스트로 병합
impression_schema = StructType(
    basic_schema.fields + [StructField("cost", DoubleType(), nullable=True)]
)

click_schema      = basic_schema
conversion_schema = basic_schema


# ── 함수 정의 ──────────────────────────────────────────────────────────────

def read_kafka_topic(spark, topic: str, bootstrap_servers: str, starting_offsets: str):
    # startingOffsets: 처음 실행 시 어디서부터 읽을지
    #   latest   → 새 메시지만 (정상 운영)
    #   earliest → 전체 재소비 (replay)
    # failOnDataLoss=false: Kafka retention 만료 등으로 offset 손실 시 에러 대신 스킵
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_topic(raw_df, schema: StructType):
    # kafka value는 binary → cast("string") 후 from_json으로 역직렬화
    # select("data.*")로 중첩 struct 해제해 개별 컬럼으로 전개
    # raw_date / raw_hour: ingest_ts(Spark 처리 시각) 기준으로 파티션 분리
    #   이벤트 timestamp 기준으로 할 수도 있으나
    #   전환 이벤트처럼 지연 발행되는 경우 ingest_ts 기준이 시간대별 파일 크기 균등에 유리
    return (
        raw_df
        .select(
            from_json(col("value").cast("string"), schema).alias("data"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("timestamp").alias("kafka_timestamp"),  # 브로커 수신 시각
        )
        .select("data.*", "kafka_partition", "kafka_offset", "kafka_timestamp")
        .withColumn("ingest_ts", current_timestamp())
        .withColumn("raw_date",  to_date(col("ingest_ts")))
        .withColumn("raw_hour",  hour(col("ingest_ts")))
    )


def write_stream(df, output_path: str, checkpoint_path: str):
    # outputMode("append"): raw zone은 수정 없이 쌓기만 함 → append 고정
    # partitionBy: Hive-style prefix (raw_date=2025-01-01/raw_hour=13/) 로 S3에 저장
    #   후속 Athena/Trino 쿼리에서 파티션 pruning으로 풀스캔 방지
    # checkpointLocation: Spark이 처리 완료한 offset을 기록 → 재시작 시 중복 처리 방지
    return (
        df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .partitionBy("raw_date", "raw_hour")
        .start()
    )


def main():
    args = parse_args()

    # 해당 코드도 서버에 항상 띄워놔야하나? streaming이니까.
    # → YES. Spark Structured Streaming은 long-running 프로세스
    #   아래 awaitAnyTermination()이 main thread를 무한 블로킹 → 컨테이너가 항상 떠 있어야 함
    #   운영에서는 YARN / K8s 이 크래시 감지 후 자동 재시작 처리

    # 1. SparkSession 준비
    # master는 spark-submit --master 로 외부 주입 (코드에 하드코딩 금지)
    #   로컬 테스트: spark-submit --master local[*] kafka_to_raw.py
    spark = (
        SparkSession.builder
        .appName("KafkaToRaw")
        # JAR은 Dockerfile.spark에서 /opt/spark/jars에 직접 배치
        # s3a 설정은 spark-defaults.conf에 고정 (Dockerfile.spark 참고)
        # AWS 자격증명: 코드 하드코딩 금지 → 환경변수(AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY) 사용
        # EC2 IAM 역할 환경이라면 아래 한 줄로 대체 가능
        # .config("spark.hadoop.fs.s3a.aws.credentials.provider",
        #         "com.amazonaws.auth.InstanceProfileCredentialsProvider")

        # 환경변수 AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY는 OS 레벨에 있어도,
        # Hadoop S3A는 Spark config로 명시적으로 전달해야 읽음.
        .config("spark.hadoop.fs.s3a.access.key",
                os.environ.get("AWS_ACCESS_KEY_ID", ""))
        .config("spark.hadoop.fs.s3a.secret.key",
                os.environ.get("AWS_SECRET_ACCESS_KEY", ""))
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        .getOrCreate()
    )

    # 2. 토픽별 streaming query 구성
    # readStream 사용, 토픽 3개에서 가져오려면 Spark.readStream코드 3개 생기는건가?
    # → .option("subscribe", "topic-a,topic-b,topic-c") 로 한 readStream에 묶는 방법도 있음
    #   하지만 impression에만 cost 필드가 있어 스키마가 달라 토픽별 별도 readStream이 명확함
    #   readStream 3개 = streaming query 3개가 spark.streams에 등록되어 스레드별 동시 실행됨
    # job마다 checkpoint 설정
    # → checkpointLocation을 토픽별로 다른 경로로 지정 필수
    #   같은 경로 공유 시 서로 다른 토픽의 offset 진행 상태가 섞여 재처리 시 누락 또는 중복 발생
    all_topics = [
        (args.topic_impression, impression_schema, "impressions"),
        (args.topic_click,      click_schema,      "clicks"),
        (args.topic_conversion, conversion_schema, "conversions"),
    ]
    topic_map = {
        "impression": all_topics[0],
        "click":      all_topics[1],
        "conversion": all_topics[2],
    }
    topics = [topic_map[args.topic_type]] if args.topic_type else all_topics

    for topic_name, schema, suffix in topics:
        raw_df = read_kafka_topic(spark, topic_name, args.bootstrap_servers, args.starting_offsets)
        df     = parse_topic(raw_df, schema)
        write_stream(
            df,
            f"{args.s3_raw_base}/{suffix}",
            f"{args.checkpoint_base}/{suffix}",
        )

    # 3. 세 streaming query가 종료될 때까지 main thread 대기
    # awaitAnyTermination(): 3개 쿼리 중 하나라도 종료(에러 or 명시적 stop)되면 main thread 해제
    # 정상 운영 중에는 영구 블로킹 → 컨테이너(프로세스)가 항상 살아있어야 함
    # 컨테이너가 exit하면 YARN/K8s의 supervisor가 재시작 처리
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
