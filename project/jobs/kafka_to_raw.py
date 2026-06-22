"""Kafka 브로커 → S3 raw zone Spark Structured Streaming 적재 (Bronze).

3개 토픽(impression / click / conversion)을 각각 별도 streaming query로 구성하거나,
--topic-type 인자로 단일 토픽만 처리하는 1-process 모드로 실행한다.
설계 배경(3-process 구조, checkpoint 분리, raw_date 파티셔닝 기준)은 JOBS_GUIDE.md §6 참고.
"""

import argparse
import logging
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json, hour, to_date
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import (
    DoubleType, IntegerType, LongType, StringType, StructField, StructType,
)

logger = logging.getLogger(__name__)

# 스키마는 데이터 설계 결정 — 환경/목적에 따라 바뀌지 않으므로 코드에 고정
basic_schema = StructType([
    StructField("eid",        StringType(),  nullable=False),
    StructField("uid",        StringType(),  nullable=False),
    StructField("timestamp",  LongType(),    nullable=False),  # 이벤트 발생 unix ts (초)
    StructField("event_time", StringType(),  nullable=True),   # ISO 문자열 — 후속 to_timestamp() 필요
    StructField("event_type", StringType(),  nullable=False),
    StructField("campaign",   IntegerType(), nullable=True),
])

# impression만 cost 필드 추가 — .fields로 StructField 리스트 병합
impression_schema = StructType(
    basic_schema.fields + [StructField("cost", DoubleType(), nullable=True)]
)

click_schema = basic_schema
conversion_schema = basic_schema


def parse_args() -> argparse.Namespace:
    """CLI 인자를 파싱한다.

    Returns:
        파싱된 인자 Namespace
    """
    parser = argparse.ArgumentParser(
        description="Kafka 토픽 3개를 Spark Structured Streaming으로 S3 raw zone에 적재"
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=os.environ.get("BOOTSTRAP_SERVERS", "localhost:9092"),
        help="Kafka bootstrap server 주소 (기본값: BOOTSTRAP_SERVERS 환경변수 또는 localhost:9092)",
    )
    parser.add_argument(
        "--topic-impression",
        default="ad-impressions",
        help="노출 이벤트 토픽명 (기본값: ad-impressions)",
    )
    parser.add_argument(
        "--topic-click",
        default="ad-clicks",
        help="클릭 이벤트 토픽명 (기본값: ad-clicks)",
    )
    parser.add_argument(
        "--topic-conversion",
        default="ad-conversions",
        help="전환 이벤트 토픽명 (기본값: ad-conversions)",
    )
    parser.add_argument(
        "--s3-raw-base",
        default=os.environ.get("S3_RAW_BASE", "s3a://your-bucket/raw"),
        help="S3 raw zone 기본 경로",
    )
    parser.add_argument(
        "--checkpoint-base",
        default=os.environ.get("CHECKPOINT_BASE", "s3a://your-bucket/checkpoints/kafka_to_raw"),
        help="Spark 체크포인트 기본 경로",
    )
    parser.add_argument(
        "--starting-offsets",
        default="latest",
        choices=["latest", "earliest"],
        help="스트리밍 시작 offset. latest: 정상 운영, earliest: replay (JOBS_GUIDE.md §6 참고)",
    )
    parser.add_argument(
        "--topic-type",
        choices=["impression", "click", "conversion"],
        default=None,
        help="처리할 토픽 종류. 미지정 시 3개 토픽 동시 처리 (JOBS_GUIDE.md §6 참고)",
    )
    return parser.parse_args()


def build_spark() -> SparkSession:
    """Streaming 전용 SparkSession을 생성한다.

    Iceberg / Glue Catalog 설정 없이 Hadoop S3A만 구성한다.
    batch 잡의 spark_utils.build_spark()와 설정이 달라 별도로 유지한다.
    차이 설명은 JOBS_GUIDE.md §6 참고.

    Returns:
        설정이 완료된 SparkSession
    """
    logger.info("Streaming SparkSession 생성")
    return (
        SparkSession.builder
        .appName("KafkaToRaw")
        .config("spark.hadoop.fs.s3a.access.key",
                os.environ.get("AWS_ACCESS_KEY_ID", ""))
        .config("spark.hadoop.fs.s3a.secret.key",
                os.environ.get("AWS_SECRET_ACCESS_KEY", ""))
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        .getOrCreate()
    )


def read_kafka_topic(
    spark: SparkSession,
    topic: str,
    bootstrap_servers: str,
    starting_offsets: str,
) -> DataFrame:
    """Kafka 토픽을 readStream으로 구독한다.

    Args:
        spark: SparkSession
        topic: 구독할 Kafka 토픽명
        bootstrap_servers: Kafka bootstrap server 주소
        starting_offsets: 시작 offset ("latest" | "earliest")

    Returns:
        Kafka 원시 메시지 streaming DataFrame (value: binary, partition, offset, timestamp)
    """
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        # Kafka retention 만료로 offset 손실 시 에러 대신 스킵 — JOBS_GUIDE.md §6 참고
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_topic(raw_df: DataFrame, schema: StructType) -> DataFrame:
    """Kafka 원시 메시지를 지정 스키마로 역직렬화하고 적재 메타 컬럼을 추가한다.

    raw_date / raw_hour 파티션 기준(ingest_ts vs event timestamp)은 JOBS_GUIDE.md §6 참고.

    Args:
        raw_df: read_kafka_topic()의 반환값
        schema: 토픽별 JSON 스키마

    Returns:
        역직렬화된 이벤트 컬럼 + kafka_partition / kafka_offset / kafka_timestamp /
        ingest_ts / raw_date / raw_hour가 추가된 DataFrame
    """
    return (
        raw_df
        .select(
            from_json(col("value").cast("string"), schema).alias("data"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("timestamp").alias("kafka_timestamp"),
        )
        .select("data.*", "kafka_partition", "kafka_offset", "kafka_timestamp")
        .withColumn("ingest_ts", current_timestamp())
        .withColumn("raw_date", to_date(col("ingest_ts")))
        .withColumn("raw_hour", hour(col("ingest_ts")))
    )


def write_stream(
    df: DataFrame,
    output_path: str,
    checkpoint_path: str,
) -> StreamingQuery:
    """streaming DataFrame을 S3에 Parquet으로 쓰는 streaming query를 시작한다.

    raw_date / raw_hour Hive-style 파티션으로 저장해 후속 Athena 파티션 pruning을 지원한다.
    checkpoint_path는 토픽별 고유 경로를 사용해야 한다 — JOBS_GUIDE.md §6 참고.

    Args:
        df: parse_topic()의 반환값
        output_path: S3 출력 경로 (e.g. "s3a://bucket/raw/impressions")
        checkpoint_path: Spark 체크포인트 경로 (토픽별 고유 경로 필수)

    Returns:
        시작된 StreamingQuery
    """
    return (
        df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .partitionBy("raw_date", "raw_hour")
        .start()
    )


def main() -> None:
    args = parse_args()
    spark = build_spark()

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
        logger.info("Streaming query 시작: topic=%s, suffix=%s", topic_name, suffix)
        raw_df = read_kafka_topic(
            spark, topic_name, args.bootstrap_servers, args.starting_offsets
        )
        df = parse_topic(raw_df, schema)
        write_stream(
            df,
            f"{args.s3_raw_base}/{suffix}",
            f"{args.checkpoint_base}/{suffix}",
        )

    # 등록된 streaming query 중 하나라도 종료되면 main thread 해제 — JOBS_GUIDE.md §6 참고
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    main()
