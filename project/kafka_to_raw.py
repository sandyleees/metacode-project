'''
kafka 브로커에 있는 impression/click/conversion 토픽을
Spark structured streaming으로 S3 raw에 저장
raw는 Iceberg 테이블이 아니라 append-only 파일 zone으로 둔다.
기본 포맷은 parquet이며 raw_date / raw_hour 단위로 저장한다.
'''
# 멱등성, 예외처리 고려 필요
# logging? 문제 발생 시 슬랙 등 알림 처리?

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, date_format, from_json, to_date, hour
from pyspark.sql.types import (
    DoubleType, IntegerType, LongType, StringType, StructField, StructType
)

# argparse로 받을 예정 - producer.py 패턴 따라 환경변수 → 하드코딩 기본값 순 폴백
BOOTSTRAP_SERVERS = os.environ.get("BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_IMPRESSION  = os.environ.get("TOPIC_IMPRESSION",  "ad-impressions")
TOPIC_CLICK       = os.environ.get("TOPIC_CLICK",       "ad-clicks")
TOPIC_CONVERSION  = os.environ.get("TOPIC_CONVERSION",  "ad-conversions")
S3_RAW_BASE       = os.environ.get("S3_RAW_BASE",       "s3a://your-bucket/raw")
CHECKPOINT_BASE   = os.environ.get("CHECKPOINT_BASE",   "s3a://your-bucket/checkpoints/kafka_to_raw")

# 해당 코드도 서버에 항상 띄워놔야하나? streaming이니까.
# → YES. Spark Structured Streaming은 long-running 프로세스
#   아래 awaitAnyTermination()이 main thread를 무한 블로킹 → 컨테이너가 항상 떠 있어야 함
#   운영에서는 YARN / K8s 이 크래시 감지 후 자동 재시작 처리

# 1. SparkSession 준비
spark = (
    SparkSession.builder
    .appName("KafkaToRawFiles")
    # spark 환경구성을 어떻게? - 제안. spark master와 worker 구분하는게 scale-out에 유리해보임
    # → master는 spark-submit --master yarn (또는 k8s://...) 으로 외부에서 주입하는 게 표준
    #   코드에 .master() 하드코딩하면 환경 바뀔 때마다 코드 수정 필요 → 생략이 관례
    #   로컬 테스트: spark-submit --master local[*] kafka_to_raw.py
    # 토픽 파티션 3개니까 spark worker 3개면 각 파티션 맡아서 병렬처리하는건가?
    # → 맞음. Kafka readStream은 Kafka 파티션 수만큼 Spark task를 생성
    #   파티션 3개 → task 3개 → executor 3개 이상이면 완전 병렬
    #   executor 수 < 파티션 수이면 Spark이 라운드로빈으로 스케줄링
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
        "org.apache.hadoop:hadoop-aws:3.3.4",
    ) # 이거 필요없다? - Dockerfile에서 JAR을 /opt/spark/jars에 넣어서?
    # S3(s3a://) 접근을 위한 hadoop-aws 설정
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    # AWS 자격증명: 코드 하드코딩 금지 → 환경변수(AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY) 사용
    # EC2 IAM 역할 환경이라면 아래 한 줄로 대체 가능
    # .config("spark.hadoop.fs.s3a.aws.credentials.provider",
    #         "com.amazonaws.auth.InstanceProfileCredentialsProvider")

    # 환경변수 AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY는 OS 레벨에 있어도, Hadoop S3A는 Spark config로 명시적으로 전달해야 읽음.
    .config("spark.hadoop.fs.s3a.access.key",
            os.environ.get("AWS_ACCESS_KEY_ID", ""))
    .config("spark.hadoop.fs.s3a.secret.key",
            os.environ.get("AWS_SECRET_ACCESS_KEY", ""))
    .config("spark.hadoop.fs.s3a.endpoint",
            "s3.amazonaws.com")
    .getOrCreate()
)

# 2-1. kafka에서 impression/click/conversion 읽어올 스키마 정의
# 브로커에서 데이터 읽어올 때 kafka 메타데이터:kafka_partition, kafka_offset, kafka_timestamp가져오려면 여기서 스키마 정의 필요?
# → 불필요. readStream.format("kafka") 결과에 partition/offset/timestamp 컬럼이 자동 포함됨
#   이 스키마는 kafka value(binary) → JSON 역직렬화 전용 - from_json(col("value"), schema)에서만 사용
# spark 메타데이터도 ingest_ts 말고 운영에 필요해보이는 다른 메타데이터 있으면 제안
# → 운영 필수 메타데이터:
#   - kafka_partition / kafka_offset : 재처리(replay)·중복 확인 기준점 - 이상 감지 시 어디서부터 다시 읽을지
#   - kafka_timestamp  : 브로커가 메시지를 수신한 시각 → producer.send() 시각과 비교해 consumer lag 측정
#   - ingest_ts        : Spark이 실제 처리한 시각 → Kafka-to-Spark 처리 지연 모니터링
#   추가로 source_topic을 달면 raw를 합쳐 조회할 때 출처 추적 가능 (parse_topic에서 lit(topic)으로 주입)
# 메타데이터는 밑에서 withColumn으로 추가하는건가?
# → kafka 메타데이터(partition/offset/timestamp)는 readStream 결과에 이미 컬럼으로 있어
#   select() 안에서 col("partition").alias("kafka_partition") 형태로 이름 바꿔 선택
#   ingest_ts처럼 Spark 처리 시각은 withColumn("ingest_ts", current_timestamp())으로 추가

basic_schema = StructType([
    StructField("eid",        StringType(),  nullable=False),
    StructField("uid",        StringType(),  nullable=False),
    StructField("timestamp",  LongType(),    nullable=False),  # 이벤트 발생 unix timestamp (초)
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

# 2-2. kafka에서 데이터 읽어오기
# readStream 사용, 토픽 3개에서 가져오려면 Spark.readStream코드 3개 생기는건가?
# → .option("subscribe", "topic-a,topic-b,topic-c") 로 한 readStream에 묶는 방법도 있음
#   하지만 impression에만 cost 필드가 있어 스키마가 달라 토픽별 별도 readStream이 명확함
#   readStream 3개 = streaming query 3개가 spark.streams에 등록되어 스레드별 동시 실행됨
# job마다 checkpoint 설정
# → checkpointLocation을 토픽별로 다른 경로로 지정 필수
#   같은 경로 공유 시 서로 다른 토픽의 offset 진행 상태가 섞여 재처리 시 누락 또는 중복 발생


def read_kafka_topic(topic: str):
    # startingOffsets=latest: 처음 실행 시 기존 누적 메시지 무시하고 최신부터 소비
    #   replay 목적이라면 earliest로 변경 (retention 범위 내 전체 재처리)
    # failOnDataLoss=false: Kafka retention 만료 등으로 offset 손실 시 에러 대신 스킵
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
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
        .withColumn("raw_hour", hour(col("ingest_ts")))
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


# 3. 토픽별 streaming query 구성
imp_raw   = read_kafka_topic(TOPIC_IMPRESSION)
click_raw = read_kafka_topic(TOPIC_CLICK)
conv_raw  = read_kafka_topic(TOPIC_CONVERSION)

imp_df   = parse_topic(imp_raw,   impression_schema)
click_df = parse_topic(click_raw, click_schema)
conv_df  = parse_topic(conv_raw,  conversion_schema)

# 4. S3에 쓰기 - 각 토픽마다 독립 streaming query 시작
imp_query   = write_stream(imp_df,   f"{S3_RAW_BASE}/impressions",  f"{CHECKPOINT_BASE}/impressions")
click_query = write_stream(click_df, f"{S3_RAW_BASE}/clicks",       f"{CHECKPOINT_BASE}/clicks")
conv_query  = write_stream(conv_df,  f"{S3_RAW_BASE}/conversions",  f"{CHECKPOINT_BASE}/conversions")

# 5. 세 streaming query가 종료될 때까지 main thread 대기
# awaitAnyTermination(): 3개 쿼리 중 하나라도 종료(에러 or 명시적 stop)되면 main thread 해제
# 정상 운영 중에는 영구 블로킹 → 컨테이너(프로세스)가 항상 살아있어야 함
# 컨테이너가 exit하면 YARN/K8s의 supervisor가 재시작 처리
spark.streams.awaitAnyTermination()
