"""
processed_events(silver) → campaign_summary Iceberg (gold), 일단위 배치.
Airflow 자동화: run_date_end를 {{ ds }} 로 주입하면 일배치 스케줄링 됨.

[KPI 최신 유지 전략 — 2가지 모드]

일배치 모드 (--run-date-start 미지정):
  Silver의 오늘 커밋 변경분을 Iceberg snapshot diff로 감지해 해당 event_date만 재집계.
  지연 attribution으로 인한 오래된 event_date 파티션 갱신도 lookback 제한 없이 자동 포함.
  Silver와 Gold가 같은 날 실행된다는 전제 (Airflow DAG 의존성으로 보장).

명시 재처리 모드 (--run-date-start 지정):
  지정 event_date 범위를 Silver 현재 상태 기준으로 재집계.
  Gold 로직 버그 수정 후 재집계, 특정 기간 강제 재처리 등에 사용.
  Silver snapshot 이력과 무관하게 독립 실행 가능.

[처리 지연 모니터링]
end_to_end_latency_sec 등 지연 컬럼은 campaign KPI 집계 범위 밖.
별도 monitoring.py에서 processed_events를 읽어 모니터링 테이블을 관리하는 것이 적합 (관심사 분리).
"""

import argparse
import os
from datetime import date, datetime, timedelta, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg, col, count, count_distinct, current_timestamp, lit,
    sum as spark_sum, when,
)


CATALOG      = "glue_catalog"
DATABASE     = "gold"
TABLE        = "campaign_summary"
FULL_NAME    = f"{CATALOG}.{DATABASE}.{TABLE}"

SILVER_TABLE = f"{CATALOG}.silver.processed_events"


def parse_args():
    yesterday = (date.today() - timedelta(days=1)).isoformat()

    parser = argparse.ArgumentParser(
        description="processed_events(silver) → campaign_summary Iceberg (gold) 일단위 배치"
    )
    parser.add_argument(
        "--run-date-end",
        default=yesterday,
        help="집계 종료 날짜 (YYYY-MM-DD, 기본값: 어제). "
             "일배치 모드에서는 로깅 참조용. 명시 재처리 모드에서는 event_date 상한.",
    )
    parser.add_argument(
        "--run-date-start",
        default=None,
        help="명시 재처리 시작 날짜 (YYYY-MM-DD). "
             "지정 시 해당 event_date 범위를 Silver 현재 상태 기준으로 재집계 (명시 재처리 모드). "
             "미지정 시 snapshot diff 모드로 동작.",
    )
    parser.add_argument(
        "--glue-warehouse",
        default=os.environ.get("GLUE_WAREHOUSE", "s3a://your-bucket/warehouse"),
        help="Iceberg warehouse 경로 (Glue catalog 메타스토어가 실제 데이터를 쓰는 S3 경로)",
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        default=False,
        help="지정 시 테이블 전체를 원자적으로 교체 (createOrReplace). "
             "--run-date-start / --run-date-end 범위 데이터로 재구축.",
    )

    return parser.parse_args()


def build_spark(glue_warehouse: str) -> SparkSession:
    # Iceberg + Glue Catalog SparkSession 설정 (raw_to_processed_iceberg.py와 동일한 구성)
    # 상세 설정 설명은 raw_to_processed_iceberg.py의 build_spark 주석 참고
    return (
        SparkSession.builder
        .appName("ProcessedToCampaignSummary")
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


def ensure_table(spark: SparkSession) -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG}.{DATABASE}")
    # summary_date: 파티션 키 — 일단위 DATE로 충분
    # ROAS 제외: revenue 데이터 없음
    # cvr/cpc/cpa: 분모 0이면 NULL (aggregate 함수에서 when으로 보호)
    # ctr/cpm/frequency: 분모인 impressions·unique_users는 그룹 존재 시 항상 >= 1이므로 NULL 불발생
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {FULL_NAME} (
            summary_date                DATE      NOT NULL COMMENT 'event_date 기준 집계 날짜; 파티션 키',
            campaign                    INT       NOT NULL COMMENT '캠페인 식별자',

            impressions                 BIGINT             COMMENT 'COUNT(*) — 노출 수',
            clicks                      BIGINT             COMMENT 'SUM(click)',
            conversions                 BIGINT             COMMENT 'SUM(conversion)',
            unique_users                BIGINT             COMMENT 'COUNT(DISTINCT uid); reach·frequency 분모',
            converting_users            BIGINT             COMMENT 'COUNT(DISTINCT uid WHERE conversion=1)',
            total_cost                  DOUBLE             COMMENT 'SUM(cost)',

            ctr                         DOUBLE             COMMENT 'clicks / impressions × 100',
            cvr                         DOUBLE             COMMENT 'conversions / clicks × 100; clicks=0이면 NULL',
            cpc                         DOUBLE             COMMENT 'total_cost / clicks; clicks=0이면 NULL',
            cpa                         DOUBLE             COMMENT 'total_cost / conversions; conversions=0이면 NULL',
            cpm                         DOUBLE             COMMENT 'total_cost / impressions × 1000; 노출형 광고 필수 지표',

            click_through_conversions   BIGINT             COMMENT 'COUNT(click=1 AND conversion=1)',
            view_through_conversions    BIGINT             COMMENT 'COUNT(click=0 AND conversion=1)',
            avg_conversion_delay_sec    DOUBLE             COMMENT 'AVG(conversion_delay_sec >= 0); 전환 없는 행(-1 sentinel) 제외',

            frequency                   DOUBLE             COMMENT 'impressions / unique_users',

            updated_at                  TIMESTAMP          COMMENT 'MERGE 실행 시각'
        )
        USING iceberg
        PARTITIONED BY (summary_date)
        TBLPROPERTIES (
            'format-version'                         = '2',
            'write.merge.mode'                       = 'copy-on-write',
            'write.target-file-size-bytes'           = '134217728',
            'write.metadata.previous-versions-max'   = '7',
            'write.metadata.delete-after-commit.enabled' = 'true'
        )
    """)
    # write.merge.mode COW 유지: Gold MERGE는 (summary_date, campaign) 전체를 매 실행마다 갱신.
    # write.metadata.previous-versions-max=7 + delete-after-commit.enabled:
    #   Gold는 하루 1커밋 → 7 = 7일치 metadata.json 보존 (Silver의 21과 캘린더 기준 통일).
    #   expire_snapshots(30일) > metadata.json 보존(7일) → 복구 시 스냅샷 데이터 파일 생존 보장.
    # write.update.mode / write.delete.mode 미설정: standalone UPDATE/DELETE 없음.


def get_changed_event_dates(spark: SparkSession):
    # 일배치 모드: 오늘 Silver에 커밋된 변경 event_date 수집 (Iceberg snapshot diff)
    # Silver와 Gold가 같은 날 실행된다는 전제 — Airflow DAG 의존성으로 보장.
    # Silver Stage 1/2 커밋, Silver 백필 커밋 모두 오늘 timestamp로 포함.
    # 변경 감지에 데이터 파일 스캔 불필요 — manifest 메타데이터만 읽음.
    today = date.today()
    start_ms = int(datetime(today.year, today.month, today.day, tzinfo=timezone.utc).timestamp() * 1000)
    end_ms   = start_ms + 86_400_000  # 하루 = 86400초 × 1000ms

    return (
        spark.read.format("iceberg")
        .option("start-timestamp", start_ms)
        .option("end-timestamp",   end_ms)
        .load(f"{SILVER_TABLE}.changes")
        .select("event_date")
        .distinct()
    )


def aggregate(df):
    # 여러 KPI 식에서 재사용하는 집계 표현식 — Catalyst optimizer가 그룹당 한 번만 계산
    impressions_col = count("*")
    clicks_col      = spark_sum("click")
    conversions_col = spark_sum("conversion")
    cost_col        = spark_sum("cost")
    uniq_users_col  = count_distinct("uid")

    return df.groupBy(
        col("event_date").alias("summary_date"),
        col("campaign"),
    ).agg(
        impressions_col.alias("impressions"),
        clicks_col.alias("clicks"),
        conversions_col.alias("conversions"),
        uniq_users_col.alias("unique_users"),
        # when()이 conversion != 1인 uid에 null 반환 → count_distinct가 null 제외하고 집계
        count_distinct(when(col("conversion") == 1, col("uid"))).alias("converting_users"),
        cost_col.alias("total_cost"),

        # impressions·unique_users는 그룹 존재 시 항상 >= 1 → 분모 0 보호 불필요
        (clicks_col.cast("double") / impressions_col * 100).alias("ctr"),
        when(clicks_col > 0, conversions_col.cast("double") / clicks_col * 100).alias("cvr"),
        when(clicks_col > 0, cost_col / clicks_col.cast("double")).alias("cpc"),
        when(conversions_col > 0, cost_col / conversions_col.cast("double")).alias("cpa"),
        (cost_col / impressions_col * 1000).alias("cpm"),

        # count(when(...)) : when 조건 불일치 행은 null 반환 → count가 null 제외
        count(when((col("click") == 1) & (col("conversion") == 1), lit(1)))
            .alias("click_through_conversions"),
        count(when((col("click") == 0) & (col("conversion") == 1), lit(1)))
            .alias("view_through_conversions"),
        # conversion_delay_sec = -1 은 전환 없음 sentinel (raw_to_processed_iceberg.py 정의)
        avg(when(col("conversion_delay_sec") >= 0, col("conversion_delay_sec")))
            .alias("avg_conversion_delay_sec"),

        (impressions_col.cast("double") / uniq_users_col).alias("frequency"),

        current_timestamp().alias("updated_at"),
    )


def run_full_refresh(staged) -> None:
    # createOrReplace(): Iceberg 원자적 테이블 교체
    #   - Glue catalog 엔트리 유지 (DROP TABLE과 달리 Athena/Superset 참조 유지)
    #   - 교체 전 스냅샷은 expire 전까지 time travel 가능
    #   - 실패 시 이전 상태 보존 (원자성 보장)
    staged.writeTo(FULL_NAME).createOrReplace()


def run_merge(spark: SparkSession, staged) -> None:
    staged.createOrReplaceTempView("staged")
    # (summary_date, campaign) 복합키로 upsert
    # 변경된 event_date 기준으로 재집계한 결과 → MATCHED 시 모든 지표 갱신
    spark.sql(f"""
        MERGE INTO {FULL_NAME} t
        USING staged s
        ON t.summary_date = s.summary_date AND t.campaign = s.campaign
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def main():
    args = parse_args()

    spark = build_spark(args.glue_warehouse)
    ensure_table(spark)

    if args.run_date_start:
        # 명시 재처리 모드: event_date 범위를 Silver 현재 상태 기준으로 재집계
        # Silver snapshot 이력과 무관 → Gold 독립 재처리 가능, MERGE 멱등
        df = (
            spark.table(SILVER_TABLE)
            .filter(
                (col("event_date") >= args.run_date_start)
                & (col("event_date") <= args.run_date_end)
            )
        )
    else:
        # 일배치 모드: 오늘 Silver 커밋 변경 event_date만 재집계 (snapshot diff)
        # 지연 attribution으로 갱신된 오래된 event_date도 lookback 제한 없이 포함
        changed_dates = get_changed_event_dates(spark)
        df = spark.table(SILVER_TABLE).join(changed_dates, on="event_date", how="inner")

    staged = aggregate(df)

    if args.full_refresh:
        run_full_refresh(staged)
    else:
        run_merge(spark, staged)

    spark.stop()


if __name__ == "__main__":
    main()
