"""
processed_events(silver) → campaign_summary Iceberg (gold), 일단위 배치.
Airflow 자동화: run_date_end를 {{ ds }} 로 주입하면 일배치 스케줄링 됨.

[KPI 최신 유지 전략]
30일 lookback window로 매일 재집계 후 MERGE INTO 실행.
silver의 conversion_window(30일)와 동일한 범위를 읽으므로
window 내 지연 conversion이 반영된 시점에 campaign_summary도 자동 갱신됨.
30일 초과 과거분은 MERGE 대상 외이므로 전체 재계산 불필요.

[처리 지연 모니터링]
end_to_end_latency_sec 등 지연 컬럼은 campaign KPI 집계 범위 밖.
별도 monitoring.py에서 processed_events를 읽어 모니터링 테이블을 관리하는 것이 적합 (관심사 분리).

[향후 최적화]
매일 30일치 전체 재집계 대신, Iceberg snapshot diff로 silver의 변경 파티션만 감지해
해당 summary_date만 재집계하는 delta 방식으로 전환 가능.
"""

import argparse
import os
from datetime import date, timedelta

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
    # run_date_end 기본값은 어제 — 일배치이므로 매일 전날치까지를 처리
    # lookback_days는 silver의 conversion_window_days(30일)와 반드시 일치해야
    # window 내 지연 conversion이 모두 반영된 집계를 얻을 수 있음
    yesterday = (date.today() - timedelta(days=1)).isoformat()

    parser = argparse.ArgumentParser(
        description="processed_events(silver) → campaign_summary Iceberg (gold) 일단위 배치"
    )
    parser.add_argument(
        "--run-date-end",
        default=yesterday,
        help="집계 종료 날짜 (YYYY-MM-DD inclusive, 기본값: 어제). silver event_date 파티션 기준",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=30,
        help="집계 lookback 일수 (기본값: 30). silver conversion_window_days와 맞춰야 지연 conversion 누락 없음",
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
        help="지정 시 테이블 DROP 후 재적재. lookback window 내 데이터만 재적재되므로 전체 재구축 시 --lookback-days를 넓혀야 함",
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
            'format-version'               = '2',
            'write.update.mode'            = 'copy-on-write',
            'write.merge.mode'             = 'copy-on-write',
            'write.delete.mode'            = 'copy-on-write',
            'write.target-file-size-bytes' = '134217728'
        )
    """)


def aggregate(spark: SparkSession, run_date_end: str, lookback_days: int):
    # silver의 conversion_window(30일)와 동일한 lookback으로 읽어야
    # window 내 지연 conversion이 모두 반영된 집계를 얻을 수 있음
    start_date = (
        date.fromisoformat(run_date_end) - timedelta(days=lookback_days - 1)
    ).isoformat()

    df = (
        spark.table(SILVER_TABLE)
        .filter(
            (col("event_date") >= start_date) & (col("event_date") <= run_date_end)
        )
    )

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


def run_full_refresh(spark: SparkSession, staged) -> None:
    # DROP TABLE + 재생성: lookback window 내 데이터로만 재적재
    # 전체 이력 재구축 시 --lookback-days를 넓히거나 날짜 범위를 슬라이딩하며 복수 실행
    spark.sql(f"DROP TABLE IF EXISTS {FULL_NAME}")
    ensure_table(spark)
    staged.writeTo(FULL_NAME).append()


def run_merge(spark: SparkSession, staged) -> None:
    staged.createOrReplaceTempView("staged")
    # (summary_date, campaign) 복합키로 upsert
    # 매 실행마다 30일치 집계를 재계산하므로 MATCHED 시 모든 지표 갱신 (UPDATE SET *)
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

    staged = aggregate(spark, args.run_date_end, args.lookback_days)

    if args.full_refresh:
        run_full_refresh(spark, staged)
    else:
        run_merge(spark, staged)

    spark.stop()


if __name__ == "__main__":
    main()
