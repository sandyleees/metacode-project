"""processed_events (Silver) → campaign_summary Iceberg (Gold) 일단위 배치.

두 가지 실행 모드(snapshot diff / 명시 재처리), COW 선택 이유, KPI 계산식 설계는
JOBS_GUIDE.md §3 참고.
"""

import argparse
import logging
import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg, col, count, count_distinct, current_timestamp, lit,
    sum as spark_sum, when,
)

from spark_utils import build_spark

logger = logging.getLogger(__name__)

CATALOG = "glue_catalog"
DATABASE = "gold"
TABLE = "campaign_summary"
FULL_NAME = f"{CATALOG}.{DATABASE}.{TABLE}"

SILVER_TABLE = f"{CATALOG}.silver.processed_events"


def parse_args() -> argparse.Namespace:
    """CLI 인자를 파싱하고 날짜 범위 유효성을 검사한다.

    Returns:
        파싱된 인자 Namespace

    Raises:
        SystemExit: --run-date-start와 --run-date-end가 모두 지정됐는데 start > end인 경우
    """
    yesterday = (date.today() - timedelta(days=1)).isoformat()

    parser = argparse.ArgumentParser(
        description="processed_events (Silver) → campaign_summary Iceberg (Gold) 일단위 배치"
    )
    parser.add_argument(
        "--run-date-end",
        default=yesterday,
        help="집계 종료 날짜 (YYYY-MM-DD, 기본값: 어제). "
             "명시 재처리 모드에서는 event_date 상한.",
    )
    parser.add_argument(
        "--run-date-start",
        default=None,
        help="명시 재처리 시작 날짜 (YYYY-MM-DD). "
             "지정 시 명시 재처리 모드, 미지정 시 snapshot diff 모드.",
    )
    parser.add_argument(
        "--glue-warehouse",
        default=os.environ.get("GLUE_WAREHOUSE", "s3a://your-bucket/warehouse"),
        help="Iceberg warehouse S3 경로",
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        default=False,
        help="테이블 전체를 원자적으로 교체. --run-date-start / --run-date-end 범위로 재구축.",
    )

    args = parser.parse_args()

    if args.run_date_start and args.run_date_start > args.run_date_end:
        parser.error(
            f"--run-date-start ({args.run_date_start}) 가 "
            f"--run-date-end ({args.run_date_end}) 보다 늦습니다."
        )

    return args


def ensure_table(spark: SparkSession) -> None:
    """Gold 데이터베이스와 campaign_summary 테이블이 없으면 생성한다.

    스키마 정의는 jobs/ddl/gold_campaign_summary.sql 참고.
    """
    ddl_path = Path(__file__).parent / "ddl" / "gold_campaign_summary.sql"
    for stmt in _parse_sql(ddl_path.read_text(encoding="utf-8")):
        spark.sql(stmt)


def _parse_sql(text: str) -> list[str]:
    """SQL 파일을 세미콜론 기준으로 분리하고 -- 주석 줄을 제거한다."""
    lines = [ln for ln in text.splitlines() if not ln.strip().startswith("--")]
    return [s.strip() for s in "\n".join(lines).split(";") if s.strip()]


def get_changed_event_dates(spark: SparkSession) -> DataFrame:
    """오늘 Silver에 커밋된 변경 event_date 목록을 Iceberg snapshot diff로 조회한다.

    snapshot diff 방식 설계 배경과 UTC 타임존 주의사항은 JOBS_GUIDE.md §3-1, §5-E5 참고.

    Args:
        spark: SparkSession

    Returns:
        오늘 변경된 event_date 값들의 DataFrame (컬럼: event_date)
    """
    today = date.today()
    start_ms = int(
        datetime(today.year, today.month, today.day, tzinfo=timezone.utc).timestamp() * 1000
    )
    end_ms = start_ms + 86_400_000

    return (
        spark.read.format("iceberg")
        .option("start-timestamp", start_ms)
        .option("end-timestamp", end_ms)
        .load(f"{SILVER_TABLE}.changes")
        .select("event_date")
        .distinct()
    )


def aggregate(df: DataFrame) -> DataFrame:
    """Silver 행을 (event_date, campaign) 기준으로 집계해 Gold KPI를 계산한다.

    KPI 계산식과 분모 0 처리 설계는 JOBS_GUIDE.md §3-4 참고.

    Args:
        df: Silver processed_events DataFrame (event_date 필터 적용 전제)

    Returns:
        Gold campaign_summary 스키마에 맞는 집계 DataFrame
    """
    impressions_col = count("*")
    clicks_col = spark_sum("click")
    conversions_col = spark_sum("conversion")
    cost_col = spark_sum("cost")
    uniq_users_col = count_distinct("uid")

    return df.groupBy(
        col("event_date").alias("summary_date"),
        col("campaign"),
    ).agg(
        impressions_col.alias("impressions"),
        clicks_col.alias("clicks"),
        conversions_col.alias("conversions"),
        uniq_users_col.alias("unique_users"),
        count_distinct(when(col("conversion") == 1, col("uid"))).alias("converting_users"),
        cost_col.alias("total_cost"),

        (clicks_col.cast("double") / impressions_col * 100).alias("ctr"),
        when(clicks_col > 0, conversions_col.cast("double") / clicks_col * 100).alias("cvr"),
        when(clicks_col > 0, cost_col / clicks_col.cast("double")).alias("cpc"),
        when(conversions_col > 0, cost_col / conversions_col.cast("double")).alias("cpa"),
        (cost_col / impressions_col * 1000).alias("cpm"),

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


def run_full_refresh(staged: DataFrame) -> None:
    """staged DataFrame으로 Gold 테이블 전체를 원자적으로 교체한다.

    Args:
        staged: Gold 스키마에 맞는 전체 데이터 DataFrame
    """
    staged.writeTo(FULL_NAME).createOrReplace()


def run_merge(spark: SparkSession, staged: DataFrame) -> None:
    """staged DataFrame을 (summary_date, campaign) 복합키로 Gold 테이블에 UPSERT한다.

    Args:
        spark: SparkSession
        staged: Gold 스키마에 맞는 집계 DataFrame
    """
    staged.createOrReplaceTempView("staged")
    spark.sql(f"""
        MERGE INTO {FULL_NAME} t
        USING staged s
        ON t.summary_date = s.summary_date AND t.campaign = s.campaign
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def main() -> None:
    args = parse_args()

    spark = build_spark("ProcessedToCampaignSummary", CATALOG, args.glue_warehouse)
    ensure_table(spark)

    if args.run_date_start:
        logger.info(
            "명시 재처리 모드: event_date %s ~ %s", args.run_date_start, args.run_date_end
        )
        df = spark.table(SILVER_TABLE).filter(
            (col("event_date") >= args.run_date_start)
            & (col("event_date") <= args.run_date_end)
        )
    else:
        logger.info("일배치 모드: snapshot diff 기반 변경 event_date 집계")
        changed_dates = get_changed_event_dates(spark)
        df = spark.table(SILVER_TABLE).join(changed_dates, on="event_date", how="inner")

    staged = aggregate(df)

    if args.full_refresh:
        run_full_refresh(staged)
    else:
        run_merge(spark, staged)

    spark.stop()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    main()
