from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("verify-eid3").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

RAW = "s3a://metacode-criteo-project/raw"

imp = spark.read.parquet(f"{RAW}/impressions")
clk = spark.read.parquet(f"{RAW}/clicks")

print("\n=== [1] RAW impression — eid=evt_00000003 ===")
imp.filter(col("eid") == "evt_00000003") \
   .select("eid", "timestamp", "ingest_ts", "kafka_timestamp") \
   .show(truncate=False)

print("\n=== [2] RAW click — eid=evt_00000003 ===")
clk.filter(col("eid") == "evt_00000003") \
   .select("eid", "timestamp", "ingest_ts", "kafka_timestamp") \
   .show(truncate=False)

print("\n=== [3] ingest_ts 차이 (click.ingest_ts - impression.ingest_ts, 초) ===")
i = imp.filter(col("eid") == "evt_00000003").select(col("ingest_ts").alias("i_ingest"))
c = clk.filter(col("eid") == "evt_00000003").select(col("ingest_ts").alias("c_ingest"))
if i.count() > 0 and c.count() > 0:
    i.crossJoin(c).select(
        col("i_ingest"),
        col("c_ingest"),
        (col("c_ingest").cast("long") - col("i_ingest").cast("long")).alias("diff_sec"),
    ).show(truncate=False)
else:
    print("  → impression 또는 click 중 하나가 raw에 없음")

print("\n=== [4] raw impression 총 건수 vs click 있는 impression 수 ===")
total_imp = imp.count()
imp_with_click = imp.join(
    clk.select("eid").distinct(), on="eid", how="inner"
).count()
print(f"  raw impressions 총합 : {total_imp:,}")
print(f"  click 매칭된 impression : {imp_with_click:,}")

print("\n=== [5] click.ingest_ts < impression.ingest_ts 인 건수 (역전 케이스) ===")
reversed_count = (
    imp.select(col("eid"), col("ingest_ts").alias("i_ts"))
    .join(clk.select(col("eid"), col("ingest_ts").alias("c_ts")), on="eid", how="inner")
    .filter(col("c_ts").cast("long") - col("i_ts").cast("long") < 0)
    .count()
)
print(f"  ingest_ts 역전 케이스 : {reversed_count:,}")

print("\n=== [6] click.ingest_ts - impression.ingest_ts > 60초 인 건수 (window 초과) ===")
over_window_count = (
    imp.select(col("eid"), col("ingest_ts").alias("i_ts"))
    .join(clk.select(col("eid"), col("ingest_ts").alias("c_ts")), on="eid", how="inner")
    .filter(col("c_ts").cast("long") - col("i_ts").cast("long") > 60)
    .count()
)
print(f"  window(60초) 초과 케이스 : {over_window_count:,}")

print("\n=== [7] RAW conversion — eid=evt_00000003 ===")
cvt = spark.read.parquet(f"{RAW}/conversions")
cvt.filter(col("eid") == "evt_00000003") \
   .select("eid", "timestamp", "ingest_ts", "kafka_timestamp") \
   .show(truncate=False)

print("\n=== [8] conversion ingest_ts 차이 (conv.ingest_ts - impression.ingest_ts, 초) ===")
cv = cvt.filter(col("eid") == "evt_00000003").select(col("ingest_ts").alias("cv_ingest"))
if i.count() > 0 and cv.count() > 0:
    i.crossJoin(cv).select(
        col("i_ingest"),
        col("cv_ingest"),
        (col("cv_ingest").cast("long") - col("i_ingest").cast("long")).alias("diff_sec"),
        # conv_window_sec = int(30 * 86400 * 0.01 / 100) = 259
    ).show(truncate=False)
    print("  conv_window_sec 기준: 259초")
else:
    print("  → conversion raw 없음 (eid=3은 click-only 이벤트)")

print("\n=== [9] 전체 누락 규모 요약 ===")
print(f"  raw impression 총합       : {total_imp:,}")
print(f"  click 있는 impression     : {imp_with_click:,}")
print(f"  click ingest 역전(< 0)    : {reversed_count:,}  → window filter가 impression 삭제")
print(f"  click ingest > 60초       : {over_window_count:,}  → window filter가 impression 삭제")
print(f"  click 관련 누락 예상       : {reversed_count + over_window_count:,}")

spark.stop()
