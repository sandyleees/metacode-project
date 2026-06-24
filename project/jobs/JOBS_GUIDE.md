# jobs/ 설계 가이드

코드를 읽다 "왜 이렇게 했지?"라는 의문이 생길 때 찾아보는 문서.  
코드의 **무엇(what)** 은 코드 자체와 docstring이 설명하고, 이 문서는 **왜(why)** 를 설명한다.

---

## 목차

1. [아키텍처 개요](#1-아키텍처-개요)
2. [raw_to_processed_iceberg.py — Silver 배치](#2-raw_to_processed_icebergpy--silver-배치)
   - 2-1. 왜 2-Stage인가
   - 2-2. Attribution Window
   - 2-3. full_refresh 시 raw_date 범위 확장
   - 2-4. Silver 지연 측정 컬럼
   - 2-5. createOrReplace() 원자성
   - 2-6. Iceberg MOR 선택 이유
   - 2-7. TBLPROPERTIES 설계 근거
   - 2-8. Soft failure 감지
3. [processed_to_campaign_summary.py — Gold 배치](#3-processed_to_campaign_summarypy--gold-배치)
   - 3-1. 두 가지 실행 모드
   - 3-2. Snapshot diff 방식 이유
   - 3-3. Iceberg COW 선택 이유
   - 3-4. KPI 계산식과 분모 0 처리
   - 3-5. createOrReplace() 원자성
4. [spark_utils.py — Glue + S3FileIO 자격증명 구조](#4-spark_utilspy--glue--s3fileio-자격증명-구조)
5. [운영 함정 — 처음에 놓치기 쉬운 것들](#5-운영-함정--처음에-놓치기-쉬운-것들)
6. [kafka_to_raw.py — Bronze Streaming 적재](#6-kafka_to_rawpy--bronze-streaming-적재)
   - 6-9. Bronze 추가 컬럼과 보존 정책

---

## 1. 아키텍처 개요

```
Bronze (S3 raw Parquet, append-only)
    └─ raw/impressions/raw_date=YYYY-MM-DD/raw_hour=HH/
    └─ raw/clicks/
    └─ raw/conversions/
         │
         │  raw_to_processed_iceberg.py (Silver 배치)
         ▼
Silver (Iceberg, MOR)
    └─ warehouse/silver/processed_events/   ← event_date 파티션
         │
         │  processed_to_campaign_summary.py (Gold 배치)
         ▼
Gold (Iceberg, COW)
    └─ warehouse/gold/campaign_summary/     ← summary_date 파티션
```

Bronze는 Kafka at-least-once 수집이므로 중복이 있다.  
Silver에서 dedup + attribution join을 수행해 분석 가능한 행 단위로 만든다.  
Gold는 Silver를 campaign + date 기준으로 집계한 KPI 테이블이다.

---

## 2. raw_to_processed_iceberg.py — Silver 배치

> 스키마 정의: `jobs/ddl/silver_processed_events.sql`

### 2-1. 왜 2-Stage인가 (`run_impression_stage` / `run_attribution_stage`)

**문제:** click은 impression 발생 후 며칠 뒤에 수집된다.

예를 들어 6월 1일 impression에 대한 click이 6월 3일에 브로커에 도착했다면,
이 click의 `raw_date`는 `2026-06-03`이다.
6월 1일 배치에서 `raw_date = 2026-06-01` 범위만 읽으면 이 click을 볼 수 없다.

**해결책 — 2-Stage:**

- **Stage 1** (`run_impression_stage`): 오늘 수집된 impression을 Silver에 INSERT.
  click과 conversion은 아직 모르므로 `click=0`, `conversion=0`, `conversion_timestamp=-1`로 초기화.

- **Stage 2** (`run_attribution_stage`): 오늘 수집된 click/conversion을 Silver에서 `eid`로 조회해 MERGE UPDATE.
  Silver에 이미 있는 impression 행을 기준으로 attribution window를 판정하므로, impression의 `raw_date`와 무관하게 매칭된다.

**일반 배치 vs full_refresh:**

| | 일반 배치 | full_refresh |
|---|---|---|
| 방식 | Stage 1 → Stage 2 | `transform()` 단일 join → `createOrReplace()` |
| 용도 | 매일 운영 | 전체 재구축, 스키마 변경 후 마이그레이션 |
| click/conversion raw_date | 오늘만 읽음 | window만큼 확장해서 읽음 (§2-3 참고) |

---

### 2-2. Attribution Window — 비즈니스 로직과 파티션 pruning 힌트

**Attribution window란?**

광고 클릭 또는 전환이 "이 impression 덕분"으로 인정받는 시간 한도.

- **click**: impression 후 7일 이내 — 업계 표준 (Google, Meta 동일 기준)
- **conversion**: impression 후 30일 이내 — 업계 표준

```python
# click attribution 판정 조건 (run_attribution_stage 내부)
(col("c.timestamp") - col("s.event_time").cast("long")).between(0, click_window_sec)
```

`c.timestamp`와 `s.event_time`은 모두 unix 초 단위이므로 차이가 경과 초와 일치한다.  
`>= 0` 조건은 click이 반드시 impression 이후에 발생했음을 보장한다.

**왜 lookback 기준이 `run_date_end`(수집 날짜)가 아니라 `event_time`(이벤트 발생 시각)인가?**

수집 지연이 있어도 실제 발생 시각 기준으로 window를 판정해야 정확하다.  
producer는 전송 타이밍만 압축할 뿐 메시지 timestamp는 Criteo 원본 상대시간 그대로이므로,  
`ingest_ts` / `kafka_timestamp` 기반 압축 window가 아닌 원본 timestamp 기준 window를 사용한다.

**파티션 pruning 힌트 — 왜 조건이 두 개인가?**

```python
# 조건 A: attribution window 판정 (비즈니스 로직)
& (col("c.timestamp") - col("s.event_time").cast("long")).between(0, click_window_sec)
# 조건 B: Silver event_date 파티션 pruning 힌트 (성능)
& (col("s.event_date") >= to_date((col("c.timestamp") - click_window_sec).cast("timestamp")))
```

조건 A만 있으면 논리적으로는 맞지만, Iceberg가 스캔 전에 파티션을 제거(pruning)하지 못한다.  
두 동적 컬럼의 연산 결과를 스캔 전에 알 수 없기 때문이다.

조건 B는 파티션 키(`event_date`)를 직접 리터럴과 비교하므로, Spark DFP(Dynamic File Pruning)가 인식해  
불필요한 파티션을 열기 전에 차단한다.  
조건 A와 논리적으로 중복이지만, 성능을 위해 명시적으로 추가한다.

`(c.timestamp - click_window_sec).cast("timestamp")` → click 기준 window 하한을 unix 초에서 Timestamp로 변환  
`to_date(...)` → Timestamp를 DATE로 변환해 Silver의 `event_date` 파티션 키 타입과 일치시킴

---

### 2-3. full_refresh 시 raw_date 범위 확장

일반 배치에서는 오늘 도착한 click/conversion만 Stage 2에서 처리하면 된다.  
그러나 full_refresh는 "처음부터 전부 다시 만들기"이므로,  
run_date_end 이후에 수집된 window 내 click/conversion도 포함해야 한다.

예: `run_date_end = 2026-06-01`, `click_window_days = 7`이면  
6월 1일 impression에 대한 click이 6월 8일까지 도착할 수 있으므로  
click의 `raw_date` 읽기 범위를 `2026-06-08`까지 확장한다.

```python
click_raw_end = run_date_end + timedelta(days=click_window_days)   # 7일 확장
conv_raw_end  = run_date_end + timedelta(days=conversion_window_days)  # 30일 확장
```

이 확장이 없으면 full_refresh 결과가 일반 배치 결과보다 click/conversion이 적어진다.

---

### 2-4. Silver 지연 측정 컬럼

Silver는 세 단계의 지연을 각 행에 기록한다.

| 컬럼 | 측정 구간 | 의미 |
|---|---|---|
| `producer_to_broker_sec` | `kafka_timestamp - event_time` | 프로듀서 발행 → Kafka 브로커 수신 지연 |
| `broker_to_ingest_sec` | `ingest_ts - kafka_timestamp` | Kafka 브로커 수신 → Spark ingest 지연 |
| `end_to_end_latency_sec` | `ingest_ts - event_time` | 이벤트 발생 → Spark ingest 전체 지연 |

세 값을 분리해 기록하는 이유: `end_to_end`가 높아졌을 때 어느 단계(네트워크 vs 소비자 처리)가 원인인지 즉시 구분하기 위해서다.  
`health-queries/ops/04_pipeline_latency_trend.sql`이 이 세 컬럼의 p50 추이를 시각화한다.

---

### 2-5. `createOrReplace()` — full_refresh 원자성

full_refresh는 `transform()`으로 Silver 전체를 재계산한 후 `createOrReplace()`로 기존 테이블을 교체한다.

```python
df.writeTo(FULL_NAME).tableProperty(...).createOrReplace()
```

`createOrReplace()`는 내부적으로 새 파일을 먼저 쓴 다음 메타데이터를 원자적으로 교체한다.  
`DROP TABLE` + `CREATE TABLE` + 재적재 방식과 달리, 교체 완료 전까지 기존 테이블이 유효하므로  
Athena 쿼리가 중간에 빈 테이블을 보는 순간이 없다.

---

### 2-6. Iceberg MOR 선택 이유

Silver에는 두 종류의 쓰기가 발생한다:

| 쓰기 종류 | Iceberg 동작 | 파일 처리 |
|---|---|---|
| `WHEN NOT MATCHED INSERT` | append | COW/MOR 무관, 항상 새 파일 |
| `WHEN MATCHED UPDATE` | MOR: delta 파일 append | COW: 파일 전체 재작성 |

광고 데이터에서 UPDATE 대상(click 또는 conversion이 나중에 도착한 impression)은  
전체 impression의 약 2% 미만이다.  
COW를 쓰면 2%의 행 때문에 해당 파티션 파일 전체를 재작성해야 한다.  
MOR는 delta 파일만 append하므로 쓰기가 훨씬 빠르다.

읽기 시 base + delta 병합 오버헤드는 Airflow `criteo_maintenance_dag.py`의  
daily compaction(`compact_silver`)이 흡수한다.

---

### 2-7. TBLPROPERTIES 설계 근거

**`write.metadata.previous-versions-max = 21`**

Iceberg는 커밋마다 `metadata.json` 파일을 새로 쓴다.  
오래된 `metadata.json`이 무한정 쌓이면 S3 비용과 목록 조회 속도에 영향을 준다.

Silver는 하루 3커밋 (Stage 1 impression + Stage 2 click + Stage 2 conversion)이므로:
```
21개 ÷ 3커밋/일 = 7일치 metadata.json 보존
```

`expire_snapshots`는 30일 기준으로 실행된다.  
`metadata.json` 보존(7일) < 스냅샷 보존(30일) 이므로, 복구 시 스냅샷 데이터 파일이 살아있다.  
이 부등식이 깨지면(`metadata.json` 보존 기간 > 스냅샷 보존 기간) time travel이 불안정해진다.

Gold는 하루 1커밋이므로 `previous-versions-max = 7`로 동일하게 7일을 보존한다.

**`write.target-file-size-bytes = 134217728` (128MB)**

Parquet 파일 1개당 128MB를 목표로 한다.  
너무 작으면 S3 목록 조회 overhead, 너무 크면 파티션 내 병렬 처리가 안 된다.  
128MB는 Iceberg 권장값이자 이 데이터 규모에서 검증된 값이다.

---

### 2-8. Soft failure 감지 (`_assert_attribution_matched`)

Stage 2에서 Bronze에 click이 있는데 Silver impression 매칭이 0건이면 조용히 성공처럼 보인다.  
Airflow는 MERGE가 0건 업데이트를 해도 오류로 인식하지 않는다.

이를 잡기 위해 MERGE 전에 명시적으로 카운트를 비교한다:

```python
if bronze_count > 0 and matched_count == 0:
    raise RuntimeError(...)
```

**한계:** 임계값이 "0건"이다. 매칭률이 5%여도 통과한다.  
이 한계는 의도적 결정이다 — 정상 운영에서도 attribution window 밖의 click/conversion은 매칭되지 않아  
매칭률 100%를 기대할 수 없기 때문이다. 더 정교한 임계값이 필요하면 §5-E3을 참고하라.

**`.cache()` / `.unpersist()` 패턴:**

```python
clk.cache()
_assert_attribution_matched("click", clk.count(), click_updates.count())
# ... MERGE 실행 (clk가 cached 상태) ...
clk.unpersist()
```

`clk.count()`와 `click_updates.count()`가 모두 Spark action이므로,  
`cache()` 없이 실행하면 `clk` DataFrame이 두 번 재계산된다.  
`cache()`를 MERGE 이전에 해제하지 않고 MERGE 완료 후 `unpersist()`하는 이유는,  
MERGE SQL이 실행될 때도 `clk` (click_updates의 원본)가 재사용되기 때문이다.

---

## 3. processed_to_campaign_summary.py — Gold 배치

> 스키마 정의: `jobs/ddl/gold_campaign_summary.sql`

### 3-1. 두 가지 실행 모드

**일배치 모드** (`--run-date-start` 미지정):

오늘 Silver에 커밋된 변경분만 Iceberg snapshot diff로 감지해 해당 event_date만 재집계한다.  
지연 attribution(지난주 impression에 오늘 click이 매칭됨)으로 갱신된 오래된 event_date도  
lookback 제한 없이 포함된다.

전제: Silver와 Gold가 같은 날 실행된다 — Airflow `criteo_medallion_dag.py`의 DAG 의존성으로 보장.

**명시 재처리 모드** (`--run-date-start` 지정):

Silver 현재 상태 기준으로 지정 event_date 범위를 재집계한다.  
Silver snapshot 이력과 무관하므로 Gold 단독 재처리가 가능하다.

```bash
# 예: Gold 로직 버그 수정 후 6월 한 달치 재처리
docker compose --profile batch run --rm processed-to-summary \
  /opt/spark/bin/spark-submit ... \
  --run-date-start 2026-06-01 --run-date-end 2026-06-30
```

---

### 3-2. Snapshot diff 방식 이유 (`get_changed_event_dates`)

**왜 Silver 전체를 읽지 않는가?**

Silver에 6개월치 데이터가 쌓여 있다고 가정하면,  
오늘 변경된 event_date는 1~2개뿐인데 전체를 읽으면 과도한 S3 스캔이 발생한다.

**왜 before/after snapshot 행 수 비교인가?**

직관적으로는 Iceberg changelog scan(`.changes`)이 가장 적합해 보이지만, 두 가지 이유로 사용 불가다:

- **`.changes` — MOR delete file 미지원**: Silver는 MOR(Merge-On-Read) 테이블이므로 MERGE 시 delete file이 생성된다. `.changes` changelog scan은 delete file이 포함된 테이블에서 `UnsupportedOperationException`을 던진다.
- **`$snapshots.sequence_number` 없음**: 이 Iceberg/Spark 버전의 `$snapshots` 메타테이블 실제 컬럼은 `committed_at, snapshot_id, parent_id, operation, manifest_list, summary` 뿐이다. `sequence_number`는 존재하지 않는다 → `AnalysisException`.

**실제 구현: parent_id 기반 before/after 비교**

오늘 첫 번째로 커밋된 snapshot의 `parent_id`가 batch 이전 Silver 상태를 가리킨다.  
현재 snapshot과 before snapshot의 event_date별 행 수를 비교해 변경 파티션을 감지한다.

```python
# 오늘 첫 snapshot의 parent = batch 이전 상태
first_today = spark.sql(f"""
    SELECT snapshot_id, parent_id
    FROM {SILVER_TABLE}.snapshots
    WHERE committed_at >= TIMESTAMP '{today_utc} 00:00:00'
    ORDER BY committed_at ASC LIMIT 1
""").first()

# 현재 vs before snapshot 행 수 비교
curr_counts = spark.table(SILVER_TABLE).groupBy("event_date").count()
prev_counts = (
    spark.read.format("iceberg")
    .option("snapshot-id", str(before_id))
    .load(SILVER_TABLE)
    .groupBy("event_date").count()
    .withColumnRenamed("count", "prev_count")
)
# prev_count가 없거나 달라진 event_date = 오늘 변경된 파티션
```

이 방식은 INSERT(새 event_date 추가)와 행 수 변동이 있는 UPDATE를 모두 감지한다.  
지연 attribution(지난달 impression에 오늘 click이 매칭)으로 오래된 파티션이 변경된 경우도  
Silver snapshots만 스캔하면 되므로 전체 데이터 읽기보다 훨씬 가볍다.

---

### 3-3. Iceberg COW 선택 이유

Gold MERGE는 `(summary_date, campaign)` 복합키 기준으로 upsert한다.  
변경된 event_date의 모든 campaign 행을 매 실행마다 갱신하므로,  
UPDATE 비율이 거의 100%에 가깝다.

MOR는 UPDATE 비율이 낮을 때(delta 파일이 적을 때) 읽기 효율이 좋다.  
UPDATE 비율이 100%면 MOR의 이점이 없고, 오히려 읽기 시 base + delta 병합 오버헤드만 생긴다.  
COW는 파티션 파일을 전체 재작성하지만, 쓰기 후 파일이 clean하므로 읽기 성능이 항상 일정하다.

Gold는 Superset/Athena의 조회 대상이므로 읽기 성능이 중요하다 → COW가 적합하다.

---

### 3-4. KPI 계산식과 분모 0 처리

**집계 표현식을 변수로 먼저 선언하는 이유:**

```python
impressions_col = count("*")
clicks_col      = spark_sum("click")
```

같은 집계 표현식을 여러 KPI 식에서 재사용할 때, Spark Catalyst optimizer가 그룹당 한 번만 계산한다.  
표현식 객체를 변수로 뽑지 않고 `count("*") / count("*")`처럼 쓰면, 두 번 계산될 수 있다.

**분모 0 처리 원칙:**

| KPI | 분모 | NULL 보호 |
|-----|------|-----------|
| CTR | impressions | 불필요 — 그룹이 존재하면 impressions >= 1 |
| CPM | impressions | 불필요 — 동일 |
| frequency | unique_users | 불필요 — uid가 있으면 unique_users >= 1 |
| CVR | clicks | `when(clicks > 0, ...)` 필요 — click 0인 날 있음 |
| CPC | clicks | `when(clicks > 0, ...)` 필요 |
| CPA | conversions | `when(conversions > 0, ...)` 필요 |

**click_through vs view_through conversion:**

```python
count(when((col("click") == 1) & (col("conversion") == 1), lit(1)))  # click_through
count(when((col("click") == 0) & (col("conversion") == 1), lit(1)))  # view_through
```

`count(when(...))` 패턴에서, `when` 조건이 불일치하면 `null`을 반환하고, `count`는 null을 제외한다.  
`sum(when(..., 1).otherwise(0))`과 결과는 같지만, `count`가 의도를 더 명확하게 표현한다.

**`avg_conversion_delay_sec`에서 sentinel 제외:**

```python
avg(when(col("conversion_delay_sec") >= 0, col("conversion_delay_sec")))
```

`conversion_delay_sec = -1`은 전환이 없음을 나타내는 sentinel 값이다.  
-1을 포함해서 평균을 내면 실제 전환 지연보다 낮게 집계된다.  
`>= 0` 조건으로 sentinel을 null로 바꾸면 `avg`가 자동으로 null을 제외한다.

### 3-5. `createOrReplace()` — full_refresh 원자성

Silver §2-5와 동일한 이유로, Gold full_refresh도 `createOrReplace()`를 사용한다.  
기존 테이블을 내려받는 동안 Athena/Superset 쿼리가 빈 테이블을 보는 순간이 없다.

---

## 4. spark_utils.py — Glue + S3FileIO 자격증명 구조

### 4-1. Driver vs Executor 자격증명

SparkSession 설정에서 자격증명을 두 경로로 설정한다:

```python
# 경로 1: Driver용 — Glue API 호출 (메타스토어 조회/갱신)
.config(f"spark.sql.catalog.{catalog}.client.access-key-id", aws_key)
.config(f"spark.sql.catalog.{catalog}.client.secret-access-key", aws_secret)

# 경로 2: Executor용 — S3 데이터 파일 읽기/쓰기
.config("spark.executorEnv.AWS_ACCESS_KEY_ID", aws_key)
.config("spark.executorEnv.AWS_SECRET_ACCESS_KEY", aws_secret)
```

**왜 두 경로가 필요한가?**

GlueCatalog는 Driver JVM에서 실행되며 `spark.sql.catalog.*.client.*` 설정에서 자격증명을 읽는다.  
Glue API 호출(테이블 목록 조회, 파티션 등록 등)은 Driver에서만 발생한다.

Iceberg S3FileIO는 Executor JVM에서 실행되며 `spark.sql.catalog.*`를 읽지 않는다.  
AWS EnvironmentVariableCredentialsProvider 체인에 따라 환경변수를 읽으므로,  
`spark.executorEnv.*`로 전달해야 각 Executor 프로세스에서 인식한다.

---

### 4-2. Hadoop S3A vs Iceberg S3FileIO

```python
.config("spark.hadoop.fs.s3a.access.key", aws_key)
.config("spark.hadoop.fs.s3a.secret.key", aws_secret)
```

`spark.hadoop.fs.s3a.*`는 Hadoop FileSystem API(HDFS API 경유)에서 사용한다.  
Iceberg S3FileIO와는 별개의 자격증명 체계이다.

이 설정이 필요한 이유: Spark 내부 일부 연산(checkpoint 등)이 Hadoop S3A를 경유할 수 있다.  
삭제하면 해당 경로에서 `AccessDeniedException`이 발생할 수 있으므로 유지한다.

---

## 5. 운영 함정 — 처음에 놓치기 쉬운 것들

### E1. 날짜 인자 기본값 함정

`--run-date-start`와 `--run-date-end`의 기본값은 모두 "어제"다.  
처음 환경을 세팅하거나 과거 데이터를 백필할 때, 인자 없이 실행하면 어제 하루치만 처리한다.

```bash
# 위험: 기본값(어제)으로만 실행 — 오래된 데이터가 있어도 어제치만 처리됨
docker compose --profile batch run --rm raw-to-processed

# 안전: 명시적으로 날짜 지정
docker compose --profile batch run --rm raw-to-processed \
  /opt/spark/bin/spark-submit ... \
  --run-date-start 2026-06-01 --run-date-end 2026-06-23
```

`parse_args()`에 `start > end` 검증이 있지만, `start == end == 어제`는 유효하므로  
0건 처리 후 정상 종료된다. **처리 결과 로그를 반드시 확인할 것.**

---

### E2. `dedup()` tie-breaking 비결정론

```python
w = Window.partitionBy(key_col).orderBy("ingest_ts")
```

같은 `eid`를 가진 두 행의 `ingest_ts`가 동일하면 `row_number()`의 순서가  
Spark 셔플에 따라 비결정적(non-deterministic)이 된다.

**이것이 문제가 되는 경우:** Kafka의 동일 파티션에서 매우 빠르게 연속 발행된 메시지가  
같은 `ingest_ts` (1초 단위 반올림)를 가지는 경우.

**완전한 해결:** `kafka_offset`을 2차 정렬키로 추가한다.

```python
# 현재
w = Window.partitionBy(key_col).orderBy("ingest_ts")

# 개선안
w = Window.partitionBy(key_col).orderBy("ingest_ts", "kafka_offset")
```

현재는 운영 데이터에서 발생 확률이 극히 낮아 미수정 상태이다.  
동일 `eid` + 동일 `ingest_ts` 이슈가 실제로 관측되면 수정할 것.

---

### E3. Soft failure 임계값의 한계

`_assert_attribution_matched`는 매칭 건수가 **0건**일 때만 실패한다.  
Bronze에 click 1000건이 있는데 매칭이 10건(1%)이어도 통과한다.

이는 의도적 설계다. 정상 운영에서도 attribution window 밖의 click은 매칭되지 않아  
매칭률 100%를 기대할 수 없다. "적정 매칭률"은 데이터 특성에 따라 다르다.

더 정교한 모니터링이 필요하다면:
- `health-queries/07_attribution_window_coverage.sql`로 매칭률 추이를 추적
- 매칭률이 통상 범위를 벗어나면 Airflow alert를 발생시키는 Task 추가

---

### E4. full_refresh Attribution window 경계 보호

`main()` 내 full_refresh 분기에서 raw_date 범위를 window만큼 확장하는 코드가 있다:

```python
click_raw_end = date.fromisoformat(args.run_date_end) + timedelta(days=args.click_window_days)
conv_raw_end  = date.fromisoformat(args.run_date_end) + timedelta(days=args.conversion_window_days)
```

이 코드를 제거하면 `run_date_end` 이후에 수집된 click/conversion이 누락된다.  
"단순히 날짜 범위를 좁혀보자"는 의도로 이 부분을 수정하면 silent data loss가 발생한다.  
수정 전에 반드시 §2-3을 읽을 것.

---

### E5. `date.today()` UTC/KST 불일치

코드에서 `date.today()`는 컨테이너의 로컬 시스템 시간 기준 날짜를 반환한다.  
Docker 컨테이너의 기본 타임존은 UTC다.

**Silver 배치 (`raw_to_processed_iceberg.py`)**:  
`--run-date-start` 기본값이 `date.today() - 1`이다.  
UTC 기준 자정(00:00 UTC = 09:00 KST)에 실행하면 어제 KST 기준 "어제"와 UTC "어제"가 다를 수 있다.

**Gold 배치 (`get_changed_event_dates`)**:  
`start_ms`를 UTC 자정으로 명시적으로 설정한다 (`tzinfo=timezone.utc`).  
하지만 `date.today()`가 컨테이너 로컬 날짜이므로, KST 09:00 이전에 실행하면  
UTC 기준 "오늘" 시작보다 9시간 이른 전날 자정을 `start_ms`로 사용하게 된다.

**현재 대응:** Airflow 스케줄을 매일 한국 시간 기준 낮 시간대(예: 10:00 KST)에 실행하면 영향 없다.  
UTC 자정 전후 배치 실행이 필요해지면 `date.today()`를 `datetime.now(timezone.utc).date()`로 교체할 것.

이 이슈는 CLAUDE.md "S3 UTC vs KST 타임존 처리" TODO와 연결되어 있다.

---

## 6. kafka_to_raw.py — Bronze Streaming 적재

### 6-1. 왜 Airflow DAG이 아닌 별도 장시간 프로세스인가

Spark Structured Streaming은 종료 없이 계속 실행되는 long-running 프로세스다.  
Airflow는 Task를 실행하고 완료를 기다리는 구조이므로 Streaming 잡을 Task로 등록하면  
DAG이 영원히 끝나지 않는다.

이 때문에 `kafka_to_raw.py`는 docker-compose `restart: on-failure`로 별도 관리한다.  
Airflow `criteo_medallion_dag.py`는 Streaming 잡을 직접 트리거하지 않고,  
S3KeySensor로 Bronze 파티션 파일 존재 여부만 확인해 Silver 배치 시작 여부를 결정한다.

---

### 6-2. `startingOffsets` — `latest` vs `earliest`

| 값 | 동작 | 사용 시점 |
|----|------|-----------|
| `latest` (기본값) | 구독 시작 이후 새로 들어오는 메시지만 처리 | 정상 운영 |
| `earliest` | Kafka retention 범위 내 전체 메시지 재소비 | 장애 후 replay, checkpoint 초기화 후 재처리 |

`earliest`를 사용할 때는 checkpoint를 함께 초기화해야 한다.  
checkpoint가 남아 있으면 `startingOffsets=earliest`여도 checkpoint의 마지막 offset부터 재개한다.

---

### 6-3. `failOnDataLoss=false`

Kafka 토픽의 retention 기간(기본 7일)이 지나면 오래된 메시지가 삭제된다.  
Streaming 잡이 오랫동안 중단됐다가 재시작하면 checkpoint에 기록된 offset이  
이미 삭제된 메시지를 가리킬 수 있다.

`failOnDataLoss=true`(기본)이면 이 상황에서 잡이 즉시 실패한다.  
`false`로 설정하면 손실된 offset은 스킵하고 이후 메시지부터 이어서 처리한다.

이 파이프라인에서 Bronze는 append-only 원본 보관 역할이므로,  
retention 만료로 인한 소량 손실보다 잡 계속 실행이 더 중요하다 → `false` 선택.

---

### 6-4. `raw_date` 파티션 기준 — `ingest_ts` vs `event_time`

파티션 기준으로 이벤트 발생 시각(`event_time`) 대신 Spark 수집 시각(`ingest_ts`)을 쓴다.

이유: conversion 이벤트는 impression 발생 후 수 주 뒤에 브로커에 도착한다.  
`event_time` 기준이면 오늘 수집된 conversion이 30일 전 파티션에 쓰여  
해당 날짜 파티션 파일 크기가 비정상적으로 커진다.

`ingest_ts` 기준이면 매일 수집된 모든 이벤트가 오늘 파티션에 균등하게 쌓여  
파티션당 파일 크기가 예측 가능하다.  
Silver에서 `event_date` (event_time 기준)으로 재파티셔닝하므로 분석 정확도에는 영향 없다.

---

### 6-5. 토픽별 별도 readStream인 이유

impression에만 `cost` 필드가 있어 세 토픽의 스키마가 다르다.  
`.option("subscribe", "ad-impressions,ad-clicks,ad-conversions")`으로 하나의 readStream에 묶으면  
역직렬화 시 스키마 불일치로 null 컬럼 충돌이 발생한다.

3개의 readStream = 3개의 StreamingQuery가 `spark.streams`에 등록되어 스레드별 동시 실행된다.

---

### 6-6. checkpoint 경로는 토픽별로 달라야 한다

checkpoint는 Spark가 처리 완료한 Kafka offset을 기록하는 디렉토리다.  
재시작 시 이 기록을 읽어 중복 없이 이어서 처리한다.

같은 경로를 두 토픽이 공유하면 서로 다른 토픽의 offset 진행 상태가 섞여  
재처리 시 누락 또는 중복이 발생한다.

```
# 올바른 구조
checkpoints/kafka_to_raw/impressions/
checkpoints/kafka_to_raw/clicks/
checkpoints/kafka_to_raw/conversions/
```

---

### 6-7. `awaitAnyTermination()` — 왜 main thread를 블로킹하는가

`write_stream(...).start()`는 각 StreamingQuery를 백그라운드 스레드에서 실행하고 즉시 반환한다.  
`awaitAnyTermination()` 없이 `main()`이 끝나면 JVM 프로세스가 종료되어 모든 쿼리가 중단된다.

`awaitAnyTermination()`은 등록된 쿼리 중 하나라도 종료(에러 또는 명시적 stop)될 때까지  
main thread를 블로킹해 프로세스를 살아있게 유지한다.

정상 운영 중에는 영구 블로킹 → 컨테이너(프로세스)가 항상 실행 중이어야 한다.  
컨테이너가 크래시하면 docker-compose `restart: on-failure`가 자동 재시작한다.

---

### 6-8. `--topic-type` — 3-process 컨테이너 구조

docker-compose에서 토픽마다 별도 컨테이너로 실행한다:

```
kafka-to-raw-impression:  --topic-type impression  (spark.cores.max=3)
kafka-to-raw-click:       --topic-type click       (spark.cores.max=1)
kafka-to-raw-conversion:  --topic-type conversion  (spark.cores.max=1)
```

하나의 프로세스에서 3개 토픽을 처리하면 impression이 느릴 때 click/conversion도 지연된다.  
분리하면 토픽별 독립 장애 복구와 코어 배분이 가능하다.

`--topic-type` 없이 실행하면 하나의 프로세스에서 3개 토픽을 처리한다 — 개발/테스트용.

---

### 6-9. Bronze 추가 컬럼과 보존 정책

**추가 컬럼**

Kafka에서 읽은 원본 메시지 외에 네 개의 컬럼을 추가로 기록한다.

| 컬럼 | 출처 | 용도 |
|---|---|---|
| `kafka_partition` | Kafka 메타데이터 | dedup 디버깅, 파티션 편향 진단 |
| `kafka_offset` | Kafka 메타데이터 | dedup 정렬 2차 키 후보 (JOBS_GUIDE §5-E2 참고) |
| `kafka_timestamp` | Kafka 메타데이터 | 브로커 수신 시각 — Silver 지연 측정 기준 |
| `ingest_ts` | `spark.sql.functions.current_timestamp()` | Spark ingest 시각 — Bronze `raw_date` 파티션 기준 |

**보존 정책: 무기한**

Bronze는 Silver dedup·attribution 이전의 원본 데이터다.  
Silver 배치 버그 발견 시 Bronze에서 재처리할 수 있어야 하므로 삭제하지 않는다.  
Iceberg가 아닌 append-only Parquet이기 때문에 time travel이 없다 — 원본 파일 자체가 유일한 복구 수단이다.

---

### 6-10. 왜 `spark_utils.build_spark()`를 쓰지 않는가

batch 잡은 Iceberg 테이블 읽기/쓰기를 위해 Iceberg SQL extensions + Glue Catalog 설정이 필요하다.  
`kafka_to_raw.py`는 Parquet을 S3에 직접 쓰므로 Iceberg/Glue 설정이 전혀 필요 없다.

공통 모듈을 쓰면 불필요한 설정 20줄이 항상 포함되어 오해를 유발한다.  
용도가 달라 별도 `build_spark()`를 유지하는 것이 의도를 더 명확하게 한다.
