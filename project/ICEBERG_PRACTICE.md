# Iceberg 스냅샷 / Time Travel / Rollback 실습 기록

> 2026-06-16, 빈 S3/Glue 상태에서 전체 파이프라인(Bronze→Silver→Gold)을 가동해
> 스냅샷을 누적시킨 뒤, 의도적 장애 → time travel → rollback까지 복구한 실습 로그.
> 모든 SQL은 `run_sql.py`(ad-hoc Iceberg/Glue 실행기, 본 실습에서 신규 작성)로 실행.
> 구체적인 실행 명령어, S3 버킷 상태, 로그 상태 캡쳐 사진은 notion에 별도 기록

## 0. 사전 점검에서 발견·수정한 버그

**증상**: Silver `processed_events.event_date`가 전부 `1970-01-01`로 적재됨.

**원인**: `producer` 서비스는 다른 Spark 서비스와 달리 `volumes: ./:/app` 마운트가 없고
`Dockerfile`에서 `COPY producer.py .`로 **빌드 시점에 코드를 이미지에 고정**한다.
실행 중이던 컨테이너는 Criteo 상대시간을 실제 시각으로 재매핑하는 로직(`real_ts = real_start + (ts - criteo_base)/speed_multiplier`)이
추가되기 *이전*에 빌드된 stale 이미지였고, 그 결과 Kafka로 Criteo 원본 상대시간(예: `50307`초 = 1970-01-01 13:58:27)이 그대로 발행됐다.

**조치**: `docker compose build --no-cache producer` 로 최신 코드 반영 후 재검증.
(처음 발견했던 잔여 데이터의 1970-01-01 파티션도 동일한 원인이었음 — 기존부터 있던 버그.)

```
재빌드 전: timestamp=50307   event_time=1970-01-01T13:58:27
재빌드 후: timestamp=1781591561 event_time=2026-06-16T06:32:41   ✅
```

## 1. 파이프라인 가동 & 스냅샷 누적

- docker-compose에 `processed-to-summary`(Gold batch, `profile: batch`, `cores.max=2`) 서비스 추가 (CLAUDE.md 1순위 TODO)
- Kafka/Producer/kafka-to-raw 스트리밍 가동 → S3 raw zone에 impression/click/conversion 누적
- **알게 된 운영 함정**: `raw_to_processed_iceberg.py`/`processed_to_campaign_summary.py`의 날짜 인자 기본값은 모두 `어제`.
  raw_date가 전부 "오늘"인 단일-일 테스트 환경에서는 기본값으로 실행하면 **0건 처리**된다 (실제로 1회 재현됨).
  → 배치 실행 시 `--run-date-start/--run-date-end`를 명시하거나, 운영에서는 스케줄러가 당일 날짜를 정확히 주입하는지 확인 필요.
- raw-to-processed / processed-to-summary를 각 3회 반복 실행 → 스냅샷 누적

결과:

| 테이블 | 스냅샷 수 | 최종 행 수 |
|---|---|---|
| `silver.processed_events` | 9 (Stage1 append + Stage2 click MERGE + Stage2 conversion MERGE × 3회) | 279,867 |
| `gold.campaign_summary` | 3 | 658 |

## 2. 스냅샷 history / summary 비교

```sql
SELECT snapshot_id, parent_id, is_current_ancestor, made_current_at
FROM glue_catalog.gold.campaign_summary.history
ORDER BY made_current_at
```

| snapshot_id | parent_id | is_current_ancestor | made_current_at |
|---|---|---|---|
| 1577217070436397490 | NULL | true | 06:39:06 |
| 6658559971135042942 | 1577217070436397490 | true | 06:45:14 |
| 6642388431091670155 | 6658559971135042942 | true | 06:52:37 |

```sql
SELECT snapshot_id, committed_at, operation,
       summary['added-records'], summary['deleted-records'],
       summary['total-records'], summary['added-data-files'],
       summary['deleted-data-files'], summary['total-data-files']
FROM glue_catalog.gold.campaign_summary.snapshots
ORDER BY committed_at
```

| snapshot_id | operation | added | deleted | total | files |
|---|---|---|---|---|---|
| 1577217070436397490 | overwrite | 600 | – | 600 | 1 |
| 6658559971135042942 | overwrite | 649 | 600 | 649 | 1 |
| 6642388431091670155 | overwrite | 658 | 649 | 658 | 1 |

- Gold는 매번 30일 lookback 전체를 재집계해 `MERGE INTO ... WHEN MATCHED THEN UPDATE SET *`로 쓰기 때문에, `write.merge.mode=copy-on-write` 설정상 직전 파일 전체를 삭제하고 새 파일 1개로 교체한다 (`deleted == 직전 total`, `total_data_files` 항상 1).
- `$files`로 확인한 현재 활성 파일은 마지막 스냅샷의 파일 1개뿐 — 이전 스냅샷이 쓴 파일들은 카탈로그상 더는 참조되지 않지만(논리적으로는 보이지 않음) **S3에는 물리적으로 남아있다** (→ 4절 장애 복구의 핵심 전제).

## 3. 의도적 장애 시나리오 1 — 잘못된 `--full-refresh`

운영 사고 재현: `processed_to_campaign_summary.py --full-refresh`를 `--run-date-end` 없이 실행
(기본값 = 어제 → 모든 실데이터가 "오늘"이므로 lookback 필터에 전부 빠짐).

```bash
spark-submit processed_to_campaign_summary.py --full-refresh
# run_full_refresh(): DROP TABLE IF EXISTS ... ; ensure_table(); staged.writeTo(...).append()
```

결과: `totalRecords=0` — 테이블이 통째로 비워짐.

**복구 시도 1 — 실패**:
```sql
CALL glue_catalog.system.rollback_to_snapshot('gold.campaign_summary', 6642388431091670155)
-- org.apache.iceberg.exceptions.ValidationException:
-- Cannot roll back to unknown snapshot id: 6642388431091670155
```

**원인**: `--full-refresh`는 `DROP TABLE` 후 재생성한다. Iceberg에서 `DROP TABLE`(PURGE 없이)은
**카탈로그 항목만 교체**하고 메타데이터/데이터 파일은 S3에 남기지만, 새 테이블은 완전히 새로운 스냅샷
lineage(`parent_id=NULL`부터 다시 시작)를 갖는다 → 이전 스냅샷은 `rollback_to_snapshot`/`VERSION AS OF`
어느 것으로도 접근 불가능한 **진짜 고아(orphan)** 상태가 된다. 일반적인 "잘못된 MERGE/DELETE" 사고와
근본적으로 다른, 더 심각한 사고 유형.

운영자가 --full-refresh를 실행했을 때 카탈로그와 메타데이터 사이에서는 다음과 같은 단절이 일어난 것입니다.

원래 상태: Glue 카탈로그는 658건의 역사가 고스란히 적힌 A라는 진짜 metadata.json 주소를 쥐고 있었습니다.

DROP TABLE 실행: Glue 카탈로그에서 gold.campaign_summary라는 이름표를 떼어내면서, A라는 진짜 metadata.json과의 연결 고리를 싹둑 끊어버렸습니다. (하지만 S3에 있는 A 파일 자체는 지우지 않고 그대로 둡니다.)

새 테이블 생성: Iceberg는 S3의 완전히 새로운 경로에 아무것도 기록되지 않은 텅 빈 B라는 가짜(새내기) metadata.json을 새로 굽고, Glue 카탈로그에게 "앞으로 gold.campaign_summary라는 이름표는 이 B 주소를 가리키도록 해!" 하고 연결해 버린 것입니다.

그래서 우리가 옛날 스냅샷 ID로 롤백하려고 했을 때, 카탈로그가 현재 연결된 텅 빈 B 파일을 열어보고는 "어? 내가 가진 족보(B)에는 그런 스냅샷 ID가 없는데?" 하면서 Unknown snapshot id 에러를 뱉었던 것입니다.

**복구 시도 2 — 성공 (`register_table`)**:

```bash
# S3에 직전 정상 metadata.json이 살아있는지 확인 (스냅샷 리스트, 계보, 현재 스냅샷 정보 다 여기에 있음)
aws s3 ls s3://metacode-criteo-project/warehouse/gold.db/campaign_summary/metadata/
# → 00003-9fe819f4-...-metadata.json (장애 직전 마지막 정상 상태, snapshot 6642388431091670155)
```

```sql
DROP TABLE glue_catalog.gold.campaign_summary;  -- 깨진 카탈로그 항목 제거 (0건이라 손실 없음)
-- "깨진 카탈로그 항목"이란, "우리가 원하는 진짜 과거 데이터 이력(Lineage)이 전부 잘려 나간 채, 껍데기(0건짜리 새 테이블)만 등록되어 있는 상태"

-- 과거 상태 (진짜 데이터): 스냅샷 계보가 1번 → 2번 → 3번(6642..., 658건)으로 예쁘게 이어져 있었습니다.

-- DROP 수행 후: AWS Glue Catalog(또는 우리 카탈로그)에서 gold.campaign_summary라는 이름표를 기존 스냅샷 계보로부터 완전히 떼어내 버렸습니다.

-- 재생성 후 (현재 상태): 데이터가 0건인 완전히 새로운 족보(스냅샷 ID 새로 생성, 부모 ID 없음)를 가진 껍데기 테이블을 만들고, 거기에 다시 gold.campaign_summary라는 이름표를 붙였습니다.

CALL glue_catalog.system.register_table(
  'gold.campaign_summary',
  's3a://metacode-criteo-project/warehouse/gold.db/campaign_summary/metadata/00003-9fe819f4-210c-4b5b-926c-825559de83d3.metadata.json'
);
-- current_snapshot_id=6642388431091670155 | total_records_count=658 | total_data_files_count=1
```

복구 후 `$history` 재조회 → 원래 3-스냅샷 체인이 그대로 복원됨 (사고 스냅샷은 카탈로그 교체로 흔적도 사라짐).

> **교훈**: `--full-refresh`(DROP+재생성) 사고는 `rollback_to_snapshot`으로 못 푼다.
> S3에서 직전 `metadata.json`을 찾아 `register_table`로 카탈로그를 다시 붙이는 것이 유일한 복구 경로다.
> (단, S3 버킷에 `expire_snapshots`/lifecycle로 옛 메타데이터가 이미 정리됐다면 이 방법도 불가능 —
> orchestration/maintenance.sh의 보존 기간 설계가 왜 중요한지 보여주는 사례)

## 4. 의도적 장애 시나리오 2 — 일반 DML 사고 + 정석 rollback

DROP을 동반하지 않는, lineage가 끊기지 않는 "흔한" 사고를 복원된 테이블에 재현:

```sql
DELETE FROM glue_catalog.gold.campaign_summary WHERE 1=1;
-- Committed snapshot 3584228138035791085 (operation=delete, removedRecords=658, totalRecords=0)
```

**Time travel로 사고 전 데이터 확인**:
```sql
SELECT count(*) FROM glue_catalog.gold.campaign_summary;                            -- 0  (현재, 장애 상태)
SELECT count(*) FROM glue_catalog.gold.campaign_summary VERSION AS OF 6642388431091670155;  -- 658 (과거 정상 스냅샷)
```

**Rollback 실행**:
```sql
CALL glue_catalog.system.rollback_to_snapshot('gold.campaign_summary', 6642388431091670155);
-- previous_snapshot_id=3584228138035791085 | current_snapshot_id=6642388431091670155
```

**복구 검증**:
```sql
SELECT count(*) FROM glue_catalog.gold.campaign_summary;  -- 658 ✅ 복구 완료
```

## 5. Rollback 전후 `$history` vs `$snapshots` 비교

`$snapshots` (불변 커밋 로그 — rollback 후에도 행 수 불변, 4건):

| snapshot_id | operation | added | deleted | total |
|---|---|---|---|---|
| 1577217070436397490 | overwrite | 600 | – | 600 |
| 6658559971135042942 | overwrite | 649 | 600 | 649 |
| 6642388431091670155 | overwrite | 658 | 649 | 658 |
| 3584228138035791085 | **delete** | – | 658 | **0** |

`$history` (HEAD 포인터 이동 로그 — rollback이 새 행을 추가, 5건):

| snapshot_id | is_current_ancestor | made_current_at |
|---|---|---|
| 1577217070436397490 | true | 06:39:06 |
| 6658559971135042942 | true | 06:45:14 |
| 6642388431091670155 | true | 06:52:37 |
| 3584228138035791085 | **false** | 07:40:28 |
| 6642388431091670155 | true | **07:58:14 ← rollback** |

- `rollback_to_snapshot`은 새로운 데이터 변경을 만들지 않고 **HEAD 포인터만 이동**시키는 메타데이터 연산이다.
  그래서 `$snapshots`에는 행이 추가되지 않고(사고 스냅샷도 그대로 보존돼 감사 가능), `$history`에는
  같은 `snapshot_id`가 다시 등장하는 새 행이 생긴다.
- 사고 스냅샷의 `is_current_ancestor`가 `true → false`로 바뀌어 "한때 HEAD였지만 현재 체인의 조상은 아님"을 표시한다.
- **"무슨 일이 있었나"는 `$snapshots`, "지금 무엇이 유효한가/언제 바뀌었나"는 `$history`**로 구분해서 봐야 한다.

## 6. 사용한 도구

- `run_sql.py` (신규): `glue_catalog` + Iceberg 확장을 로드한 SparkSession으로 임의 SQL/`CALL` 프로시저를
  spark-submit으로 즉석 실행하는 ad-hoc 러너. `health-queries/*.sql` 실행기로도 재사용 가능.
  ```bash
  docker compose --profile batch run --rm raw-to-processed \
    /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
    --conf spark.cores.max=1 /app/run_sql.py --sql "<SQL 또는 CALL 문>"
  ```

## 7. 최종 상태 / 인프라

- 실습 종료 시점 데이터: `silver.processed_events` 279,867건(9 스냅샷), `gold.campaign_summary` 658건(정상 복구 완료, 4 스냅샷 중 HEAD는 3번째).
- 비용 절감을 위해 모든 docker 컨테이너(`docker compose down`) 정리 완료 — S3/Glue 데이터는 보존.
- 재가동: `docker compose up -d zookeeper kafka producer spark-master spark-worker-1 spark-worker-2 spark-worker-3` 후
  `kafka-to-raw-*` 및 batch profile 서비스 필요 시 추가 기동.
