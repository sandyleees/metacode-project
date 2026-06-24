# DAGS_GUIDE.md

코드의 **무엇(what)** 은 DAG 파일 자체가 설명하고, 이 문서는 **왜(why)** 를 설명한다.

---

## 목차

1. [DAG 구성 개요](#1-dag-구성-개요)
2. [criteo_medallion_dag.py — 일배치 파이프라인](#2-criteo_medallion_dagpy--일배치-파이프라인)
3. [criteo_maintenance_dag.py — 일간 Iceberg 유지보수](#3-criteo_maintenance_dagpy--일간-iceberg-유지보수)
4. [criteo_maintenance_monthly_dag.py — 월간 manifest 재편성](#4-criteo_maintenance_monthly_dagpy--월간-manifest-재편성)

---

## 1. DAG 구성 개요

```
criteo_medallion_daily (매일 UTC 02:00)
  check_bronze_impressions
    │
    ▼
  silver_batch
    │
    ├─ verify_silver_snapshot   ($snapshots 메타테이블 — 오늘 Iceberg 커밋 존재 확인)
    └─ verify_silver_rows       (event_date={ds} 행 수 > 0 확인)
    │  (둘 다 통과해야 gold_batch 진입)
    ▼
  gold_batch
    │
    ├─ verify_gold_snapshot          ($snapshots 메타테이블 — Gold 오늘 커밋 존재 확인)
    └─ verify_silver_gold_consistency (Silver COUNT(*) vs Gold SUM(impressions) 10% 이내)
                                           │
                       ExternalTaskSensor (gold_batch SUCCESS)
                                           │
criteo_iceberg_maintenance (매일 UTC 02:00, medallion 완료 후 시작)
  ├─ compact_silver → expire_silver → orphan_silver ─┐
  └─ expire_gold    → orphan_gold ──────────────────→ maintenance_done
                                                            │
                                          ExternalTaskSensor (매월 1일만)
                                                            │
criteo_iceberg_maintenance_monthly (매월 1일 UTC 02:00)
  ├─ rewrite_silver_manifests
  └─ rewrite_gold_manifests
```

medallion DAG과 maintenance DAG을 분리한 이유: 배치 실패 시 maintenance가 실행되지 않도록.  
데이터 적재가 실패했는데 compaction이 실행되면 의미 없는 작업 + 비용 낭비.

### 파일 구조

| 파일 | 역할 |
|---|---|
| `dag_config.py` | 공통 상수·환경변수 (`S3_RAW_BUCKET`, `SPARK_CONF`, `ATHENA_RESULTS_S3`, `spark_env_vars()`) |
| `dag_utils.py` | 공통 팩토리 (`make_sql_task()`, `make_athena_gate_task()`) |
| `criteo_medallion_dag.py` | Bronze 확인 → Silver → [검증] → Gold → [검증] 일배치 |
| `criteo_maintenance_dag.py` | Compaction / Expire / Orphan 일간 유지보수 |
| `criteo_maintenance_monthly_dag.py` | manifest 재편성 월간 유지보수 |

---

## 2. criteo_medallion_dag.py — 일배치 파이프라인

### 2-1. Bronze 확인 Task

Silver 배치를 시작하기 전에 Bronze에 오늘 파티션 파일이 존재하는지 확인한다.  
Bronze가 없으면 Silver 배치를 시작해도 0건 처리 후 정상 종료된다.  
불필요한 Spark JVM 기동 비용을 막기 위한 방어 로직이다.

### 2-2. verify_* Task (AthenaOperator + PythonOperator)

Silver/Gold 배치 완료 후 즉시 Iceberg `$snapshots` 메타테이블을 조회해 커밋이 발생했는지 확인한다.  
Spark 잡이 exit 0으로 종료해도 Iceberg 커밋이 없으면 데이터가 들어가지 않은 것이다.

`dag_utils.make_athena_gate_task()` 패턴: 0행=정상, 1행+=AirflowException.

| task | 감지 대상 | 통과 조건 |
|---|---|---|
| `verify_silver_snapshot` | Silver Iceberg 커밋 누락 | `MAX(committed_at) >= current_date` |
| `verify_silver_rows` | Silver `{ds}` 파티션 빈 배치 | `COUNT(*) > 0` |
| `verify_gold_snapshot` | Gold Iceberg 커밋 누락 | `MAX(committed_at) >= current_date` |
| `verify_silver_gold_consistency` | Silver↔Gold 집계 불일치 또는 Gold 파티션 부재 | `diff_pct ≤ 10%` AND Gold 파티션 존재 |

`health-queries/alert/`의 4개 SQL과 목적이 비슷하지만 다른 점:
- DAG 검증 게이트: `{ds}` 기준 (당일 배치 결과 즉시 검증, Gold 진입 차단)
- `health-queries/alert/`: `current_date` 기준 (독립 실행, 별도 스케줄)

### 2-3. 검증 범위와 한계

현재 verify task는 **ds-1(처리 대상 날짜) 파티션 하나만** 확인한다.

Silver batch는 Bronze `raw_date=ds-1`을 읽어 **event_date 기준으로 재파티셔닝**한다.  
Attribution window(30일) 때문에 `raw_date=ds-1` 데이터의 event_date가 ds-1 외에  
과거 30일 내 여러 날짜에 분산될 수 있다.

```
Bronze raw_date=6/23
  → Silver event_date=5/25 업데이트  ← attribution (30일 전 impression에 오늘 click)
  → Silver event_date=6/10 업데이트  ← attribution
  → Silver event_date=6/23 신규 삽입 ← 당일 대부분의 데이터
```

| 검증 항목 | 커버 여부 |
|---|---|
| 당일(ds-1) 신규 데이터가 Silver에 들어갔는가 | ✅ `verify_silver_rows` |
| Silver/Gold에 Iceberg 커밋이 발생했는가 | ✅ `verify_silver_snapshot`, `verify_gold_snapshot` |
| 당일(ds-1) Silver↔Gold 집계 일치 | ✅ `verify_silver_gold_consistency` |
| attribution으로 변경된 과거 파티션 Silver↔Gold 정합성 | ❌ 미커버 |
| Gold가 Silver 변경분 전체(모든 event_date)를 처리했는가 | ❌ 미커버 |

과거 파티션 정합성은 `health-queries/alert/` SQL 확장 또는 Superset 차트로 추세를 모니터링하는 방향으로 보완해야 한다 (CLAUDE.md TODO 참고).

---

## 3. criteo_maintenance_dag.py — 일간 Iceberg 유지보수

### 3-1. compact_silver — 왜 필요하고 왜 35일인가

Silver는 MOR(Merge-On-Read)를 사용한다.  
MERGE UPDATE마다 delete file이 append되어 쌓이고, 읽기 시 base + delete 병합 오버헤드가 증가한다.

`compact_silver`는 이 delete file을 흡수해 clean한 data file로 재작성한다 (MOR 선택 이유: JOBS_GUIDE §2-4).

**왜 compaction 대상이 `event_date >= 오늘 - 35일`인가?**

conversion attribution window가 30일이다.  
오늘 수집된 conversion이 30일 전 impression을 업데이트할 수 있으므로,  
30일 전 파티션까지 오늘도 delete file이 새로 추가될 수 있다.  
여기에 수집 지연 여유 5일을 더해 35일이다.

35일보다 오래된 파티션은 attribution으로 인한 추가 업데이트가 없으므로  
compaction 대상에서 제외해 비용을 절약한다.

### 3-2. expire_silver / expire_gold — 왜 30일인가

Iceberg 스냅샷 보존 목적은 time travel(과거 조회)과 롤백이다.  
30일 이상 오래된 스냅샷으로 복구해야 하는 장애는 사실상 없다.  
이 이상 보존하면 S3 비용과 manifest 조회 오버헤드만 증가한다.

### 3-3. orphan_silver / orphan_gold — 왜 3일인가

고아 파일(orphan file)은 커밋 실패 등으로 Iceberg 메타데이터에 등록되지 않은 S3 파일이다.  
진행 중인 쓰기와 충돌하지 않도록 3일 이상 된 파일만 제거한다.  
3일 미만 파일을 제거하면 현재 진행 중인 커밋의 파일을 잘못 삭제할 위험이 있다.

### 3-4. Gold에 Compaction이 없는 이유

Gold는 COW(Copy-On-Write)를 사용한다.  
MERGE가 파티션 파일 전체를 재작성하므로 delete file 자체가 생기지 않는다.  
파티션당 파일 1개가 항상 유지되므로 compaction 대상이 없다 (COW 선택 이유: JOBS_GUIDE §3-3).

---

## 4. criteo_maintenance_monthly_dag.py — 월간 manifest 재편성

### 4-1. 왜 월간 DAG가 별도로 존재하는가

일간 maintenance DAG는 데이터 파일을 정리하지만 manifest 파일은 최적화하지 않는다.

Iceberg는 스냅샷마다 manifest list → manifest file 계층을 쌓는다.  
`expire_snapshots`가 오래된 스냅샷을 제거해도 잔존 manifest 파일은 파편화된 채로 남는다.

Silver MOR 테이블은 파티션이 많고 커밋이 잦아 manifest 파일이 빠르게 늘어난다.  
`rewrite_manifests`는 분산된 manifest를 소수의 큰 파일로 재편성해  
쿼리 플래닝 시 manifest 조회 오버헤드를 줄인다.

### 4-2. 왜 월 1회인가

manifest 재편성은 즉각적인 데이터 정합성 영향이 없는 순수 최적화 작업이다.  
일간 compaction/expire가 파일 수를 관리하므로 manifest 증가 속도가 빠르지 않다.  
월 1회로 충분하며, 빈도를 높여도 효과 대비 비용 증가만 발생한다.

### 4-3. 수동 트리거 시 logical_date 주의

`wait_for_daily_maintenance`(ExternalTaskSensor)는 **동일한 logical_date**를 가진  
`criteo_iceberg_maintenance.maintenance_done` task의 성공을 감지한다.

monthly DAG는 `schedule="0 2 1 * *"` (매월 1일 02:00)이고,  
daily maintenance DAG도 같은 `0 2 * * *` 기준으로 `logical_date`가 당일 02:00으로 발행된다.  
정상 스케줄 기동 시 두 DAG의 `logical_date`가 `YYYY-MM-01T02:00:00+00:00`으로 동일해 매칭된다.

**수동 트리거 시 반드시 daily maintenance와 동일한 logical_date를 사용해야 한다:**

```bash
# daily maintenance가 2026-06-24T02:00:00+00:00으로 완료된 경우
# monthly를 임의 시각으로 트리거하면 ExternalTaskSensor가 영원히 up_for_reschedule 상태 유지

# 잘못된 예 — sensor가 08:10:00 시각의 daily maintenance를 찾다가 timeout
TOKEN=$(curl -s -X POST 'http://localhost:8081/auth/token' ...)
curl -X POST '.../dagRuns' -d '{"logical_date": "2026-06-24T08:10:00+00:00"}'

# 올바른 예 — daily maintenance의 logical_date와 맞춤
curl -X POST '.../dagRuns' -d '{"logical_date": "2026-06-24T02:00:00+00:00"}'
```

daily maintenance의 정확한 logical_date 확인 방법:
```bash
# Airflow API로 최근 run_id 조회
curl -s 'http://localhost:8081/api/v2/dags/criteo_iceberg_maintenance/dagRuns?limit=3' \
  -H "Authorization: Bearer $TOKEN" | python3 -c \
  'import sys,json; [print(r["dag_run_id"], r["state"]) for r in json.load(sys.stdin)["dag_runs"]]'
```
