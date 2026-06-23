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
criteo_medallion_dag              매일 실행 (schedule: @daily)
  Bronze 확인 → Silver 배치 → Gold 배치
        │
        │ ExternalTaskSensor (gold_batch SUCCESS 감지)
        ▼
criteo_maintenance_dag            medallion 완료 후 실행
  Silver: compact → expire → orphan
  Gold:   expire → orphan

criteo_maintenance_monthly_dag    매월 1일 실행
  Silver: rewrite_manifests
  Gold:   rewrite_manifests
```

medallion DAG과 maintenance DAG을 분리한 이유: 배치 실패 시 maintenance가 실행되지 않도록.  
데이터 적재가 실패했는데 compaction이 실행되면 의미 없는 작업 + 비용 낭비.

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
