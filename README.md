# Criteo Advertising Data Pipeline

Kafka → Spark Structured Streaming → S3 Iceberg (Medallion) → Athena → Superset로 이어지는
광고 성과 분석용 배치/스트리밍 ETL 파이프라인입니다. Criteo Display Advertising Dataset을
소스로 사용해 impression/click/conversion 이벤트를 수집·정제·집계합니다.

- **도메인**: 광고 성과 분석 (impression / click / conversion)
- **규모 가정**: 일 100만 이벤트, 6개월 내 10x 성장 목표
- **BI**: Apache Superset (Athena 연결)

## 저장소 구조

이 저장소의 핵심은 `project/`이며, 이하 내용은 모두 `project/` 기준입니다.

```
project/          파이프라인 실제 코드 (본 문서가 다루는 대상)
design-notes/     개발 과정에서 고민한 구조 스케치/사진
presentations/    2026.06.27 발표 자료 (최종 슬라이드, 스크립트)
week1/, week2/    과제용 자료 — 이 파이프라인과 무관
```

---

## 아키텍처

```
Criteo Dataset (HuggingFace streaming)
    │
    ▼
[producer.py] ──▶ [Kafka] 3 Topics × 3 Partitions
                    (ad-impressions / ad-clicks / ad-conversions)
    │  Spark Structured Streaming (토픽당 컨테이너 1개)
    ▼
[kafka_to_raw.py] ──▶ [S3] raw/  ← Bronze (Parquet, append-only, raw_date/raw_hour 파티션)
    │  일배치 (Airflow SparkSubmitOperator)
    ▼
[raw_to_processed_iceberg.py] ──▶ [S3] warehouse/silver/  ← Silver (Iceberg MOR, event_date 파티션)
    │  일배치
    ▼
[processed_to_campaign_summary.py] ──▶ [S3] warehouse/gold/  ← Gold (Iceberg COW, summary_date 파티션)
    │
    ▼
[Glue Catalog] → [Athena] → [Apache Superset :8088]
```

Bronze는 원본 이벤트를 그대로 append하고, Silver에서 dedup·attribution(클릭/전환 조인)을 수행하며,
Gold에서 캠페인 단위 일별 집계를 만듭니다. Airflow가 Silver→Gold 배치와 Iceberg 유지보수
(compaction, snapshot expiration, manifest 재편성)를 스케줄링합니다.

---

## 왜 이 도메인이 어려운가 — Attribution Window

impression/click/conversion은 같은 사용자의 같은 행위 사슬이지만 도착 시점이 전혀 다릅니다.

| 이벤트 | 의미 | 도착 시점 |
|---|---|---|
| impression | 광고 노출, cost 발생 | 즉시 |
| click | 광고 클릭 | impression 후 수초~수일 |
| conversion | 구매/가입 완료 | impression 후 수일~수십일 |

오늘 저장한 impression에 대한 click은 7일 뒤, conversion은 30일 뒤에야 도착할 수 있습니다.
그래서 "이 impression 덕분에 발생한 click/conversion"을 인정하는 시간 한도(Attribution Window)를
click 7일 / conversion 30일로 설정했습니다(업계 표준, Google/Meta와 동일). 이 window는 Silver의
파티션 스캔 범위, Iceberg compaction 주기(35일), Gold KPI 정확도를 결정하는 핵심 파라미터입니다.

---

## 기술 스택

| 영역 | 기술 | 선택 이유 |
|---|---|---|
| 메시징 | Kafka | 이벤트 타입(impression/click/conversion)별 토픽 분리로 처리량·장애 격리 |
| 스트리밍/배치 | Spark Structured Streaming, Spark Batch | Bronze 적재는 스트리밍, Silver/Gold는 일배치로 역할 분리 |
| 스토리지 포맷 | Apache Iceberg | 스키마 진화, 파티션 진화, snapshot 기반 time travel/incremental read |
| 오케스트레이션 | Apache Airflow 3.x | DAG 기반 일배치 스케줄링, `ExternalTaskSensor`로 DAG 간 의존성 관리 |
| 카탈로그 | AWS Glue | Iceberg 테이블 메타데이터 중앙 관리, Athena와 자연스럽게 연동 |
| 쿼리 엔진 | AWS Athena | 서버리스, Glue Catalog 기반 Iceberg 테이블 직접 쿼리 |
| BI | Apache Superset | 무료, Athena 커넥터 지원 |
| 인프라 | Docker Compose | 로컬에서 전체 스택(Kafka/Spark/Airflow/Superset) 재현 |

### 왜 Apache Iceberg인가

일반 Parquet 대비 이 프로젝트가 실제로 사용하는 기능 기준 비교입니다.

| 항목 | 일반 Parquet | Iceberg (이 프로젝트) |
|---|---|---|
| ACID 쓰기 (MERGE INTO) | 미지원 — 파티션 전체 DROP/INSERT 필요 | 네이티브 지원, 멱등 upsert 가능 |
| 스냅샷 & Time Travel | 불가 — 덮어쓰면 이전 상태 복구 불가 | 스냅샷별 이전 상태 조회 (Gold snapshot diff의 기반) |
| 파티션 변경 | 컬럼 변경 시 파일 전체 재작성 | 메타데이터만 변경, 데이터 파일 유지 |
| 파일 수준 통계 | 파티션 단위만 | 컬럼 min/max 통계로 파티션 내 파일 pruning |
| COW / MOR 전략 | 없음 — 항상 파일 전체 재작성 | 쓰기 패턴에 따라 선택 (Silver=MOR, Gold=COW) |

---

## 주요 설계 포인트

- **토픽 3분리**: impression/click/conversion을 하나의 토픽으로 묶지 않고 분리해, 이벤트 타입별로
  파티션 수·Spark 코어 배분을 독립적으로 튜닝 (`project/ingestion/INGESTION_GUIDE.md`).
- **Medallion 아키텍처**: Bronze(원본 append) → Silver(dedup + attribution) → Gold(캠페인 집계)로
  단계를 분리해 재처리 범위를 최소화 (`project/jobs/JOBS_GUIDE.md`).
- **Silver 2-Stage MERGE**: 어제 Bronze의 impression은 Stage 1에서 event_date(발생 시각) 기준으로
  Silver에 INSERT하고, click/conversion은 Stage 2에서 7일/30일 파티션 범위 내 event_id로 매칭되는
  impression을 찾아 MERGE UPDATE합니다. Silver가 attribution 중간 저장소 역할을 해 Bronze 30일치를
  매번 재스캔하지 않아도 됩니다.
- **Iceberg 테이블 전략 차등화**: Silver는 MOR(Merge-On-Read, UPDATE 대상이 전체의 ~2%라 delta
  append가 파일 전체 재작성보다 저렴), Gold는 COW(Copy-On-Write, Superset/Athena가 가장 많이
  읽는 테이블이라 읽기 성능 우선)로 워크로드에 맞게 분리.
- **Gold snapshot diff**: 매일 Silver 전체를 재스캔하지 않고, 오늘 배치 이전/이후 Silver 스냅샷을
  event_date별 행수로 비교해 변경된 파티션만 골라 재집계합니다.
- **DAG 분리**: 일배치 파이프라인(`criteo_medallion_dag`)과 Iceberg 유지보수
  (`criteo_maintenance_dag`, 월간 `criteo_maintenance_monthly_dag`)를 별도 DAG로 운영
  (`project/orchestration/DAGS_GUIDE.md`).
- **헬스 체크 쿼리셋**: alert/ops/infra/business/incident 5개 카테고리, 15개 Athena SQL로
  파이프라인 정상성·데이터 정합성을 점검 (`project/health-queries/HEALTH_QUERIES_GUIDE.md`).

---

## 트러블슈팅 / 알려진 한계

- **파이프라인 전체 UTC 고정**: Kafka 타임스탬프부터 Spark 컨테이너, Airflow 스케줄, S3 파티션
  경로까지 전부 UTC 기준이라 KST 자정 경계에서 날짜가 어긋날 수 있습니다. 현재는 Airflow
  스케줄을 KST 낮 시간대에 배치해 우회 중이며, 근본적으로는 국제화 시 전체 날짜 기준 재검토가
  필요합니다.
- **Airflow 3.x `{{ ds }}` 시맨틱 변경 대응**: Airflow 2.x에서 `{{ ds }}`는 처리 대상일(전날)을
  의미했지만 3.x에서는 트리거 당일을 의미하도록 바뀌었습니다. DAG 템플릿은
  `{{ macros.ds_add(ds, -1) }}`로, Python 콜백은 명시적 날짜 계산으로 이 차이를 흡수합니다.
- **Gold snapshot diff의 attribution UPDATE 누락 (설계 버그)**: 변경 파티션 감지가 row count
  비교 방식이라, Silver의 click/conversion attribution UPDATE만 있고 row 수는 그대로인 과거
  파티션을 놓칠 수 있습니다. 단기적으로는 attribution window(30일)를 항상 재집계하는 방식으로
  완화하고, 장기적으로는 Silver를 COW + event_id sort로 재설계해 `.changes()`를 활용할 계획입니다.
- **인프라 HA 미고려**: Kafka가 단일 브로커(replication factor=1)라 브로커 장애 시 메시지가
  유실될 수 있고, Spark Worker도 물리적으로 단일 머신에 떠 있어 실제 분산 환경이 아닙니다
  (로컬 재현 목적의 흉내 수준). 장애 복구 절차도 별도로 설계하지 않았습니다.
- **테이블 1:1 하드코딩 구조**: 현재 DAG/유지보수 잡은 Silver·Gold 테이블이 각각 1개라는 전제로
  짜여 있어, 테이블이 늘어나면 DAG 전체를 수정해야 합니다. 레이어별로 유지보수 DAG를 분리하는
  편이 더 적합한 설계였을 것으로 판단하고 있으며, 스케일아웃(파티션 전략·코어 배분 재설계)
  전략도 아직 수립하지 않았습니다.

더 자세한 함정/설계 결정은 `project/CLAUDE.md`와 각 `*_GUIDE.md` 문서에 정리되어 있습니다.

---

## Quick Start

모든 명령어는 `project/` 디렉터리에서 실행합니다.

```bash
cd project

# 전체 스택 기동
docker compose up -d

# Airflow (DAG 스케줄러/웹서버)
docker compose up -d airflow-webserver airflow-scheduler
# http://localhost:8080  admin / admin

# Superset 초기화 (최초 1회) 후 기동
docker compose --profile superset-init up --abort-on-container-exit superset-init
docker compose up -d superset
# http://localhost:8088  admin / admin

# Silver / Gold 수동 배치 실행
docker compose --profile batch run --rm raw-to-processed
docker compose --profile batch run --rm processed-to-summary
```

Kafka UI: `http://localhost:8090` · Spark Master UI: `http://localhost:8080`

실행 전 `project/.env`에 AWS 자격증명(`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`)과
S3 경로(`S3_RAW_BASE`, `GLUE_WAREHOUSE`, `CHECKPOINT_BASE`)를 채워주세요 (`.env.example` 템플릿은 준비 중).

---

## 디렉토리 구조 (`project/` 내부)

```
ingestion/       Kafka producer, Bronze 적재 (kafka_to_raw.py)
jobs/            Silver/Gold Spark 배치 잡, DDL, 공통 유틸(spark_utils.py)
orchestration/   Airflow DAG (medallion / maintenance / maintenance_monthly)
health-queries/  alert / ops / infra / business / incident 5개 카테고리 Athena SQL
analytics/       Iceberg/Glue ad-hoc SQL 실행기
docker-compose.yaml, Dockerfile*   전체 스택 컨테이너 정의
```

---

## 향후 개선 계획

- Gold snapshot diff 설계 재검토 (Silver COW + sort 기반 incremental read)
- Compaction 시 sort/z-order 적용 (Silver: event_id, Gold: campaign)
- Superset Dataset 캐싱 설정으로 Athena 스캔 비용 절감
- Airflow DAG Run Conf 연동 (UI에서 백필 날짜 입력)
- health-queries의 ops/business 지표를 Superset 차트로 상시 등록

전체 TODO는 `project/CLAUDE.md` 참고.
