# CLAUDE.md — Criteo Advertising Data Pipeline

## 프로젝트 개요

**도메인**: 광고 성과 분석 (Criteo Display Advertising Dataset)  
**목표**: Kafka → Spark Streaming → S3 Iceberg (Medallion) → Athena → Superset  
**규모 가정**: 일 100만 이벤트 (impression/click/conversion), 6개월 내 10x 성장 목표  
**BI 도구**: Apache Superset (docker-compose, 무료, Athena 연결)

---

## 현재 구현 상태

```
✅ 완료
  ingestion/producer.py                              Kafka 3토픽 발행
  ingestion/INGESTION_GUIDE.md                       3토픽 분리 설계·시간 재매핑·알려진 한계
  ingestion/kafka_to_raw.py                          Bronze: Streaming → S3 raw Parquet
  jobs/raw_to_processed_iceberg.py                   Silver: raw → processed_events Iceberg
  jobs/processed_to_campaign_summary.py              Gold: processed_events → campaign_summary Iceberg
  docker-compose.yaml                                Kafka + Spark + Airflow + Superset 전체 환경
  Dockerfile / Dockerfile.spark / Dockerfile.airflow / Dockerfile.superset
  analytics/run_sql.py                               Iceberg/Glue ad-hoc SQL 실행기
  orchestration/dags/criteo_medallion_dag.py         Bronze확인→Silver→Gold 일배치 DAG
  orchestration/dags/criteo_maintenance_dag.py       Silver/Gold 일간 유지보수 DAG
  orchestration/dags/criteo_maintenance_monthly_dag.py  Silver/Gold 월간 manifest 재편성 DAG
  health-queries/                                    15개 Athena SQL (alert/ops/infra/business/incident/)

🔲 미완료 (평가 기준 필수)
  dashboard/                               Superset 스크린샷
  README.md                                평가 답변 섹션
  .env.example                             환경변수 템플릿
```

설계 상세: `jobs/JOBS_GUIDE.md` | DAG 설계: `orchestration/DAGS_GUIDE.md` | 헬스쿼리: `health-queries/HEALTH_QUERIES_GUIDE.md`

---

## 아키텍처 스냅샷

```
Criteo Dataset (HuggingFace streaming)
    │
    ▼
[producer.py]
    │
    ▼
[Kafka]  3 Topics × 3 Partitions (ad-impressions / ad-clicks / ad-conversions)
    │
    ▼  Spark Structured Streaming (토픽당 컨테이너 1개, restart: on-failure)
[kafka_to_raw.py]
    │
    ▼
[S3] raw/  ← Bronze (Parquet, append-only, raw_date/raw_hour 파티션)
    │
    ▼  일배치 (Airflow SparkSubmitOperator)
[raw_to_processed_iceberg.py]
    │
    ▼
[S3] warehouse/silver/  ← Silver (Iceberg MOR, event_date 파티션)
    │
    ▼  일배치
[processed_to_campaign_summary.py]
    │
    ▼
[S3] warehouse/gold/  ← Gold (Iceberg COW, summary_date 파티션)
    │
    ▼
[Glue Catalog] → [Athena] → [Apache Superset :8088]
```

---

## 알려진 함정

- **DDL COMMENT 세미콜론 금지**: `parse_sql()`이 `;` 기준으로 분리하므로
  `COMMENT '...; ...'` 형태는 CREATE TABLE이 중간에 잘림 → `,` 사용
- **health-queries UTC/KST 경계**: `event_date < current_date` 조건은 Athena UTC 기준.
  KST 09:00 이전에는 당일 UTC 데이터를 반환하지 않음
  → Airflow 스케줄을 KST 10:00 이후로 설정하면 영향 없음 (JOBS_GUIDE §5-E5 참고)
- **producer 재빌드 필수**: `volumes` 마운트 없음. `producer.py` 수정 후
  `docker compose build --no-cache producer` 누락 시 stale 이미지로 잘못된 데이터 발행
  (실제 `event_date=1970-01-01` 버그 발생 사례)
- **배치 날짜 인자 기본값 함정**: `--run-date-start` 미지정 시 UTC 어제 기준 처리.
  KST 09:00 이전 실행 시 의도와 다른 날짜 처리됨 (JOBS_GUIDE §5-E1, E5 참고)
- **Airflow 3.x `{{ ds }}` semantics 변경**: Airflow 2.x에서 `{{ ds }}` = `data_interval_start.date()` = 전날이었으나,
  Airflow 3.x에서는 `{{ ds }}` = `logical_date.date()` = 트리거 당일.
  DAG 템플릿에서 "처리 대상 날짜(어제)"를 참조할 때는 `{{ macros.ds_add(ds, -1) }}` 사용.
  Python 콜백에서는 `(datetime.strptime(str(context["ds"]), "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")`.
- **`$snapshots` 메타테이블에 `sequence_number` 없음**: 이 Iceberg/Spark 버전의 `$snapshots` 실제 컬럼은
  `committed_at, snapshot_id, parent_id, operation, manifest_list, summary` 뿐이다.
  `sequence_number`를 참조하면 `AnalysisException` 발생. 오늘 변경된 파티션 감지 구현 시
  before/after snapshot 비교 방식 사용 (JOBS_GUIDE §3-2 참고).
- **monthly maintenance DAG 수동 트리거 시 logical_date 필수**: `wait_for_daily_maintenance`
  ExternalTaskSensor가 동일 logical_date의 daily maintenance 완료를 감지한다.
  임의 시각으로 트리거하면 sensor가 영원히 `up_for_reschedule`. daily maintenance의
  logical_date(`YYYY-MM-DDT02:00:00+00:00`)와 정확히 맞춰야 한다 (DAGS_GUIDE §4-3 참고).

---

## Spark 코어 배분

```
Worker 3개 × 3코어 = 9코어 총합

kafka-to-raw-impression     cores.max=3  (Streaming, 파티션 3개 병렬)
kafka-to-raw-click          cores.max=1  (Streaming)
kafka-to-raw-conversion     cores.max=1  (Streaming, 메시지 빈도 최저)
raw-to-processed   (배치)   cores.max=2
processed-to-summary (배치) cores.max=2
```

---

## 환경 변수 (.env.example)

```bash
# AWS
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_DEFAULT_REGION=ap-northeast-2

# S3
S3_RAW_BASE=s3a://metacode-criteo-project/raw
GLUE_WAREHOUSE=s3a://metacode-criteo-project/warehouse
CHECKPOINT_BASE=s3a://metacode-criteo-project/checkpoints/kafka_to_raw

# Kafka (컨테이너 내부: kafka:29092 / 호스트: localhost:9092)
BOOTSTRAP_SERVERS=kafka:29092

# Spark
SPARK_NO_DAEMONIZE=true

# HuggingFace 캐시
HF_HOME=/app/hf_cache
```

---

## 자주 쓰는 명령어

```bash
# 전체 스택 기동
docker compose up -d

# Superset 초기화 (최초 1회)
docker compose --profile superset-init up --abort-on-container-exit superset-init
docker compose up -d superset
# http://localhost:8088  admin / admin
# Athena 연결 URI: awsathena+rest://@athena.ap-northeast-2.amazonaws.com:443/silver
#   ?s3_staging_dir=s3://metacode-criteo-project/athena-results/&work_group=primary

# Airflow 기동
docker compose up -d airflow-webserver airflow-scheduler
# http://localhost:8080  admin / admin

# 로그 확인
docker compose logs -f producer
docker compose logs -f kafka-to-raw-impression

# Spark UI: http://localhost:8080 (Master) / Kafka UI: http://localhost:8090

# Silver 일배치
docker compose --profile batch run --rm raw-to-processed

# Gold 일배치
docker compose --profile batch run --rm processed-to-summary

# Silver 날짜 범위 재처리
docker compose --profile batch run --rm raw-to-processed \
  /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
  --conf spark.cores.max=2 /app/jobs/raw_to_processed_iceberg.py \
  --run-date-start 2026-06-01 --run-date-end 2026-06-07

# Gold 날짜 범위 재처리
docker compose --profile batch run --rm processed-to-summary \
  /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
  --conf spark.cores.max=2 /app/jobs/processed_to_campaign_summary.py \
  --run-date-start 2026-06-01 --run-date-end 2026-06-07

# Athena 헬스 쿼리 실행
aws athena start-query-execution \
  --query-string "$(cat health-queries/alert/01_pipeline_no_snapshot_today.sql)" \
  --result-configuration OutputLocation=s3://metacode-criteo-project/athena-results/ \
  --work-group primary
```

---

## 커밋 컨벤션

Conventional Commits + 한국어 subject.

```
feat: Superset docker-compose 연동
fix: DDL COMMENT 세미콜론 파싱 오류 수정
refactor: spark_utils.py 공통 모듈 분리
docs: README.md 평가 답변 섹션 작성
chore: .gitignore __pycache__ 추가
```

---

## TODO

- [ ] Superset 대시보드 스크린샷 (비즈니스 탭 + 운영 탭)
- [ ] README.md: 평가 답변 9개 섹션
- [ ] .env.example 작성
- [ ] Airflow DAG 실행 검증 (medallion + maintenance)
- [ ] S3 UTC vs KST 타임존 처리 — `date.today()` → `datetime.now(timezone.utc).date()` 교체 (JOBS_GUIDE §5-E5)
- [ ] dedup tie-breaking: `kafka_offset` 2차 정렬키 추가 (JOBS_GUIDE §5-E2)
- [ ] producer.py 멱등성: 재시작 시 `seen_conversion_ids` 초기화 문제
- [ ] producer.py Kafka 튜닝 파라미터 (`acks`, `linger_ms`, `batch_size`)
- [ ] `build_spark()` 중복 코드 → spark_utils.py 공통화 (raw_to_processed / processed_to_summary / run_sql.py 3곳 중복)
- [ ] 운영 가시성 개선 — 단기: `make_athena_gate_task()`가 정상 통과 시에도 결과 건수를 `logger.info`로 출력 (현재 0행=정상 구조라 통과 시 아무것도 안 찍힘)
- [ ] 운영 가시성 개선 — 장기: health-queries/ops/, health-queries/business/ SQL을 Superset 차트로 등록 → 일별 수치 추이 자동 누적 (verify task=alert 전용, Superset=가시성 전용으로 역할 분리)
- [ ] DAG verify 게이트 보완 — attribution 변경 파티션(ds-1 외) Silver↔Gold 정합성은 현재 미커버, health-queries/alert 확장 또는 Superset으로 보완 필요 (DAGS_GUIDE §2-3 참고)
