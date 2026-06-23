# DAG 의존 관계

## 실행 흐름

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

## 파일 구조

| 파일 | 역할 |
|---|---|
| `dag_config.py` | 공통 상수·환경변수 (`S3_RAW_BUCKET`, `SPARK_CONF`, `ATHENA_RESULTS_S3`, `spark_env_vars()`) |
| `dag_utils.py` | 공통 팩토리 (`make_sql_task()`, `make_athena_gate_task()`) |
| `criteo_medallion_dag.py` | Bronze 확인 → Silver → [검증] → Gold → [검증] 일배치 |
| `criteo_maintenance_dag.py` | Compaction / Expire / Orphan 일간 유지보수 |
| `criteo_maintenance_monthly_dag.py` | manifest 재편성 월간 유지보수 |

## 주요 설계 결정

- **ExternalTaskSensor**: medallion → maintenance → monthly 완료 순서를 스케줄이 아닌 실제 Task SUCCESS로 보장
- **make_athena_gate_task()**: 0행=정상 구조의 Athena 검증 게이트를 PythonOperator로 생성
  - `AthenaHook.run_query()` → `poll_query_status()` → `get_query_results()`
  - 결과 행이 1개 이상이면 `AirflowException` — 하위 task 진입 차단
  - SQL 내 `{ds}`는 Airflow 실행 날짜(`context["ds"]`)로 치환
- **make_sql_task()**: `run_sql.py`를 SparkSubmitOperator로 호출 — task마다 JVM 기동 ~45s  
  task 수 증가 시 `jobs/iceberg_maintenance.py` 전용 스크립트로 교체 검토
- **max_active_runs=1**: Silver/Gold MERGE 동시 실행 방지
- **catchup=False**: 유지보수·manifest 재편성은 최신 실행만 의미 있음

## 검증 게이트 설계 원칙

| task | 감지 대상 | 통과 조건 |
|---|---|---|
| `verify_silver_snapshot` | Silver Iceberg 커밋 누락 | `MAX(committed_at) >= current_date` |
| `verify_silver_rows` | Silver `{ds}` 파티션 빈 배치 | `COUNT(*) > 0` |
| `verify_gold_snapshot` | Gold Iceberg 커밋 누락 | `MAX(committed_at) >= current_date` |
| `verify_silver_gold_consistency` | Silver↔Gold 집계 불일치 또는 Gold 파티션 부재 | `diff_pct ≤ 10%` AND Gold 파티션 존재 |

`health-queries/alert/`의 4개 SQL과 목적이 비슷하지만 다른 점:
- DAG 검증 게이트: `{ds}` 기준 (당일 배치 결과 즉시 검증, Gold 진입 차단)
- `health-queries/alert/`: `current_date` 기준 (독립 실행, Airflow AthenaOperator 별도 스케줄)
