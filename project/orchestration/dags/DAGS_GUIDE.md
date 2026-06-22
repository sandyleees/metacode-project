# DAG 의존 관계

## 실행 흐름

```
criteo_medallion_daily (매일 UTC 02:00)
  check_bronze_impressions → silver_batch → gold_batch
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
| `dag_config.py` | 공통 상수·환경변수 (`S3_RAW_BUCKET`, `SPARK_CONF`, `spark_env_vars()`) |
| `dag_utils.py` | 공통 팩토리 (`make_sql_task()`) |
| `criteo_medallion_dag.py` | Bronze 확인 → Silver → Gold 일배치 |
| `criteo_maintenance_dag.py` | Compaction / Expire / Orphan 일간 유지보수 |
| `criteo_maintenance_monthly_dag.py` | manifest 재편성 월간 유지보수 |

## 주요 설계 결정

- **ExternalTaskSensor**: medallion → maintenance → monthly 완료 순서를 스케줄이 아닌 실제 Task SUCCESS로 보장
- **make_sql_task()**: `run_sql.py`를 SparkSubmitOperator로 호출 — task마다 JVM 기동 ~45s  
  task 수 증가 시 `jobs/iceberg_maintenance.py` 전용 스크립트로 교체 검토
- **max_active_runs=1**: Silver/Gold MERGE 동시 실행 방지
- **catchup=False**: 유지보수·manifest 재편성은 최신 실행만 의미 있음
