# HEALTH_QUERIES_GUIDE.md

코드의 **무엇(what)** 은 각 SQL 파일 상단 주석이 설명하고, 이 문서는 **왜(why)** 를 설명한다.

---

## 역할 분리 원칙

| 디렉토리 | 실행 주체 | 실행 방식 | 주기 |
|---|---|---|---|
| `alert/` | Airflow 자동 | AthenaOperator → PythonOperator 행 수 검증 | 매일 |
| `ops/` | 운영 엔지니어 | Superset Dataset (24h 캐시) | 매일·주간 |
| `infra/` | 운영 엔지니어 | aws athena CLI 수동 | 주간·월간 |
| `business/` | BI팀 | Superset Dataset (캐시) | 대시보드 상시 |
| `incident/` | 온콜 엔지니어 | aws athena CLI 수동, DATE 리터럴 수정 후 실행 | 장애 시 |

**DE가 보는 것**: alert/ + ops/ + infra/ — 파이프라인이 정상 작동하는가  
**DE가 보지 않는 것**: business/ — CTR/CVR/CPC 성과 판단은 Campaign팀·BI팀 몫

---

## 쿼리별 실행 주기

| 쿼리 | 주기 | 실행 방식 |
|---|---|---|
| alert/01 snapshot freshness | 매일 자동 | Airflow AthenaOperator |
| alert/02 silver-gold consistency | 매일 자동 | Airflow AthenaOperator |
| alert/03 missing dates | 매일 자동 | Airflow AthenaOperator |
| alert/04 latency breach | 매일 자동 | Airflow AthenaOperator |
| ops/01 daily volume trend | 매일 | Superset Dataset 24h 캐시 |
| ops/02 dedup rate | **주간** | Superset Dataset 주간 refresh |
| ops/03 attribution coverage | 매일 | Superset Dataset 24h 캐시 |
| ops/04 pipeline latency trend | 매일 | Superset Dataset 24h 캐시 |
| infra/01 compaction effect | 주간 | aws athena CLI 수동 |
| infra/02 manifest trend | 월간 | aws athena CLI 수동 (monthly DAG 전후) |
| infra/03 snapshot retention | 주간 | aws athena CLI 수동 |
| business/01 kpi trend | 매일 | Superset Dataset 24h 캐시 |
| business/02 conversion delay dist | **주간** | Superset Dataset 주간 refresh |
| incident/01, 02 | 장애 시 | aws athena CLI 수동 (DATE 리터럴 수정 후 실행) |

ops/02(dedup), business/02(conversion delay)를 주간으로 내린 이유는 **비용이 아니라 정보 변화 속도** 때문.  
Kafka 중복률·conversion 지연 분포는 일간 변동이 의미 없고 주간 추이로 봐야 패턴이 보임.

---

## Athena 과금 기준

- **$snapshots / $files / $manifests 메타테이블**: 사실상 무료 — 크기가 수 KB 수준
- **Silver processed_events 스캔**: 💰 파티션 프루닝(`WHERE event_date`) 필수. COUNT DISTINCT·APPROX_PERCENTILE 연산은 특히 비쌈
- **Gold campaign_summary**: 집계 테이블이라 Silver 대비 소량 — 비교적 저렴
- **핵심 비용 대응 전략**: 빈도를 줄이는 것보다 **Superset Dataset 24h 캐싱**이 효과적.  
  Silver 14일치 쿼리를 하루 1번 캐싱하면 여러 팀이 조회해도 Athena 스캔은 1번만 발생

---

## 설계 원칙

- `alert/` 쿼리는 **0행=정상** 구조로 작성 → Airflow PythonOperator가 `len(rows) > 0`이면 AirflowException
- Silver 스캔 쿼리는 반드시 `WHERE event_date >= ...` 파티션 조건 포함
- 💰 표기 쿼리는 Superset Dataset 캐싱으로 운영 — 하루 1번 스캔으로 여러 팀 조회 커버
- `event_date < current_date` 조건은 Athena(UTC 기준) 자정 이전에는 당일 데이터를 반환하지 않음  
  → Airflow 스케줄을 KST 10:00 이후로 설정하면 영향 없음 (JOBS_GUIDE §5-E5 참고)
