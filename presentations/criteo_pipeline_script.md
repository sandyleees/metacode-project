# Criteo 광고 데이터 파이프라인 — 발표 대본

대상: 부트캠프 멘토 및 동료 수강생 (포트폴리오 리뷰)
예상 발표 시간: 20~25분 (Q&A 제외)

---

## Slide 01 — 표지

안녕하세요, 저는 이번에 Criteo 광고 데이터 파이프라인 프로젝트를 진행한 [이름]입니다.

오늘 발표할 내용은 실제 광고 플랫폼의 데이터 흐름을 모사해 만든 end-to-end 데이터 파이프라인입니다.
Kafka로 이벤트를 수집하고, Spark로 처리하고, S3 Iceberg에 저장한 다음, Athena와 Superset으로
시각화하는 전체 스택을 직접 구현했습니다.

---

## Slide 02 — 프로젝트 개요 및 목표

이 프로젝트의 도메인은 **광고 성과 분석**입니다.

데이터 소스는 Criteo Attribution Dataset이라는 오픈소스 데이터셋인데요,
HuggingFace에서 streaming으로 불러옵니다.
원본 데이터는 impression, click, conversion이 하나의 행에 이미 조인된 형태입니다.
저는 이것을 실제 광고 시스템처럼 **세 개의 별도 이벤트 스트림으로 분리**해서
Kafka에 발행하도록 설계했습니다. 이 차이가 나중에 Silver 레이어 설계에 핵심 제약이 됩니다.

기술 스택은 슬라이드 오른쪽에 보시는 것처럼 메시지큐부터 시각화까지 완전한 데이터 플랫폼 스택입니다.

---

## Slide 03 — 광고 이벤트 데이터 이해

본격적인 설명 전에, 이 파이프라인이 다루는 데이터가 뭔지부터 짚겠습니다.

광고 이벤트는 크게 세 종류입니다.

**impression**은 광고가 화면에 노출되는 이벤트입니다. 광고가 보이는 순간 발생하고, cost(노출 비용)가 생깁니다.

**click**은 사용자가 광고를 실제로 클릭한 이벤트입니다. impression이 발생하고 나서 수초에서 수일 뒤에 도착합니다.

**conversion**은 사용자가 구매나 가입을 완료한 이벤트입니다. impression 후 수일에서 수십일 뒤에 도착합니다.

여기서 핵심 문제가 있습니다. **같은 사용자 행동인데 이벤트가 도착하는 시점이 다릅니다.**
impression은 지금 저장했는데, 그에 대한 click은 7일 뒤, conversion은 30일 뒤에 도착할 수 있습니다.
파이프라인이 매일 배치로 돌더라도 오늘 impression의 성과를 오늘 당장 알 수 없다는 뜻입니다.
이 문제를 Silver 레이어에서 2-Stage MERGE로 해결합니다.

또 하나 중요한 개념이 **Attribution Window**입니다.
"이 impression 덕분에 conversion이 발생했다"고 인정하는 시간 한도입니다.
이 프로젝트에서는 업계 표준을 따라 click window 7일, conversion window 30일로 설정했습니다.
Google이나 Meta 같은 주요 광고 플랫폼도 같은 기준을 씁니다.
이 window 값이 나중에 Silver 파티션 스캔 범위와 Iceberg compaction 35일 기준, Gold KPI 정확도에 모두 영향을 줍니다.

---

## Slide 04 — 전체 아키텍처

전체 흐름을 한 눈에 보면 이렇습니다.

Criteo 데이터를 producer.py가 읽어서 Kafka 3개 토픽으로 발행합니다.
Spark Structured Streaming이 각 토픽을 구독해서 S3에 Parquet 파일로 적재합니다. 이게 Bronze입니다.

Bronze는 **원본 보존 영역**입니다. 나중에 처리 로직에 버그가 생기면 이 원본에서 재처리할 수 있어야 하니까요.

그 다음 Airflow가 매일 새벽 2시(UTC)에 Spark 배치 잡을 실행해서 Bronze를 Silver로 가공합니다.
Silver는 중복 제거와 Attribution 조인이 된 행 단위 이벤트 테이블입니다.

Silver를 다시 campaign × 날짜 기준으로 집계한 게 Gold입니다.
Gold가 Athena와 Superset의 실제 조회 대상입니다.

배치 사이사이에 Athena 검증 쿼리를 심어뒀습니다. 다만 검증 범위에 한계가 있어서 100% 정합성 보장은 아닙니다. 이 한계는 DAG 슬라이드에서 설명하겠습니다.

---

## Slide 05 — 왜 Apache Iceberg인가

Silver와 Gold에 왜 일반 Parquet 대신 Iceberg를 썼는지 먼저 짚고 가겠습니다.

가장 큰 이유는 **MERGE INTO**입니다.
Silver에서는 나중에 도착한 click/conversion으로 기존 impression 행을 업데이트해야 합니다.
일반 Parquet이면 파티션 전체를 DROP하고 INSERT해야 하는데, 이건 원자적이지 않아서
Athena가 중간에 빈 테이블을 볼 수 있습니다.
Iceberg는 MERGE INTO를 네이티브로 지원해서 멱등한 upsert가 가능합니다.
이 프로젝트에서는 단일 파이프라인이라 동시 쓰기 상황은 없지만, Iceberg 자체는 낙관적 동시성 제어를 갖추고 있습니다.

두 번째는 **스냅샷 기반 Time Travel**입니다.
Gold 배치가 "오늘 Silver에서 변경된 파티션만 재집계"하려면 배치 이전 Silver 상태를 알아야 합니다.
Iceberg의 스냅샷 히스토리가 이 기반이 됩니다. 나중에 Gold 슬라이드에서 자세히 설명하겠습니다.

세 번째는 **파일 수준 min/max 통계**입니다.
일반 Parquet은 파티션 단위 필터링만 되는데, Iceberg는 manifest에 파일 단위 컬럼 min/max를 기록합니다.
Silver에서 attribution JOIN 시 파티션 안에서도 불필요한 파일을 추가로 건너뛸 수 있습니다.

네 번째는 **COW / MOR 전략 선택**입니다.
Silver와 Gold의 쓰기 패턴이 달라서 레이어별로 다른 전략을 선택했는데, 이것도 곧 설명드리겠습니다.

---

## Slide 06 — Medallion 레이어별 설계 개요

세 레이어의 핵심 설계 결정을 요약하면 이렇습니다.

Bronze는 Parquet append-only입니다. 원본을 그대로 보존하는 게 목적이라 Iceberg가 필요 없습니다.
파티션은 raw_date와 raw_hour로 나뉘는데, 여기서 raw_date의 기준이 event_time이 아니라
**ingest_ts(Spark 수집 시각)**인 것이 중요합니다. 이유는 다음 슬라이드에서 설명합니다.

Silver는 Iceberg MOR, event_date 파티션입니다.
MERGE INTO ON (event_id, event_date)로 멱등성을 보장합니다.

Gold는 Iceberg COW, summary_date 파티션입니다.
MERGE INTO ON (summary_date, campaign)로 멱등성을 보장합니다.

왜 Silver는 MOR이고 Gold는 COW인지가 이 프로젝트에서 가장 고민한 설계 결정입니다.

---

## Slide 07 — Bronze 상세

**Kafka 3토픽 분리 설계**부터 설명하겠습니다.

세 이벤트 타입을 왜 분리했냐면, impression에만 cost 필드가 있어서 스키마가 다르기 때문입니다.
하나의 토픽에 묶으면 역직렬화 시 null 컬럼 충돌이 생깁니다.
분리하면 토픽별로 독립적인 장애 복구와 코어 배분이 가능합니다.

Spark Worker가 3대에 총 9코어인데, impression Streaming에 3코어,
click과 conversion에 각 1코어를 배분했습니다.
impression이 트래픽 중에서 가장 많으니까요.
남은 4코어는 Silver/Gold 일배치에 씁니다.

**raw_date를 ingest_ts 기준으로 한 이유**도 중요합니다.
event_time 기준으로 하면, 오늘 수집된 conversion이 30일 전 impression에 대한 것일 수 있어서
오늘 수집했는데 30일 전 파티션 파일에 기록됩니다.
파티션 크기를 예측할 수 없고 파일이 편중됩니다.
ingest_ts 기준이면 오늘 수집된 건 모두 오늘 파티션에 균등하게 쌓입니다.
Silver에서 event_date(impression 발생 기준)로 재파티셔닝하기 때문에 분석 정확도에는 영향이 없습니다.
추가로, ingest_ts 기준이면 장애 복구 시 "이 날짜 파티션부터 Silver 재처리가 필요하다"는 범위를 수집 시각으로 특정할 수 있습니다. event_time 기준이었다면 장애 직전 수집분이 여러 날짜 파티션에 산재해 범위 파악이 복잡해졌을 겁니다.

Bronze는 append-only 원본 저장이 목적이라 MERGE나 Time Travel 같은 Iceberg 기능이 필요하지 않습니다. 일반 Parquet으로 충분합니다.

**Kafka Checkpoint**로 Spark Streaming이 크래시 후 재시작해도 마지막 처리한 Kafka offset에서
이어서 처리할 수 있습니다. 토픽별 독립 checkpoint 경로를 쓰는 게 중요한데,
공유하면 offset 진행 상태가 섞여서 재처리 시 누락이나 중복이 생깁니다.

한계로는, Kafka 브로커를 단일로만 구성해서 브로커가 죽으면 메시지가 손실됩니다.
운영 환경이라면 3개 이상 브로커로 replication을 구성해야 합니다.
producer도 재시작하면 데이터셋 처음부터 다시 읽어서 impression, click, conversion 모든 이벤트가 중복 발행될 수 있는데, Silver MERGE가 흡수해서
downstream 영향은 제한적이긴 합니다.

---

## Slide 08 — Silver 상세

Silver의 핵심 설계는 **2-Stage 배치**입니다.

왜 2-Stage인지 구체적으로 설명하면, Silver 배치가 오늘(6/25) 돌면 어제(6/24) Bronze raw_date=6/24 파티션을 처리합니다.
어제 Bronze에는 impression은 6/24 발생한 것들이 있고, click은 어제 도착했지만
원본 impression이 6/17~6/24 사이에 발생한 것들입니다.
click window가 7일이니까요.
conversion은 더해서 5/25~6/24 사이 impression에 대한 것들입니다.

1-Stage로 "어제 impression + 어제 click JOIN"을 하면,
click은 어제 Bronze에 있지만 6/17 impression은 어제 Bronze에 없습니다. 매칭이 안 됩니다.
매일 최대 30일치 Bronze를 읽어야 해결되는데 너무 비쌉니다.
(conversion window 30일 기준)

그래서 2-Stage로 설계했습니다.

**Stage 1**: 어제 Bronze의 impression을 Silver에 INSERT합니다.
impression의 event_date는 event_time 기준이라 여러 날짜에 분산될 수 있습니다.
이미 Silver에 있는 impression은 INSERT하지 않아 멱등성이 보장됩니다.
click=0, conversion=0으로 초기값을 채웁니다.

**Stage 2**: 어제 Bronze의 click과 conversion을 가져와서, Silver에 이미 있는 impression 행을
eid로 찾아서 MERGE UPDATE합니다.

이때 Silver 전체 파티션을 다 열지는 않습니다. 3단계로 스캔 범위를 좁힙니다.
먼저, JOIN 조건에 event_date 파티션 범위 힌트를 넣어 Iceberg manifest에서 window 밖 파티션을 제거합니다.
그 다음, manifest에 기록된 파일 단위 eid min/max 통계로, eid 범위가 맞지 않는 파일을 추가로 건너뜁니다.
마지막으로, 남은 파일 안에서 eid로 실제 impression 행을 찾습니다. row-level index는 없어서 파일을 열어야 합니다.

MERGE INTO ON (event_id, event_date)로 재실행이 안전합니다.
동일 날짜를 다시 실행하면 이미 있는 impression은 INSERT하지 않고 넘어갑니다.

MOR를 선택한 이유는 UPDATE 비율이 낮기 때문입니다.
전체 impression 중 click이나 conversion이 매칭되는 게 약 2% 정도입니다.
COW를 쓰면 2% 행이 바뀌어도 그 파티션 파일 전체를 새로 써야 합니다.
MOR는 변경된 행만 delete file로 append합니다. 훨씬 효율적입니다.
읽기 시 delta 병합 오버헤드가 생기는데, 이건 뒤에 나오는 유지보수 DAG의 daily compaction이 해결합니다.

한 가지 주의할 점은, Silver도 백필 시 반드시 `--run-date-start`를 지정해서 명시 재처리 모드로 실행해야 합니다.
기본 모드인 snapshot diff는 "오늘 첫 번째 스냅샷의 parent"를 기준으로 비교하는데, 같은 날 여러 번 실행하면 이 기준이 틀어져서 변경 감지가 깨집니다.

---

## Slide 09 — Gold 상세

Gold 배치의 핵심 아이디어는 **"일배치니까 오늘 Silver에서 바뀐 것만 재집계하면 된다"**입니다.

Silver 전체를 매번 재스캔하는 건 낭비입니다.
attribution window 기준으로 최근 30일치만 봐도 되지만, 그것도 매일 전부 재집계하면 비효율입니다.
오늘 실제로 변경된 event_date 파티션만 정확히 찾아서 그것만 재집계하는 게 가장 효율적입니다.
그 방법으로 Iceberg 스냅샷 diff를 씁니다.

구체적으로는 오늘 Silver 첫 번째 커밋의 parent_id를 찾습니다.
그 parent가 배치 이전 상태입니다.
현재 스냅샷과 parent 스냅샷의 event_date별 행수를 비교해서 달라진 날짜를 찾습니다.
당일 신규 impression뿐 아니라 지연 attribution으로 변경된 과거 파티션도 자동으로 포함됩니다.

왜 `.changes()` changelog scan을 쓰지 않았냐면, Silver가 MOR라서 delete file이 있기 때문입니다.
`.changes()`는 delete file을 포함한 테이블에서 예외를 던집니다.
이건 실제로 에러를 겪으면서 알게 됐습니다.

COW를 선택한 이유는 Silver와 정반대입니다.
Gold MERGE를 실행할 때, 변경된 event_date의 모든 campaign을 재집계합니다.
예를 들어 6/23에 campaign이 7, 9 두 개 있는데 7번 campaign에만 변경이 생겼어도,
재집계한 staged DataFrame에는 7번과 9번 둘 다 포함됩니다.
그래서 6/23 파티션의 campaign 행 전부가 MATCHED UPDATE됩니다.
UPDATE 비율이 ~100%입니다. 이 경우 MOR의 이점이 없고 오히려 delete file 병합 오버헤드만 생깁니다.
Gold는 Superset/Athena에서 가장 많이 조회하는 테이블입니다. 읽기 성능이 최우선이므로 COW가 적합합니다.
COW는 파티션 파일을 새로 쓰지만 항상 clean하니까 Athena 스캔 성능이 일정합니다.

KPI는 CTR, CVR, CPC, CPA, CPM, frequency, click-through/view-through conversion 등을 계산합니다.
CPC같이 분모가 0이 될 수 있는 지표는 `when(clicks > 0, ...)` 패턴으로 NULL을 보호했습니다.

---

## Slide 10 — Airflow DAG 구조

DAG는 총 3개로 구성했습니다.

**criteo_medallion_daily**가 핵심입니다.
매일 UTC 02:00에 실행되고, Bronze 파티션 파일 존재 확인 → Silver 배치 → 검증 → Gold 배치 → 검증 순서입니다.
여기서 "처리 대상 날짜"는 어제입니다. 어제 수집된 Bronze를 처리하는 거고, 따라서 Silver에 어제 event_date 파티션이 새로 생깁니다. 거기에 attribution으로 어제 이전 파티션들도 업데이트될 수 있습니다.

검증 게이트 설계가 중요한데, "0행 = 정상" 패턴입니다.
여기서 기준이 둘로 나뉩니다. 오늘은 배치가 실행된 날이고, 어제는 Bronze 처리 대상일입니다.
Silver 배치가 Spark exit 0으로 종료해도 Iceberg 커밋이 없으면 데이터가 없는 겁니다.
`verify_silver_snapshot`은 오늘 배치가 Silver에 커밋을 남겼는지 확인합니다.
`verify_silver_rows`는 어제(처리 대상) event_date 파티션에 실제로 행이 있는지 확인합니다.
Gold 쪽도 마찬가지로 오늘 배치의 snapshot 확인과 Silver↔Gold 집계 일치를 검증합니다.
여기서 10% 이내 차이를 허용하는 이유는, attribution 지연으로 Silver에서 이미 처리된 데이터와 Gold 집계가 완전히 일치하지 않을 수 있어서 보수적으로 기준을 잡은 겁니다.
이 둘이 모두 통과해야 다음 Task로 진행됩니다.

**criteo_iceberg_maintenance**는 medallion DAG의 gold_batch가 성공하면 ExternalTaskSensor로 감지해서 시작합니다.
Silver는 MOR 특성상 delete file이 매일 누적되니까 daily compaction이 필수입니다.
compaction 대상 범위를 35일로 잡은 이유는, attribution window가 30일이니까
30일 전 파티션도 오늘 업데이트될 수 있고, 거기에 여유 5일을 더한 겁니다.

**criteo_iceberg_maintenance_monthly**는 매월 1일에 manifest 파일을 재편성합니다.
expire_snapshots로 오래된 스냅샷을 제거해도 manifest 파편화는 남습니다.
월 1회 rewrite_manifests로 정리해서 쿼리 플래닝 오버헤드를 줄입니다.

한계를 솔직히 말씀드리면, 이 DAG 구조는 Silver/Gold 테이블이 각각 1개라는 전제에 완전히 의존합니다.
새 테이블이 하나 추가되면 medallion DAG, maintenance DAG를 전부 수정해야 하는 구조입니다.
처음부터 레이어별 유지보수 DAG로 분리했으면 더 나은 확장성을 가졌을 것 같습니다.

그리고 백필 시 Airflow UI에서 날짜를 지정하는 연결이 미구현 상태라서, docker compose 명령을 직접 실행해야 합니다.

---

## Slide 11 — Iceberg 유지보수 전략

유지보수 전략도 Silver와 Gold가 다릅니다.

Silver는 MOR이라서 MERGE가 매일 delete file을 append합니다.
`compact_silver`가 이 delete file을 base data file에 흡수합니다. 읽기 오버헤드를 제거하는 거죠.
대상 범위를 `event_date >= 오늘-35일`로 한정한 이유는 아까 설명한 것처럼 attribution 35일 경계입니다.
더 오래된 파티션은 더 이상 업데이트가 없으니까 compaction 대상에서 제외해서 비용을 줄입니다.

expire_snapshots로 30일 이상 된 스냅샷을 제거합니다. 30일 넘은 스냅샷으로 롤백할 일은 없으니까요.
orphan 파일은 커밋 실패로 메타데이터에 등록되지 못한 고아 파일을 제거합니다.
4일 이상 된 것만 제거하는 이유는 현재 진행 중인 쓰기와 충돌하지 않기 위해서입니다.

한 가지 짚고 넘어가면, 스냅샷 보존 기간은 31일인데 metadata.json 버전 보존은 7일치(21개)입니다.
스냅샷은 롤백과 시간여행을 위해 길게 유지하고, metadata.json 파일 수는 무한 누적을 막기 위해 짧게 관리하는 것으로 역할이 다릅니다.

Gold는 COW라서 MOR처럼 delete file이 쌓이는 문제는 없습니다. 그래서 매일 compaction이 필수는 아닙니다.
다만 현재 데이터 규모에서 파티션당 Parquet 파일이 1개 수준이라 소형 파일 누적 자체가 없어서 불필요한 것이기도 합니다.
데이터 규모가 커져서 파일이 여러 개로 쪼개지기 시작하면 Gold도 compaction이 필요해질 수 있습니다.
지금은 expire와 orphan 정리만 합니다.

monthly 유지보수는 manifest 파일 재편성입니다.
Iceberg는 스냅샷마다 manifest 계층이 쌓이는데, expire 후에도 파편화된 채 남습니다.
월 1회 rewrite_manifests로 소수의 큰 파일로 재편성해서 쿼리 플래닝 속도를 유지합니다.

MERGE와 Compaction이 동시에 실행되면 Iceberg 낙관적 잠금 충돌이 발생할 수 있습니다.
ExternalTaskSensor로 medallion DAG의 gold_batch SUCCESS를 확인하고 maintenance를 시작해서
이 충돌을 원천 차단했습니다.

---

## Slide 12 — Superset 대시보드

Superset 대시보드는 두 가지로 나눠 설계했습니다.

**비즈니스 대시보드(Campaign팀/BI팀용)**는 Gold를 씁니다.
CTR, CVR, CPC, CPA 같은 캠페인 성과 지표와 conversion 지연 분포를 보여줍니다.
Gold는 집계 테이블이라 Silver 대비 Athena 스캔 비용이 훨씬 낮습니다.

**운영 대시보드(DE용)**는 Silver를 데이터 소스로 씁니다.
일별 트래픽 볼륨 추이, attribution 커버리지, 파이프라인 지연 추이 같은 파이프라인 정상 동작 여부 확인용입니다.

Superset Dataset 캐싱은 아직 구현하지 않았는데, 24h 캐싱을 적용하면 하루 1번 Athena 스캔으로 여러 팀이 조회해도 비용이 1번만 발생해서 비용 절감이 가능합니다.

[스크린샷 placeholder 설명: 추후 대시보드 캡쳐 삽입 예정]

---

## Slide 13 — 솔직한 회고

마지막으로 이 프로젝트의 한계와 진행하면서 느낀 점을 솔직하게 정리하겠습니다.

**설계 한계 — 고려하지 못한 것들**

인프라 HA를 고려하지 못했습니다.
Kafka 단일 브로커라서 브로커 장애 시 메시지가 손실됩니다.
Spark Worker도 단일 물리 머신에 다 올라가 있어서 실제 분산 환경이라고 할 수 없습니다.
장애 복구 절차도 설계하지 못했습니다.

플랫폼 확장성도 부족합니다.
테이블이 Silver/Gold 각 1개라는 전제가 하드코딩되어 있어서
테이블이 늘어나면 DAG 전체를 수정해야 합니다.
또한 Bronze는 현재 Silver에 문제가 생기지 않는 한 다시 읽을 일이 없다고 판단해서 compaction을 만들지 않았는데,
Silver 테이블이 여러 개로 늘어나면 Bronze를 읽는 빈도도 늘어나서 Bronze compaction Spark job을 별도로 만들 필요가 생길 수 있습니다.
데이터가 10배 늘었을 때 파티션 전략이나 코어 배분을 어떻게 바꿔야 할지도 설계하지 못했습니다.

날짜 기준이 UTC로 고정되어 있습니다.
Kafka, Spark, Airflow, S3 모두 UTC 기준으로 동작합니다.
한국 시간 자정은 UTC로 전날 오후 3시인데, 이 경계에서 처리하면 날짜가 어긋날 수 있습니다.
지금은 Airflow를 KST 낮 시간대에 실행해서 회피하고 있는데, 이건 근본 해결이 아닙니다.

백필 운영도 미흡합니다.
CLI 인자로 날짜 범위를 지정할 수 있는데, Airflow UI에서 트리거할 때 날짜를 넘기는 연결이 아직 구현되지 않았습니다.
백필이 필요하면 docker compose 명령을 직접 실행해야 합니다.

Compaction 정렬 전략도 고려하지 못했습니다.
현재 bin-pack은 파일 크기만 균등하게 맞추고 행 정렬은 하지 않습니다.
Silver에 event_id 정렬, Gold에 campaign 정렬을 적용하면 조회 성능이 개선될 수 있는데 이 부분은 생각하지 못했습니다.

마지막으로 Gold 집계 버그가 있는데요, 발표 준비하면서 발견한 내용입니다.
Silver 스냅샷을 전후 비교해서 변경된 파티션만 Gold에서 재집계하는 방식을 썼는데,
click이나 conversion은 기존에 있던 행의 값만 바뀌는 거라서 행 수는 그대로입니다.
행 수가 안 바뀌면 변경으로 감지가 안 돼서 Gold 재집계가 누락됩니다.
당장 수정하진 못하고 이 상태로 발표하게 됐습니다.

**프로젝트 진행 후기**

CS 기반이 부족해서 환경 설정에서 막혔습니다.
환경변수, 포트, 네트워크, 엔드포인트 같은 개념 자체가 처음이었고,
`.config()` 자격증명이나 docker-compose, Dockerfile 설정은 Claude Code에 맡기고
앱이 원하는 대로 돌아가면 된 거겠지 하고 진행했습니다.

기능 단위 테스트를 하지 않았습니다.
기능을 여러 개 붙이고 파이프라인을 한 번에 돌려서 오류를 역추적하는 방식으로 작업했는데,
기능 하나 추가할 때마다 소규모 테스트로 검증하고 붙여나갔으면 훨씬 효율적이었을 것 같습니다.

적재 데이터를 직접 검증하지 않았습니다.
이건 기능 테스트와는 다른 레이어의 이야기인데, 파이프라인이 오류 없이 돌면 데이터가 잘 들어온 거겠지 하고 넘어갔습니다.
Silver의 click_flag가 실제로 업데이트됐는지, 중복 행은 없는지, attribution이 맞게 붙었는지 같은 걸
테이블을 만들 때마다 소규모 Athena 쿼리로 여러 케이스를 확인했어야 했습니다.
코드가 오류 없이 돌아간다는 것과 데이터가 올바르게 쌓인다는 건 별개의 문제인데 그 구분을 못 했습니다.

Spark 자원 배분 고민을 못 해봤습니다.
worker나 executor를 늘려서 처리량을 높인다는 개념은 알지만,
job별로 자원을 구체적으로 어떻게 설계해야 하는지까지는 못 해봤습니다.

Claude Code를 처음 써봤는데 AI가 써준 코드를 읽는 것부터 막혔습니다.
구현됐다고 하면 코드를 안 읽고 패스하는 식으로 진행했습니다.
코드 읽기부터 막히는 수준인데 직접 쓰기까지 가는 건 갭이 얼마나 클지 모르겠고,
바이브코딩·노코드 추세에서 코드 읽기/쓰기를 얼마나 직접 해야 하는지 방향이 아직 안 잡힙니다.

이상으로 발표를 마치겠습니다.

---

## Slide 14 — 감사합니다

감사합니다.

---

## 예상 Q&A 준비

**Q1. Silver에서 attribution window를 벗어난 click은 어떻게 처리되나요?**
A. Stage 2 JOIN 조건에서 window 밖이면 매칭이 안 됩니다. 그 click은 업데이트되지 않고 버려집니다.
   이건 의도된 설계입니다. 7일 지난 click을 이 impression 덕분이라고 인정하지 않는 게
   attribution의 비즈니스 로직이니까요.

**Q2. Bronze 파티션이 없으면 Silver 배치가 어떻게 되나요?**
A. S3KeySensor가 Bronze 파티션 파일 존재를 1시간 동안 대기합니다.
   1시간 내에 파일이 없으면 FAILED로 마킹되고 Silver 배치가 시작되지 않습니다.
   이렇게 해서 빈 배치로 Silver가 실행되는 걸 막습니다.

**Q3. Silver↔Gold 집계 정합성 검증에서 10% 기준은 어떻게 정했나요?**
A. 솔직히 경험 기반의 수치가 아니라 보수적으로 잡은 임의 기준입니다.
   attribution으로 인해 Silver의 event_date 분포가 Gold 집계와 완전히 일치하지 않을 수 있어서
   약간의 여유를 두었습니다. 실제 운영이라면 정상 운영 기간의 데이터를 보고
   적정 임계값을 설정해야 합니다.

**Q4. Airflow 3.x에서 {{ ds }} semantics 문제는 어떻게 해결했나요?**
A. Airflow 2.x에서는 `{{ ds }}`가 전날 날짜였는데, 3.x에서는 실행 당일 날짜입니다.
   처음에 2.x 기준으로 코딩해서 처리 대상 날짜가 하루 밀리는 버그가 있었습니다.
   `{{ macros.ds_add(ds, -1) }}`로 명시적으로 전날을 참조하도록 수정했습니다.
   이런 버전별 semantics 차이를 직접 겪으면서 알게 됐습니다.

**Q5. 실시간 처리가 아니라 일배치인데 Kafka가 필요한가요?**
A. 좋은 질문입니다. Kafka는 두 가지 역할을 합니다.
   첫째는 데이터 소스와 처리 파이프라인 간의 버퍼입니다.
   producer 속도와 Spark 처리 속도가 달라도 Kafka가 완충해줍니다.
   둘째는 Bronze 레이어의 Streaming 적재 기반입니다.
   실제 광고 시스템에서는 impression이 실시간으로 발생하고,
   이를 실시간으로 수집해야 하는 요구사항이 있습니다.
   일배치는 Silver/Gold 가공 단계이고, Bronze 수집은 24/7 Streaming으로 운영합니다.
