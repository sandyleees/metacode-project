# INGESTION_GUIDE.md

코드의 **무엇(what)** 은 producer.py 자체가 설명하고, 이 문서는 **왜(why)** 를 설명한다.

---

## 목차

1. [원본 Criteo 데이터 구조](#1-원본-criteo-데이터-구조)
2. [3토픽 분리 설계](#2-3토픽-분리-설계)
3. [시간 재매핑 설계](#3-시간-재매핑-설계)
4. [알려진 한계](#4-알려진-한계)

---

## 1. 원본 Criteo 데이터 구조

Criteo Attribution Dataset은 **한 행에 impression + click + conversion이 모두 포함된** Silver 수준 데이터다.

```
timestamp | uid | campaign | click | conversion | conversion_id | conversion_timestamp | cost | ...
----------+-----+----------+-------+------------+---------------+---------------------+------+
1000      | 42  | 7        | 1     | 0          | -             | -                   | 0.5  |
1020      | 42  | 7        | 0     | 1          | 9001          | 87400               | 0.5  |
1020      | 42  | 7        | 0     | 1          | 9001          | 87400               | 0.5  |  ← 동일 conversion_id 중복 행
```

실제 광고 시스템에서는 impression → click → conversion이 **시간차를 두고 별도 이벤트로** 도착한다.  
producer.py는 이 원본 구조를 분해해 실시간 3토픽 스트림을 흉내낸다.

---

## 2. 3토픽 분리 설계

### 2-1. impression — 즉시 발행

메인 루프에서 매 행마다 바로 `producer.send()`.  
impression은 "지금 이 순간 노출됐다"는 이벤트이므로 지연 없음.

### 2-2. click / conversion — PriorityQueue + 별도 스레드 지연 발행

click과 conversion은 impression보다 **나중에** 도착해야 실제 광고 이벤트 흐름과 같아진다.

| 이벤트 | 원본 지연 | 발행 방식 |
|---|---|---|
| impression | 없음 | 메인 루프 즉시 send |
| click | impression 후 1~30초 (랜덤) | PriorityQueue → 워커 스레드 지연 send |
| conversion | impression 후 최대 수십일 | PriorityQueue → 워커 스레드 지연 send (추가 압축) |

**왜 PriorityQueue인가?**  
click과 conversion은 도착 순서가 뒤섞일 수 있다. PriorityQueue에 `send_at` 시각을 우선순위로 넣으면 워커 스레드가 항상 가장 이른 발행 시각 순으로 처리한다.

**왜 별도 스레드인가?**  
메인 루프가 다음 impression을 처리하는 동안 이전 click/conversion을 지정된 시각에 발행해야 한다. 단일 스레드로는 `time.sleep()` 중 인상 발행이 멈춘다.

**`seq` 역할**  
PriorityQueue는 `(send_at, seq, topic, message)` 튜플을 비교한다.  
`send_at`이 동일하면 두 번째 요소인 `seq`를 비교하므로, dict 비교 시도를 막기 위해 단조 증가 정수로 충돌을 방지한다.

### 2-3. conversion 중복 제거

원본 데이터에 동일한 `conversion_id`가 여러 행에 걸쳐 중복 존재한다.  
`seen_conversion_ids`(set)에 이미 발행한 ID를 기록해 첫 번째 행만 Kafka로 흘려보낸다.  
`set`을 쓰는 이유: `in` 연산이 list O(n) 대비 O(1).

---

## 3. 시간 재매핑 설계

Criteo 원본 `timestamp`는 **0부터 시작하는 상대 시각(초 단위 정수)**이다.  
impression 행마다 0, 1, 2, 3... 순으로 증가하고, `conversion_timestamp`도  
해당 impression 기준 몇 초 뒤인지를 나타내는 상대 값이다.

이를 Unix timestamp로 그대로 해석하면 1970-01-01(Unix epoch)에 가까운 날짜가 된다.  
실제로 이 오해로 `event_date=1970-01-01` 버그가 발생한 사례가 있다 (CLAUDE.md 알려진 함정 참고).

**재매핑 수식**

```
real_ts = real_start + (criteo_ts - criteo_base) / speed_multiplier
```

- `criteo_base`: 첫 번째 행의 상대 시각 (기준점으로 삼아 0으로 맞춤)
- `criteo_ts - criteo_base`: 첫 이벤트 기준 경과 시간
- `/ speed_multiplier`: 경과 시간을 speed_multiplier배 압축
- `+ real_start`: 압축된 경과 시간을 현재 시각에 더해 절대 시각으로 변환

이벤트 간 **상대 간격**은 유지하면서 **절대 시각**을 현재로 이동시킨다.  
결과적으로 `event_date`가 오늘 날짜가 되어 Bronze `raw_date` 파티션과 일치한다.

**conversion_delay_scale**

원본 conversion 지연은 `conv_ts - impression_ts`로 최대 수십일에 달한다.  
`speed_multiplier`만으로는 부족하므로 추가 축소 인수를 곱한다.

```
send_at = now + (original_delay × conversion_delay_scale / speed_multiplier)
# 예) 원본 3,600초 지연 → 3600 × 0.01 / 100 = 0.36초 후 발행
```

---

## 4. 알려진 한계

### 멱등성 미구현

재시작 시 더 광범위한 중복 문제가 발생한다.

**HuggingFace 데이터셋 처음부터 재읽기**  
`producer.py`는 재시작할 때마다 HuggingFace 데이터셋을 첫 번째 행부터 다시 읽어서 발행한다.  
impression, click, conversion 구분 없이 전체 데이터가 처음부터 중복 발행된다.

**`seen_conversion_ids` 초기화**  
메모리 내 set이라 재시작 시 초기화된다.  
conversion 중복 제거가 무력화되어 같은 `conversion_id`가 여러 번 발행된다.

**영향 범위**  
- Kafka 토픽: 중복 메시지 누적
- Bronze S3: append-only라 중복 행이 그대로 쌓임
- Silver: `dedup()` 단계에서 `ingest_ts` 기준 최신 1건만 유지하고 MERGE가 `(event_id, event_date)`로 중복 흡수 → 최종 집계 정합성은 유지됨
- Gold: Silver 집계 기반이라 영향 없음

Silver dedup이 흡수해주므로 분석 결과에 미치는 영향은 제한적이지만,  
Bronze에 불필요한 중복 데이터가 쌓인다는 점은 인지해야 한다.

### Kafka 튜닝 파라미터 미적용

현재 `KafkaProducer`는 기본값으로 동작한다.  
운영 환경에서는 `acks`, `linger_ms`, `batch_size`, `compression_type` 조정이 필요하다.

### 단일 워커 스레드

click/conversion을 처리하는 워커 스레드가 1개다.  
이벤트 속도가 빠른 경우 큐가 쌓일 수 있다.  
현재 `speed_multiplier=100` 기준에서는 문제없으나, 높은 배수에서는 워커 병렬화가 필요하다.
