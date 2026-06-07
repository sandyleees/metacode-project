# 컨테이너로 producer 띄울 예정

from kafka import KafkaProducer # pip install kafka-python-ng
import json
import time

import random

from queue import PriorityQueue, Empty
import threading

from datasets import load_dataset

import os
BOOTSTRAP_SERVERS = os.environ.get("BOOTSTRAP_SERVERS", "localhost:9092")
# BOOTSTRAP_SERVERS          = "localhost:9092"
TOPIC_IMPRESSION           = "ad-impressions"
TOPIC_CLICK                = "ad-clicks"
TOPIC_CONVERSION           = "ad-conversions"
SPEED_MULTIPLIER           = 100
CONVERSION_DELAY_SCALE     = 0.01

from kafka.admin import KafkaAdminClient, NewTopic

def create_topics():
    # kafka 토픽 사전 생성
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    topics = [
        NewTopic(name=TOPIC_IMPRESSION, num_partitions=3, replication_factor=1),
        NewTopic(name=TOPIC_CLICK,      num_partitions=3, replication_factor=1),
        NewTopic(name=TOPIC_CONVERSION, num_partitions=3, replication_factor=1),
    ]
    # 이미 존재하는 토픽 제외하고 생성
    existing = admin.list_topics()
    new_topics = [t for t in topics if t.name not in existing]
    if new_topics:
        admin.create_topics(new_topics)
        print(f"토픽 생성: {[t.name for t in new_topics]}")
    else:
        print("토픽 이미 존재함, 건너뜀")
    admin.close()

# KafkaProducer 생성 전에 토픽 먼저 만들기
create_topics()

# 1. kafka producer 설정
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8") if k else None,
)
# 운영용 튜닝 파라미터 acks, linger_ms, batch_size, comporession_type 등 고려해볼것
# retries 도? 


# 2. 데이터 읽어옴
ds = load_dataset("criteo/criteo-attribution-dataset", split="train", streaming=True )
# 데이터를 청크 방식으로 읽어옴? - streaming = True

# ------------------------- 루프 안에서 쓰기 위해 필요한 내용 세팅 ----------------------------------

seen_conversion_ids = set() # 루프 밖에 중복 허용X 집합 선언 - 파이썬 기본 자료형 
# 동일한 conversion id인 conversion이 원본 데이터의 행에 중복 존재해서 그 중 하나만 흘려넣기 위해
# # conversion이 중복으로 들어오지 않기 위해 produce 할 때 중복 제거해서 kafka로 흘려보냄 

rng = random.Random(42)  # 42는 시드값 - 동일 시드면 항상 같은 난수 순서 보장
# click 메시지 지연시간 랜덤한 추가를 위해

# 루프 밖 - 큐 관련 초기화
delayed_queue = PriorityQueue()  # (send_at, seq, topic, message) 형태로 삽입 : send_at이 우선순위
# 우선순위 큐는 넣을 때 우선순위 지정하면 우선순위 순서대로 나옴 - 형식 : (우선순위, 데이터)
seq = 0            # PriorityQueue 동일 send_at 충돌 방지용 시퀀스
# send_at이 동일한 이벤트가 두 개 들어오면 두 번째 요소인 seq를 비교
    # # (send_at, seq, topic, message) 구조에서 send_at 동일 값일 때
    # (1000.0, ?, TOPIC_CLICK, click_message)
    # (1000.0, ?, TOPIC_CONVERSION, conv_message)

    # # topic(str)끼리 비교하면 오류 발생
    # # seq(int)로 순서 보장하면 오류 없음
    # (1000.0, 1, TOPIC_CLICK, click_message)      # seq=1
    # (1000.0, 2, TOPIC_CONVERSION, conv_message)  # seq=2
# 참고자료의 경우 클래스 구조로 별도 함수 안에서 외부 변수 seq를 변경하는 구조로 작성되어있음 ->  
# 함수 내부에서 외부 변수를 직접 변경할 수 없기 때문에 seq를 int가 아닌 list로 감싸 
# 리스트 내부 요소 변경하는 방식으로 구현 - 함수 밖 리스트 내부 요소는 변경 가능
# 지금은 seq가 별도의 함수 안에 있는게 아니라, 루프 안에 있으므로 그냥 int로 사용

# 루프 밖 - 스레드 관련 초기화
stop_event = threading.Event()   # 워커 스레드 종료 신호 # 스레드 간 신호 전달용 플래그
# 내부적으로 True/False 플래그 - 메인 루프가 끝났을 때 워커 스레드에게 "새 항목 없다"고 알리는 신호
# 내부: _flag = False

# 워커 함수 - 큐에서 꺼내서 send_at 시각에 발행
def delayed_event_worker():
            # stop_event.is_set() : _flag 값 반환 (True or False)
    while not stop_event.is_set() or not delayed_queue.empty(): # 워커 스레드 종료 조건
        #         ↑ False면 계속      ↑ 큐에 남은 것 있으면 계속
        # = 메인 루프가 아직 안끝났다면 or 큐에 남은 click / conversion 이벤트 있다면 계속
        try:
            send_at, _, topic, message = delayed_queue.get(timeout=1)
            # delayed_queue.get(): 큐가 비어있으면 항목이 들어올 때까지 무한 대기 
            # -> stop_event를 영원히 확인 못함 -> timeout=1으로 1초 간 기다리다 항목X면 Empty 예외처리
        except Empty:
            continue # 큐 비었으면 while 조건 재확인 -> stop_event.is_set()이 True면 종료

        while True:
            remaining = send_at - time.time() # 현재시각 기준 남은 시간 계산
            if remaining <= 0: # 시각이 됐으면
                break # while True 루프 탈출
            time.sleep(min(remaining, 5.0)) # 아직이면 대기

        producer.send(topic, key=message['uid'], value=message) # 시각 됐으면 발행(send)
         # send(): 즉시 전송하지 않고 내부 버퍼에 쌓아뒀다가 배치 전송

# 워커 스레드 시작 - 루프 전에 (스레드는 하나의 프로세스 안에서 동시에 실행되는 흐름)
worker = threading.Thread(
   target=delayed_event_worker, # 이 함수를 별도 스레드로 실행
   daemon=True,  # 메인 프로세스 종료 시 강제 종료
)
worker.start() # 스레드 시작

# impression 전송 시간 압축
prev_ts = None  # 처음엔 이전 행이 없으므로 None

# -------------------------------------------------------------------

# 3. 데이터에서 필요한 부분만 쪼개서 impression, click, conversion 토픽으로 보낼 메시지 작성 루프
for i, row in enumerate(ds):
  # event_id 인위적 생성 - 노출/클릭/전환 사이 체인 구조 형성
  # (실제 실무 상황에서는 노출/클릭/전환 당 각각의 이벤트 id 있을 것으로 추정되나 제공된 원본 데이터에 남아있지 않아서 인위적으로 생성해 통일)
  eid = f"evt_{i:08d}"  # i = 42이면 → "evt_00000042"
  uid = str(row['uid']) # str에만 key_serializer에서 .encode() 가능
  cmp = int(row['campaign'])
  ts = int(row['timestamp']) 
  
  # impression 간격 압축
  if prev_ts is not None and ts > prev_ts:
  #  ↑ 첫 번째 행은 건너뜀    ↑ 현재가 이전보다 클 때만
    delay = (ts - prev_ts) / SPEED_MULTIPLIER
    #        현재ts - 이전ts = 원본 간격 / 100 = 압축된 간격
    if 0 < delay < 5.0:  # 데이터 gap 무시
        time.sleep(delay)
  prev_ts = ts
  
  # impression 토픽으로 보낼 메시지 - 바로 전송 
  imp_message = {
    'eid' : eid,
    'uid' : uid,
    'timestamp' : ts ,
    'event_type' : "impression",
    'campaign' : cmp,
    'cost' : float(row['cost']) if row['cost'] else 0.0,
    }
  producer.send(TOPIC_IMPRESSION, key = uid, value = imp_message) # 여기에 토픽 하드코딩 별로인가?
  # send(): 즉시 전송하지 않고 내부 버퍼에 쌓아뒀다가 배치 전송

  # click 토픽으로 보낼 메시지 - 힙&우선순위큐 저장 후 별도 스레드 전송
  if int(row['click']) == 1 :
    click_delay = rng.randint(1, 30)  # 1~30 사이 정수 랜덤 반환
    click_message = {
        'eid' : eid,
        'uid' : uid ,
        'timestamp' : ts +  click_delay , # 지연시간 랜덤하게 1~30초
        'event_type' : "click",
        'campaign' : cmp,
    }
    # 힙으로 추가 -- 소비는 힙을 처리하는 워커 스레드에서 (delayed_event_worker)
    send_at = time.time() + (click_delay / SPEED_MULTIPLIER)
    seq += 1
    delayed_queue.put((send_at, seq, TOPIC_CLICK, click_message))

  
  # conversion 토픽으로 보낼 메시지 - 힙&우선순위큐 저장 후 별도 스레드 전송
  # - conversion_id 같으면 conversion timestamp도 같은것같으니 하나만 전송
  if int(row['conversion']) == 1 :
    conv_id = int(row['conversion_id'])
    conv_ts = int(row['conversion_timestamp'])

                # seen_conversion_ids : list로도 가능하지만 in 연산이 list는 O(n), set은 O(1)
    if conv_id in seen_conversion_ids:  # O(1) - 즉시 확인
        # 동일한 conversion_id인 conversion이 이미 읽혔다면
        continue # for 루프 다음 행으로 (해당 conversion 스킵해서 중복 conversion 입력 방지)
    seen_conversion_ids.add(conv_id)

    conv_message ={
        'eid' : eid ,
        'uid' : uid ,
        'timestamp' : conv_ts ,
        'event_type' : "conversion",
        'campaign' : cmp,
    }
    # 힙으로 추가 -- 소비는 힙을 처리하는 워커 스레드에서(delayed_event_worker)
    # 원본 지연 = 전환 시각(conv_ts) - 노출 시각(timestamp) = 최대 수십일
    original_delay = max(conv_ts - int(row['timestamp']), 0)
        # 압축: 원본 지연 * CONVERSION_DELAY_SCALE / SPEED_MULTIPLIER
        # ex) 원본 3600초 지연 → 3600 * 0.01 / 100 = 0.36초 후 발행
    send_at = time.time() + (original_delay * CONVERSION_DELAY_SCALE / SPEED_MULTIPLIER)
    seq += 1
    delayed_queue.put((send_at, seq, TOPIC_CONVERSION, conv_message))


# 4. 받은 메시지 각 토픽으로 전송

# 루프 종료 후  - 워커 스레드 종료 대기
producer.flush() # 메인 스레드가 보낸 impression 버퍼 비우기
stop_event.set() # 플래그를 True로 # _flag = True로 변경 # 워커에게 "메인 루프 끝났다" 신호
worker.join()  # 워커 스레드가 완전히 끝날 때까지 기다림 
producer.flush() # 워커 스레드가 보낸 click/conversion 버퍼 비우기

# 실시간 재현 위한 옵션 arguement.parse로 구현필요? 
# 실시간 재현을 위한 send 사이사이에 time.sleep 필요


