# 컨테이너로 producer 띄울 예정 -- 계속 들어오는 streaming이면 항상 띄워놔야하나?
# 멱등성 문제(재시작 시 처음부터 재발행 - 중복 등 문제) 
# 예외처리 필요 - 아직은 구현에 집중
# logging?

'''
실버 레이어 형태인 criteo 데이터셋을 스레드 + 힙&우선순위큐로 인위적으로 쪼개 
impression/click/conversion 3개의 실시간 데이터 들어오는 구조를 흉내내고
kafka 브로커에 impression/click/conversion 3토픽 저장
'''

import argparse
import json
import os
import random
import threading
import time
from queue import Empty, PriorityQueue

from datetime import datetime

from datasets import load_dataset
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic


def create_topics(bootstrap_servers, topic_impression, topic_click, topic_conversion):
    # kafka 토픽 사전 생성
    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    topics = [
        NewTopic(name=topic_impression, num_partitions=3, replication_factor=1),
        NewTopic(name=topic_click,      num_partitions=3, replication_factor=1),
        NewTopic(name=topic_conversion, num_partitions=3, replication_factor=1),
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


def parse_args():
    # CLI 인자 파싱 — 전역 상수 대신 실행 시점에 값 주입
    # 미지정 시 환경변수 → 하드코딩 기본값 순으로 폴백
    parser = argparse.ArgumentParser(description="Criteo 데이터셋을 읽어 Kafka로 ad 이벤트를 스트리밍하는 Producer")

    parser.add_argument(
        "--bootstrap-servers",
        default=os.environ.get("BOOTSTRAP_SERVERS", "localhost:9092"),
        help="Kafka bootstrap server 주소 (기본값: BOOTSTRAP_SERVERS 환경변수 또는 localhost:9092)",
    )
    parser.add_argument(
        "--topic-impression",
        default="ad-impressions",
        help="노출(impression) 이벤트를 발행할 Kafka 토픽명 (기본값: ad-impressions)",
    )
    parser.add_argument(
        "--topic-click",
        default="ad-clicks",
        help="클릭(click) 이벤트를 발행할 Kafka 토픽명 (기본값: ad-clicks)",
    )
    parser.add_argument(
        "--topic-conversion",
        default="ad-conversions",
        help="전환(conversion) 이벤트를 발행할 Kafka 토픽명 (기본값: ad-conversions)",
    )
    parser.add_argument(
        "--speed-multiplier",
        type=int,
        default=100,
        help="전체 이벤트 타임라인 재생 속도 배수 — 클수록 빠르게 재생 (기본값: 100)",
    )
    parser.add_argument(
        "--conversion-delay-scale",
        type=float,
        default=0.01,
        help="conversion 원본 지연(최대 수십일)을 추가 축소하는 스케일 인수 (기본값: 0.01)",
    )

    return parser.parse_args()


def main():
    args = parse_args()

    # KafkaProducer 생성 전에 토픽 먼저 만들기
    create_topics(
        args.bootstrap_servers,
        args.topic_impression,
        args.topic_click,
        args.topic_conversion,
    )

    # 1. kafka producer 설정
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )
    # 운영용 튜닝 파라미터 acks, linger_ms, batch_size, compression_type 등 고려해볼것
    # retries 도?

    # 2. 데이터 읽어옴
    ds = load_dataset("criteo/criteo-attribution-dataset", split="train", streaming=True)
    # 데이터를 청크 방식으로 읽어옴 - streaming=True

    # ------------------------- 루프 안에서 쓰기 위해 필요한 내용 세팅 ----------------------------------
    # 뭔가 이 부분이 지저분하고 기능별로 함수화? 모듈화? 잘 안 되어있는 느낌이라 리펙토링 가능해보임

    seen_conversion_ids = set()  # 루프 밖에 중복 허용X 집합 선언 - 파이썬 기본 자료형
    # 동일한 conversion id인 conversion이 원본 데이터의 행에 중복 존재해서 그 중 하나만 흘려넣기 위해
    # conversion이 중복으로 들어오지 않기 위해 produce 할 때 중복 제거해서 kafka로 흘려보냄

    rng = random.Random(42)  # 42는 시드값 - 동일 시드면 항상 같은 난수 순서 보장
    # click 메시지 지연시간 랜덤한 추가를 위해

    # 루프 밖 - 큐 관련 초기화
    delayed_queue = PriorityQueue()  # (send_at, seq, topic, message) 형태로 삽입 : send_at이 우선순위
    # 우선순위 큐는 넣을 때 우선순위 지정하면 우선순위 순서대로 나옴 - 형식 : (우선순위, 데이터)
    seq = 0  # PriorityQueue 동일 send_at 충돌 방지용 시퀀스
    # send_at이 동일한 이벤트가 두 개 들어오면 두 번째 요소인 seq를 비교
    # seq가 루프 안에 있으므로 int로 사용
    # (참고: 별도 함수 내부에서 외부 int 변경 불가 → list로 감싸는 패턴도 있음)

    # 루프 밖 - 스레드 관련 초기화
    stop_event = threading.Event()  # 워커 스레드 종료 신호 - 스레드 간 신호 전달용 플래그
    # 메인 루프가 끝났을 때 워커 스레드에게 "새 항목 없다"고 알리는 신호
    # 내부: _flag = False

    # 워커 함수 - 큐에서 꺼내서 send_at 시각에 발행
    def delayed_event_worker():
                # stop_event.is_set() : _flag 값 반환 (True or False)
        while not stop_event.is_set() or not delayed_queue.empty():  # 워커 스레드 종료 조건
            #         ↑ False면 계속      ↑ 큐에 남은 것 있으면 계속
            # = 메인 루프가 아직 안끝났다면 or 큐에 남은 click / conversion 이벤트 있다면 계속
            try:
                send_at, _, topic, message = delayed_queue.get(timeout=1)
                # delayed_queue.get(): 큐가 비어있으면 항목이 들어올 때까지 무한 대기
                # -> stop_event를 영원히 확인 못함 -> timeout=1으로 1초 간 기다리다 항목X면 Empty 예외처리
            except Empty:
                continue  # 큐 비었으면 while 조건 재확인 -> stop_event.is_set()이 True면 종료

            while True:
                remaining = send_at - time.time()  # 현재시각 기준 남은 시간 계산
                if remaining <= 0:  # 시각이 됐으면
                    break  # while True 루프 탈출
                time.sleep(min(remaining, 5.0))  # 아직이면 대기

            producer.send(topic, key=message['uid'], value=message)  # 시각 됐으면 발행(send)
            # send(): 즉시 전송하지 않고 내부 버퍼에 쌓아뒀다가 배치 전송

    # 워커 스레드 시작 - 루프 전에 (스레드는 하나의 프로세스 안에서 동시에 실행되는 흐름)
    worker = threading.Thread(
        target=delayed_event_worker,  # 이 함수를 별도 스레드로 실행
        daemon=True,  # 메인 프로세스 종료 시 강제 종료
    )
    worker.start()  # 스레드 시작

    # Criteo 상대시간 → 현재 실시간 재매핑
    # real_ts = real_start + (criteo_ts - criteo_base) / speed_multiplier
    # criteo_base: 첫 이벤트의 Criteo timestamp (기준점)
    # 이렇게 하면 이벤트 간 상대 간격은 그대로 유지되면서 절대 시각이 현재로 이동
    # raw_date / event_date 모두 2026년대 날짜가 되어 실제 파이프라인 동작과 동일해짐
    real_start   = time.time()
    criteo_base  = None  # 첫 행에서 설정
    prev_ts      = None

    # -------------------------------------------------------------------

    # 3. 데이터에서 필요한 부분만 쪼개서 impression, click, conversion 토픽으로 보낼 메시지 작성 루프
    for i, row in enumerate(ds):
        # event_id 인위적 생성 - 노출/클릭/전환 사이 체인 구조 형성
        # (실제 실무 상황에서는 노출/클릭/전환 당 각각의 이벤트 id 있을 것으로 추정되나 제공된 원본 데이터에 남아있지 않아서 인위적으로 생성해 통일)
        eid = f"evt_{i:08d}"  # i = 42이면 → "evt_00000042"
        uid = str(row['uid'])  # str에만 key_serializer에서 .encode() 가능
        cmp = int(row['campaign'])
        ts  = int(row['timestamp'])

        if criteo_base is None:
            criteo_base = ts

        # Criteo 상대시간 → 실시간 변환
        real_ts = int(real_start + (ts - criteo_base) / args.speed_multiplier)

        # impression 간격 압축 (sleep 타이밍은 Criteo 상대 간격 기준 유지)
        if prev_ts is not None and ts > prev_ts:
        #  ↑ 첫 번째 행은 건너뜀    ↑ 현재가 이전보다 클 때만
            delay = (ts - prev_ts) / args.speed_multiplier
            #        현재ts - 이전ts = 원본 간격 / speed_multiplier = 압축된 간격
            if 0 < delay < 5.0:  # 데이터 gap 무시
                time.sleep(delay)
        prev_ts = ts

        # impression 토픽으로 보낼 메시지 - 바로 전송
        imp_message = {
            'eid':        eid,
            'uid':        uid,
            'timestamp':  real_ts,
            'event_time': datetime.fromtimestamp(real_ts).isoformat(),
            'event_type': "impression",
            'campaign':   cmp,
            'cost':       float(row['cost']) if row['cost'] else 0.0,
        }
        producer.send(args.topic_impression, key=uid, value=imp_message)
        # send(): 즉시 전송하지 않고 내부 버퍼에 쌓아뒀다가 배치 전송

        # click 토픽으로 보낼 메시지 - 힙&우선순위큐 저장 후 별도 스레드 전송
        if int(row['click']) == 1:
            click_delay = rng.randint(1, 30)  # 1~30 사이 정수 랜덤 반환 (Criteo 초 단위)
            real_click_ts = int(real_start + (ts + click_delay - criteo_base) / args.speed_multiplier)
            click_message = {
                'eid':        eid,
                'uid':        uid,
                'timestamp':  real_click_ts,
                'event_time': datetime.fromtimestamp(real_click_ts).isoformat(),
                'event_type': "click",
                'campaign':   cmp,
            }
            # 힙으로 추가 -- 소비는 힙을 처리하는 워커 스레드에서 (delayed_event_worker)
            send_at = time.time() + (click_delay / args.speed_multiplier)
            seq += 1
            delayed_queue.put((send_at, seq, args.topic_click, click_message))

        # conversion 토픽으로 보낼 메시지 - 힙&우선순위큐 저장 후 별도 스레드 전송
        # - conversion_id 같으면 conversion timestamp도 같은것같으니 하나만 전송
        if int(row['conversion']) == 1:
            conv_id = int(row['conversion_id'])
            conv_ts = int(row['conversion_timestamp'])

                        # seen_conversion_ids : list로도 가능하지만 in 연산이 list는 O(n), set은 O(1)
            if conv_id in seen_conversion_ids:  # O(1) - 즉시 확인
                # 동일한 conversion_id인 conversion이 이미 읽혔다면
                continue  # for 루프 다음 행으로 (해당 conversion 스킵해서 중복 conversion 입력 방지)
            seen_conversion_ids.add(conv_id)

            real_conv_ts = int(real_start + (conv_ts - criteo_base) / args.speed_multiplier)
            conv_message = {
                'eid':        eid,
                'uid':        uid,
                'timestamp':  real_conv_ts,
                'event_time': datetime.fromtimestamp(real_conv_ts).isoformat(),
                'event_type': "conversion",
                'campaign':   cmp,
            }
            # 힙으로 추가 -- 소비는 힙을 처리하는 워커 스레드에서(delayed_event_worker)
            # 원본 지연 = 전환 시각(conv_ts) - 노출 시각(timestamp) = 최대 수십일
            original_delay = max(conv_ts - ts, 0)
                # 압축: 원본 지연 * conversion_delay_scale / speed_multiplier
                # ex) 원본 3600초 지연 → 3600 * 0.01 / 100 = 0.36초 후 발행
            send_at = time.time() + (
                original_delay * args.conversion_delay_scale / args.speed_multiplier
            )
            seq += 1
            delayed_queue.put((send_at, seq, args.topic_conversion, conv_message))

    # 4. 받은 메시지 각 토픽으로 전송

    # 루프 종료 후 - 워커 스레드 종료 대기
    producer.flush()   # 메인 스레드가 보낸 impression 버퍼 비우기
    stop_event.set()   # 플래그를 True로 / 워커에게 "메인 루프 끝났다" 신호
    worker.join()      # 워커 스레드가 완전히 끝날 때까지 기다림
    producer.flush()   # 워커 스레드가 보낸 click/conversion 버퍼 비우기


if __name__ == "__main__":
    main()
