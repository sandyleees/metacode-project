"""Criteo Attribution 데이터셋을 읽어 Kafka 3토픽(impression/click/conversion)으로 스트리밍하는 Producer.

원본 데이터 구조와 3토픽 분리 설계, 시간 재매핑 방식, 알려진 한계는
ingestion/INGESTION_GUIDE.md 참고.
"""

import argparse
import json
import logging
import os
import random
import threading
import time
from datetime import datetime, timezone
from queue import Empty, PriorityQueue

from datasets import load_dataset
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

logger = logging.getLogger(__name__)


def create_topics(bootstrap_servers, topic_impression, topic_click, topic_conversion):
    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    try:
        topics = [
            NewTopic(name=topic_impression, num_partitions=3, replication_factor=1),
            NewTopic(name=topic_click,      num_partitions=3, replication_factor=1),
            NewTopic(name=topic_conversion, num_partitions=3, replication_factor=1),
        ]
        existing = admin.list_topics()
        new_topics = [t for t in topics if t.name not in existing]
        if new_topics:
            admin.create_topics(new_topics)
            logger.info("토픽 생성: %s", [t.name for t in new_topics])
        else:
            logger.info("토픽 이미 존재함, 건너뜀")
    finally:
        admin.close()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Criteo 데이터셋을 읽어 Kafka로 ad 이벤트를 스트리밍하는 Producer"
    )
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


def build_impression_message(eid: str, uid: str, campaign: int, cost, real_ts: int) -> dict:
    return {
        "eid":        eid,
        "uid":        uid,
        "timestamp":  real_ts,
        "event_time": datetime.fromtimestamp(real_ts, tz=timezone.utc).isoformat(),
        "event_type": "impression",
        "campaign":   campaign,
        "cost":       float(cost) if cost else 0.0,
    }


def build_click_message(eid: str, uid: str, campaign: int, real_click_ts: int) -> dict:
    return {
        "eid":        eid,
        "uid":        uid,
        "timestamp":  real_click_ts,
        "event_time": datetime.fromtimestamp(real_click_ts, tz=timezone.utc).isoformat(),
        "event_type": "click",
        "campaign":   campaign,
    }


def build_conversion_message(eid: str, uid: str, campaign: int, real_conv_ts: int) -> dict:
    return {
        "eid":        eid,
        "uid":        uid,
        "timestamp":  real_conv_ts,
        "event_time": datetime.fromtimestamp(real_conv_ts, tz=timezone.utc).isoformat(),
        "event_type": "conversion",
        "campaign":   campaign,
    }


def delayed_event_worker(
    queue: PriorityQueue,
    stop_event: threading.Event,
    producer: KafkaProducer,
) -> None:
    """큐에서 (send_at, seq, topic, message)를 꺼내 send_at 시각에 Kafka로 발행한다.

    메인 루프가 끝난 뒤(stop_event.set())에도 큐가 빌 때까지 계속 실행한다.
    timeout=1 로 get()하는 이유: 블로킹 get()은 stop_event를 확인할 수 없어
    큐가 빈 상태에서 종료 신호를 영원히 놓침.
    """
    while not stop_event.is_set() or not queue.empty():
        try:
            send_at, _, topic, message = queue.get(timeout=1)
        except Empty:
            continue

        while True:
            remaining = send_at - time.time()
            if remaining <= 0:
                break
            time.sleep(min(remaining, 5.0))

        producer.send(topic, key=message["uid"], value=message).add_errback(
            lambda e, t=topic: logger.error("Kafka send 실패 topic=%s: %s", t, e)
        )


def main():
    args = parse_args()

    create_topics(
        args.bootstrap_servers,
        args.topic_impression,
        args.topic_click,
        args.topic_conversion,
    )

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        # TODO: acks, linger_ms, batch_size, compression_type 운영 튜닝 파라미터 추가
    )

    dataset = load_dataset("criteo/criteo-attribution-dataset", split="train", streaming=True)

    seen_conversion_ids: set[int] = set()
    rng = random.Random(42)

    # PriorityQueue 동일 send_at 충돌 방지용 단조 증가 시퀀스 (메인 스레드 전용)
    seq = 0
    delayed_queue: PriorityQueue = PriorityQueue()
    stop_event = threading.Event()

    worker = threading.Thread(
        target=delayed_event_worker,
        args=(delayed_queue, stop_event, producer),
        daemon=True,
    )
    worker.start()

    # real_ts = real_start + (criteo_ts - criteo_base) / speed_multiplier
    # 이벤트 간 상대 간격을 유지하면서 절대 시각을 현재로 이동
    real_start = time.time()
    criteo_base = None
    prev_ts = None

    for i, row in enumerate(dataset):
        eid = f"evt_{i:08d}"
        uid = str(row["uid"])
        campaign = int(row["campaign"])
        ts = int(row["timestamp"])

        if criteo_base is None:
            criteo_base = ts

        real_ts = int(real_start + (ts - criteo_base) / args.speed_multiplier)

        # impression 간격을 Criteo 원본 상대 간격 기준으로 압축 재생
        if prev_ts is not None and ts > prev_ts:
            delay = (ts - prev_ts) / args.speed_multiplier
            if 0 < delay < 5.0:  # 데이터 gap(장기간 공백) 무시
                time.sleep(delay)
        prev_ts = ts

        imp_message = build_impression_message(eid, uid, campaign, row["cost"], real_ts)
        producer.send(args.topic_impression, key=uid, value=imp_message).add_errback(
            lambda e: logger.error("Kafka send 실패 topic=%s: %s", args.topic_impression, e)
        )

        if int(row["click"]) == 1:
            click_delay = rng.randint(1, 30)
            real_click_ts = int(real_start + (ts + click_delay - criteo_base) / args.speed_multiplier)
            click_message = build_click_message(eid, uid, campaign, real_click_ts)
            send_at = time.time() + (click_delay / args.speed_multiplier)
            seq += 1
            delayed_queue.put((send_at, seq, args.topic_click, click_message))

        if int(row["conversion"]) == 1:
            conv_id = int(row["conversion_id"])
            conv_ts = int(row["conversion_timestamp"])

            # 원본 데이터에 동일 conversion_id가 여러 행에 걸쳐 중복 존재 — 첫 번째만 발행
            if conv_id in seen_conversion_ids:
                continue
            seen_conversion_ids.add(conv_id)

            real_conv_ts = int(real_start + (conv_ts - criteo_base) / args.speed_multiplier)
            conv_message = build_conversion_message(eid, uid, campaign, real_conv_ts)
            # 원본 지연(수십일)을 conversion_delay_scale로 추가 압축
            original_delay = max(conv_ts - ts, 0)
            send_at = time.time() + (
                original_delay * args.conversion_delay_scale / args.speed_multiplier
            )
            seq += 1
            delayed_queue.put((send_at, seq, args.topic_conversion, conv_message))

        if i % 10_000 == 0:
            logger.info("행 %d 처리 완료 (eid=%s)", i, eid)

    producer.flush()
    stop_event.set()
    worker.join()
    producer.flush()
    logger.info("모든 이벤트 발행 완료")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    main()
