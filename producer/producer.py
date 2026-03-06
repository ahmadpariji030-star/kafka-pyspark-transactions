import json
import random
import time
from datetime import datetime, timedelta, timezone
from kafka import KafkaProducer


KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "transactions"

VALID_SOURCES = ["mobile", "web", "pos"]
UNKNOWN_SOURCES = ["appx", "tablet", "unknown"]


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=5
    )


def iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def generate_valid_event():
    now = datetime.now(timezone.utc)
    event = {
        "user_id": f"U{random.randint(10000, 99999)}",
        "amount": random.randint(1000, 500000),
        "timestamp": iso_z(now),
        "source": random.choice(VALID_SOURCES)
    }
    return event


def generate_invalid_events():
    now = datetime.now(timezone.utc)

    invalid_1 = {
        "user_id": f"U{random.randint(10000, 99999)}",
        "amount": -5000,  # invalid: negative amount
        "timestamp": iso_z(now),
        "source": "mobile"
    }

    invalid_2 = {
        "user_id": f"U{random.randint(10000, 99999)}",
        "amount": 20000000,  # invalid: too large
        "timestamp": iso_z(now),
        "source": "web"
    }

    invalid_3 = {
        "user_id": f"U{random.randint(10000, 99999)}",
        "amount": 120000,
        "timestamp": "2025-99-99T99:99:99Z",  # invalid timestamp
        "source": "pos"
    }

    invalid_4 = {
        "user_id": f"U{random.randint(10000, 99999)}",
        "amount": 90000,
        "timestamp": iso_z(now),
        "source": random.choice(UNKNOWN_SOURCES)  # invalid source
    }

    # duplicate event pair
    duplicate_base = {
        "user_id": "U77777",
        "amount": 150000,
        "timestamp": iso_z(now - timedelta(seconds=10)),
        "source": "mobile"
    }

    duplicate_copy = duplicate_base.copy()

    return [invalid_1, invalid_2, invalid_3, invalid_4, duplicate_base, duplicate_copy]


def generate_late_events():
    now = datetime.now(timezone.utc)

    late_1 = {
        "user_id": f"U{random.randint(10000, 99999)}",
        "amount": 50000,
        "timestamp": iso_z(now - timedelta(minutes=4)),  # late > 3 min
        "source": "web"
    }

    late_2 = {
        "user_id": f"U{random.randint(10000, 99999)}",
        "amount": 70000,
        "timestamp": iso_z(now - timedelta(minutes=5, seconds=10)),  # late > 3 min
        "source": "mobile"
    }

    late_3 = {
        "user_id": f"U{random.randint(10000, 99999)}",
        "amount": 80000,
        "timestamp": iso_z(now - timedelta(minutes=6)),  # late > 3 min
        "source": "pos"
    }

    return [late_1, late_2, late_3]


def main():
    producer = create_producer()

    invalid_events = generate_invalid_events()
    late_events = generate_late_events()

    # campur event valid + invalid + late
    event_pool = []

    # tambah banyak event valid
    for _ in range(15):
        event_pool.append(generate_valid_event())

    # tambah event invalid
    event_pool.extend(invalid_events)

    # tambah event late
    event_pool.extend(late_events)

    # acak urutan
    random.shuffle(event_pool)

    print(f"Producing {len(event_pool)} events to topic '{TOPIC_NAME}'...")

    for i, event in enumerate(event_pool, start=1):
        producer.send(TOPIC_NAME, value=event)
        producer.flush()
        print(f"[{i}] Sent: {json.dumps(event)}")

        # kirim setiap 1–2 detik
        time.sleep(random.uniform(1, 2))

    print("All events sent successfully.")


if __name__ == "__main__":
    main()