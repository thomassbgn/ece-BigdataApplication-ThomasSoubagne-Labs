"""Send each UTF-8 book line to Kafka, then a completion message."""
import argparse
import json
import uuid
from confluent_kafka import Producer
from config import BROKER, TOPIC, BOOK


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--broker", default=BROKER)
    parser.add_argument("--topic", default=TOPIC)
    parser.add_argument("--book", type=str, default=str(BOOK))
    parser.add_argument("--run-id", default=str(uuid.uuid4()))
    args = parser.parse_args()
    producer = Producer({"bootstrap.servers": args.broker,
                         "enable.idempotence": True,
                         "delivery.timeout.ms": 60000})
    errors = []

    def delivered(error, message):
        if error:
            errors.append(str(error))

    def send(value, kind, key):
        while True:
            try:
                producer.produce(
                    args.topic, partition=0, key=str(key).encode(), value=value,
                    headers={"run-id": args.run_id, "kind": kind},
                    on_delivery=delivered,
                )
                producer.poll(0)
                return
            except BufferError:
                producer.poll(0.1)
                if errors:
                    raise RuntimeError(errors[0])

    count = 0
    with open(args.book, encoding="utf-8-sig") as book:
        for count, line in enumerate(book, start=1):
            send(line.rstrip("\r\n").encode("utf-8"), "line", count)
    remaining = producer.flush(65)
    if remaining or errors:
        raise RuntimeError(f"Delivery failed: {remaining} pending; {errors[:3]}")
    send(json.dumps({"lines": count}).encode(), "end", "end")
    if producer.flush(65) or errors:
        raise RuntimeError(f"Completion message failed: {errors[:3]}")
    print(f"Sent {count} lines; run-id={args.run_id}")


if __name__ == "__main__":
    main()
