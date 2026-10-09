"""Create the lab topic and wait for the Kafka broker."""
import argparse
import time
from confluent_kafka import KafkaException, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
from config import BROKER, TOPIC


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--broker", default=BROKER)
    parser.add_argument("--topic", default=TOPIC)
    args = parser.parse_args()
    admin = AdminClient({"bootstrap.servers": args.broker})
    deadline = time.monotonic() + 90
    while True:
        try:
            admin.list_topics(timeout=5)
            break
        except KafkaException:
            if time.monotonic() >= deadline:
                raise RuntimeError("Kafka is unavailable after 90 seconds")
            time.sleep(2)
    future = admin.create_topics(
        [NewTopic(args.topic, num_partitions=1, replication_factor=1)],
        request_timeout=15,
    )[args.topic]
    try:
        future.result()
        print(f"Created topic: {args.topic}")
    except KafkaException as exc:
        if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
            raise
        print(f"Topic already exists: {args.topic}")
    metadata = admin.list_topics(args.topic, timeout=10)
    topic = metadata.topics[args.topic]
    if topic.error:
        raise KafkaException(topic.error)
    if len(topic.partitions) != 1:
        raise RuntimeError("This lab requires a topic with exactly one partition")


if __name__ == "__main__":
    main()
