# %%
"""Create the Kafka topic used by the wikistreams demo."""
import argparse
from confluent_kafka import KafkaError
from confluent_kafka.admin import AdminClient, NewTopic

# %%
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--broker", default="localhost:9092")
parser.add_argument("--topic", default="wikistreams")
parser.add_argument("--delete", action="store_true",
                    help="Delete the topic instead of creating it")
args = parser.parse_args()

admin_client = AdminClient({"bootstrap.servers": args.broker})

# %%
if args.delete:
    admin_client.delete_topics([args.topic])[args.topic].result()
    print(f"Deleted topic {args.topic}")
else:
    future = admin_client.create_topics(
        [NewTopic(args.topic, num_partitions=1, replication_factor=1)]
    )[args.topic]
    try:
        # Wait for the broker answer instead of exiting before creation.
        future.result()
        print(f"Created topic {args.topic}")
    except Exception as error:
        if error.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
            raise
        print(f"Topic {args.topic} already exists")

# %%
for t in admin_client.list_topics(timeout=10).topics:
    print(t)
