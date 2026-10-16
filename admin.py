import argparse
from confluent_kafka import KafkaException, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--bootstrap-servers', default='localhost:9094')
    parser.add_argument('--topic', default='wikistreams')
    args = parser.parse_args()
    admin = AdminClient({'bootstrap.servers': args.bootstrap_servers})
    futures = admin.create_topics([NewTopic(args.topic, num_partitions=1, replication_factor=1)])
    for topic, future in futures.items():
        try:
            future.result()
            print(f'Created {topic}')
        except KafkaException as error:
            if error.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise
            print(f'{topic} already exists')
    print('Topics:', ', '.join(sorted(admin.list_topics(timeout=10).topics)))


if __name__ == '__main__':
    main()
