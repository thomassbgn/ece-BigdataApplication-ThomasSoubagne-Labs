import argparse
import json
import socket
import sys
import time

from confluent_kafka import Producer
from pywikibot.comms.eventstreams import EventStreams


class DurationReached(Exception):
    pass


def main():
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    parser = argparse.ArgumentParser()
    parser.add_argument('--bootstrap-servers', default='localhost:9094')
    parser.add_argument('--topic', default='wikistreams')
    parser.add_argument('--server', default='fr.wikipedia.org')
    parser.add_argument('--title', help='Exact page title')
    parser.add_argument('--bot', choices=['all', 'yes', 'no'], default='all')
    parser.add_argument('--duration', type=float, default=10, help='Minutes')
    args = parser.parse_args()
    if args.duration <= 0:
        parser.error('--duration must be positive')
    errors = []
    sent = 0

    def delivered(error, message):
        if error:
            errors.append(str(error))

    producer = Producer({
        'bootstrap.servers': args.bootstrap_servers,
        'client.id': socket.gethostname(),
        'compression.type': 'lz4',
    })
    stream = EventStreams(streams=['recentchange'], timeout=10)
    deadline = time.monotonic() + args.duration * 60

    def within_duration(change):
        if time.monotonic() >= deadline:
            raise DurationReached
        return True

    stream.register_filter(within_duration)
    stream.register_filter(server_name=args.server, type='edit')
    if args.title:
        stream.register_filter(title=args.title)
    if args.bot != 'all':
        stream.register_filter(bot=args.bot == 'yes')
    print(f'Following edits on {args.server}', flush=True)
    try:
        for change in stream:
            if time.monotonic() >= deadline:
                break
            value = json.dumps(change, ensure_ascii=False).encode('utf-8')
            while True:
                try:
                    producer.produce(args.topic, key=change['title'], value=value, callback=delivered)
                    break
                except BufferError:
                    producer.poll(1)
            producer.poll(0)
            sent += 1
            print(f"{sent}: {change['title']} | bot={change.get('bot')} | {change.get('length')}", flush=True)
    except DurationReached:
        pass
    except KeyboardInterrupt:
        print('Stopping producer', flush=True)
    finally:
        if hasattr(stream, 'source'):
            stream.source.close()
        remaining = producer.flush(30)
        print(f'Sent {sent} events; {remaining} messages left', flush=True)
    if remaining or errors:
        raise RuntimeError(f'Delivery failed: {errors}; pending={remaining}')


if __name__ == '__main__':
    main()
