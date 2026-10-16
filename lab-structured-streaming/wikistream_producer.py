# %% Dependencies
"""Send filtered Wikimedia recent changes to Kafka.

Examples:
  python wikistream_producer.py                       # demo: fr.wikipedia edits
  python wikistream_producer.py --server-name en.wikipedia.org de.wikipedia.org
  python wikistream_producer.py --type edit new --humans-only --minutes 5
  python wikistream_producer.py --title "Paris" "Lyon"
"""
import argparse
import json
import os
import socket
from datetime import datetime, timedelta

# pywikibot needs no account to read EventStreams: skip user-config.py.
os.environ.setdefault("PYWIKIBOT_NO_USER_CONFIG", "2")

from confluent_kafka import Producer
from pywikibot.comms.eventstreams import EventStreams

# %% Command line options (producer-level filters)
parser = argparse.ArgumentParser(
    description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
parser.add_argument("--broker", default="localhost:9092")
parser.add_argument("--topic", default="wikistreams")
parser.add_argument("--minutes", type=float, default=10,
                    help="Streaming duration in minutes")
parser.add_argument("--since",
                    help="Replay events from this date (e.g. 20261016), "
                         "Wikimedia keeps about 7 days")
parser.add_argument("--server-name", nargs="+", default=["fr.wikipedia.org"],
                    help="Wikis to follow, or 'all'")
parser.add_argument("--type", nargs="+", default=["edit"],
                    help="Change types: edit, new, log, categorize")
parser.add_argument("--title", nargs="+",
                    help="Only follow these page titles")
parser.add_argument("--namespace", nargs="+", type=int,
                    help="Namespaces to follow, e.g. 0 for articles")
bots = parser.add_mutually_exclusive_group()
bots.add_argument("--bots-only", action="store_true")
bots.add_argument("--humans-only", action="store_true")
parser.add_argument("--quiet", action="store_true",
                    help="Print a counter instead of every event")
args = parser.parse_args()

# %% Helper Functions
# Serializer function to change message from python dict to json
value_serializer = lambda val: json.dumps(val).encode("utf-8")

# %% Producer Instantiation
conf = {"bootstrap.servers": args.broker,
        "client.id": socket.gethostname(),
        "compression.type": "lz4"}
producer = Producer(conf)

# %% Create wikistreams query with filters
stream = EventStreams(streams=["recentchange"], since=args.since)
# Each keyword filter keeps an event when its value is one of the given ones.
filters = {"type": tuple(args.type)}
if args.server_name != ["all"]:
    filters["server_name"] = tuple(args.server_name)
if args.title:
    filters["title"] = tuple(args.title)
if args.namespace:
    filters["namespace"] = tuple(args.namespace)
stream.register_filter(**filters)
if args.bots_only or args.humans_only:
    stream.register_filter(bot=args.bots_only)
print(f"Filters: {filters}, bots_only={args.bots_only}, "
      f"humans_only={args.humans_only}")

# %% Streaming Query
stop_time = datetime.now() + timedelta(minutes=args.minutes)
errors = []


def delivered(error, message):
    if error:
        errors.append(error)


sent = 0
try:
    while datetime.now() < stop_time:
        change = next(stream)
        # The wiki name as key keeps the events of one wiki together.
        producer.produce(topic=args.topic, key=change["server_name"],
                         value=value_serializer(change),
                         on_delivery=delivered)
        producer.poll(0)
        sent += 1
        if args.quiet:
            print(f"\rSent {sent} events", end="", flush=True)
        else:
            print('{type} on "{title}" ({server_name}) by "{user}" '
                  "at {meta[dt]}".format(**change))
except KeyboardInterrupt:
    print("\nInterrupted")

# Wait for the pending messages instead of a fixed sleep.
remaining = producer.flush(60)
print(f"\nSent {sent} events to {args.topic}; {remaining} not delivered; "
      f"{len(errors)} errors")
