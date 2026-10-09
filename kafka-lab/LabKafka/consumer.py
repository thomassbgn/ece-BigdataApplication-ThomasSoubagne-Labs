"""Read the book topic, clean each line, and write a UTF-8 text file."""
import argparse
from collections import Counter
import json
from pathlib import Path
import time
import uuid
from confluent_kafka import Consumer, KafkaException
from config import BROKER, TOPIC, OUTPUT
from text_cleaning import clean_tokens


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--broker", default=BROKER)
    parser.add_argument("--topic", default=TOPIC)
    parser.add_argument("--run-id", help="Only process this producer execution")
    parser.add_argument("--output", type=Path, default=OUTPUT)
    parser.add_argument("--timeout", type=float, default=120)
    args = parser.parse_args()
    consumer = Consumer({"bootstrap.servers": args.broker,
                         "group.id": f"book-cleaner-{uuid.uuid4()}",
                         "auto.offset.reset": "earliest",
                         "enable.auto.commit": False})
    consumer.subscribe([args.topic])
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_suffix(args.output.suffix + ".tmp")
    lines = 0
    cleaned_lines = 0
    frequencies = Counter()
    selected_run = args.run_id
    deadline = time.monotonic() + args.timeout
    try:
        with temporary.open("w", encoding="utf-8", newline="\n") as output:
            while True:
                if time.monotonic() >= deadline:
                    raise TimeoutError("No completion message before timeout")
                message = consumer.poll(1)
                if message is None:
                    continue
                if message.error():
                    raise KafkaException(message.error())
                headers = dict(message.headers() or [])
                run_id = headers.get("run-id", b"").decode("utf-8")
                if selected_run is None:
                    selected_run = run_id
                if run_id != selected_run:
                    continue
                deadline = time.monotonic() + args.timeout
                kind = headers.get("kind", b"").decode("utf-8")
                if kind == "end":
                    expected = json.loads(message.value())["lines"]
                    if lines != expected:
                        raise RuntimeError(f"Expected {expected} lines, got {lines}")
                    break
                if kind != "line":
                    raise ValueError(f"Unknown message kind: {kind}")
                lines += 1
                if message.key() != str(lines).encode():
                    raise RuntimeError("Missing, duplicate or out-of-order line")
                tokens = clean_tokens(message.value().decode("utf-8"))
                if tokens:
                    output.write(" ".join(tokens) + "\n")
                    frequencies.update(tokens)
                    cleaned_lines += 1
        temporary.replace(args.output)
        counts_file = args.output.with_name(args.output.stem + "_word_counts.csv")
        with counts_file.open("w", encoding="utf-8", newline="\n") as counts:
            counts.write("word,count\n")
            for word, count in sorted(frequencies.items(), key=lambda item: (-item[1], item[0])):
                counts.write(f"{word},{count}\n")
        summary = {"topic": args.topic, "run_id": selected_run,
                   "received_lines": lines, "cleaned_lines": cleaned_lines,
                   "tokens": sum(frequencies.values()),
                   "unique_words": len(frequencies), "output": str(args.output)}
        args.output.with_name(args.output.stem + "_summary.json").write_text(
            json.dumps(summary, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(summary, indent=2))
    finally:
        consumer.close()
        temporary.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
