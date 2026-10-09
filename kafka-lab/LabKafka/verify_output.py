"""Check the complete Kafka output against the downloaded book."""
import json
from config import BOOK, OUTPUT
from text_cleaning import clean_tokens

with BOOK.open(encoding="utf-8-sig") as book:
    source_lines = list(book)
expected_lines = [" ".join(tokens) for line in source_lines
                  if (tokens := clean_tokens(line))]
actual = OUTPUT.read_text(encoding="utf-8").splitlines()
summary = json.loads(OUTPUT.with_name(OUTPUT.stem + "_summary.json").read_text())
assert summary["received_lines"] == len(source_lines), "Missing source lines"
assert actual == expected_lines, "Kafka output does not match the cleaned book"
assert summary["tokens"] == sum(len(line.split()) for line in actual)
assert clean_tokens("The HELLO, world! 123 -- and café.") == ["hello", "world", "café"]
assert clean_tokens(" \t 123 !!! ") == []
assert clean_tokens("Don't stop—reading!") == ["don", "stop", "reading"]
print(f"Verified: {len(source_lines)} source lines, {len(actual)} cleaned lines, "
      f"{summary['tokens']} tokens. Output matches the entire book.")
