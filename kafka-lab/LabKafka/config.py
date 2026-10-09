from pathlib import Path

BROKER = "localhost:9092"
TOPIC = "gutenberg-book-lines"
BOOK = Path(__file__).parent / "data/around_the_world_in_80_days.txt"
OUTPUT = Path(__file__).parent / "output/cleaned_book.txt"
