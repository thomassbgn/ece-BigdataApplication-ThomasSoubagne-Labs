"""Basic NLP cleaning inspired by the course word-count notebook."""
import re
import unicodedata

STOP_WORDS = frozenset("""
a an the and or but if then than as at by for from in into of on to with
is am are was were be been being have has had do does did it its this that
these those i me my we us our you your he him his she her they them their
not no nor so too very can could will would shall should may might must
s t d ll re ve m
""".split())


def clean_tokens(text):
    # Normalize Unicode, lowercase, extract words (no numbers/punctuation),
    # and remove a small, explicit list of common English stop words.
    normalized = unicodedata.normalize("NFKC", text).casefold()
    words = re.findall(r"[^\W\d_]+", normalized, flags=re.UNICODE)
    return [word for word in words if word not in STOP_WORDS]
