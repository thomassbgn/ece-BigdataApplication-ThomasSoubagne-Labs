from text_cleaning import clean_tokens
assert clean_tokens("The HELLO, world! 123 -- and café.") == ["hello", "world", "café"]
assert clean_tokens(" \t 123 !!! ") == []
assert clean_tokens("Don't stop—reading!") == ["don", "stop", "reading"]
print("Cleaning checks passed")
