import hashlib
from itertools import product

from masking.utils.hash import hash_string

values = ["line", "line2"]
hash_functions = [hashlib.sha256, hashlib.sha512, hashlib.md5]
secrets = ["secret", "_secret"]
encodings = ["utf-8", "latin-1", "utf-16"]

params = product(values, hash_functions, secrets, encodings)

for param in params:
    inputs = {
        "data": param[0],
        "secret": param[2],
        "method": param[1],
        "text_encoding": param[3],
    }

    hashed_value = hash_string(**inputs)

    method_name = inputs["method"].__name__.replace("openssl_", "hashlib.")
    string_to_print = f"('{inputs['data']}', {method_name}, '{inputs['secret']}', '{inputs['text_encoding']}', '{hashed_value}'),"
    print(string_to_print)  # noqa: T201
