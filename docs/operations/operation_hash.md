# Operation `hash`

Hashes values using a cryptographic hash function (default: SHA256). Optionally, a secret key can be provided to introduce keyed hashing (HMAC-style), enhancing the security of the hash.


### Additional Parameters

- `secret` (`str`, optional):
  The optional secret to use when hashing;

- `hash_function` (`Callable`, default=`hashlib.sha256`):
  The hashing function to use. Must follow the hashlib interface.


### Behavior
- Converts the input to string if necessary.
- Applies a hash function (SHA256 or custom).
- If secret is provided, uses it to produce a deterministic keyed hash (via hash_string).
