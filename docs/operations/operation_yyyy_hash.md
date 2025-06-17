# Operation `yyyy_hash`

Hashes datetime values by appending to the front the clear value of the year.


### Additional Parameters

- `secret` (`str`, optional):
  The optional secret to use when hashing;

- `hash_function` (`Callable`, default=`hashlib.sha256`):
  The hashing function to use. Must follow the hashlib interface.


### Behavior
- Converts the input to a string if it’s not already;
- Parses the input using dateparser.parse() to extract the year;
- Hashes the original full date using the hashing function.
- Returns a combined string of year and hash in the form `<year>_hash` where `year` is the 4-digit year, and `hash` is the hash of the entire date.
