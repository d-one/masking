import re
from datetime import datetime
from functools import lru_cache

from dateparser import parse

# ISO dates without timezone, e.g. 2020-01-31, 2020-01-31 10:00, 2020-01-31T10:00:00.123
_ISO_DATE = re.compile(r"\d{4}-\d{2}-\d{2}([T ]\d{2}:\d{2}(:\d{2}(\.\d{1,6})?)?)?")


@lru_cache(maxsize=2**16)
def _parse_with_dateparser(value: str) -> datetime | None:
    return parse(value)


def parse_date(value: str) -> datetime | None:
    """Parse a date string, equivalent to dateparser.parse but faster.

    ISO formatted strings are parsed with datetime.fromisoformat, everything else
    falls back to dateparser.parse, whose results are cached.

    Args:
    ----
        value (str): string to parse

    Returns:
    -------
        datetime | None: parsed date, None if the string is not a date

    """
    if isinstance(value, str) and _ISO_DATE.fullmatch(value):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            pass
    return _parse_with_dateparser(value)
