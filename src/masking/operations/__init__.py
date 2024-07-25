from .operation_date import FakeDate
from .operation_hash import HashSHA256, Operation
from .operation_plz import FakePLZ
from .operation_presidio import HashPresidio

MASKING_OPERATIONS: dict[str, Operation] = {
    "hash": HashSHA256,
    "presidio": HashPresidio,
    "fake_date": FakeDate,
    "fake_plz": FakePLZ,
}
