from .operation import Operation


def load_fake_date() -> Operation:
    from .operation_date import FakeDate

    return FakeDate


def load_fake_plz() -> Operation:
    from .operation_plz import FakePLZ

    return FakePLZ


def load_hash_presidio() -> Operation:
    from .operation_presidio import HashPresidio

    return HashPresidio


def load_hash_sha256() -> Operation:
    from .operation_hash import HashSHA256

    return HashSHA256


MASKING_OPERATIONS: dict[str, Operation] = {
    "hash": load_hash_sha256(),
    "presidio": load_hash_presidio(),
    "fake_date": load_fake_date(),
    "fake_plz": load_fake_plz(),
}
