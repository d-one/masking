import pytest
from masking.base_operations.operation_fake_plz import FakePLZBase
from masking.faker.plz import FakePLZProvider

REPEAT = 20


class FakePLZ(FakePLZBase):
    def _mask_data(self, data):
        return data


@pytest.fixture(scope="module")
def known_plz() -> set[str]:
    return {str(p) for p in FakePLZ(col_name="p").faker.files["PLZ"]}


def masked(op: FakePLZ, line: str | int) -> list[str]:
    return [op._mask_line(line) for _ in range(REPEAT)]


def test_provider():
    assert isinstance(FakePLZ(col_name="p").faker, FakePLZProvider)


@pytest.mark.parametrize("line", ["8001", 8001, "3436"])
def test_masked_value_is_a_different_known_plz(line, known_plz):
    for value in masked(FakePLZ(col_name="p"), line):
        assert value in known_plz
        assert value != str(line)


@pytest.mark.parametrize(
    ("preserve", "prefix_length"),
    [("district", 1), (("district", "area"), 2), (("district", "area", "route"), 3)],
)
def test_preserved_prefix(preserve, prefix_length, known_plz):
    for value in masked(FakePLZ(col_name="p", preserve=preserve), "8001"):
        assert value[:prefix_length] == "8001"[:prefix_length]
        assert value != "8001"
        assert value in known_plz


def test_preserving_everything_cannot_mask():
    op = FakePLZ(col_name="p", preserve=("district", "area", "route", "postcode"))
    op.MAX_RETRY_MASK_LINE = 3
    with pytest.raises(ValueError, match="Unable to mask"):
        op._mask_line("8001")


def test_invalid_preserve_raises():
    with pytest.raises(ValueError, match="Preserve values"):
        FakePLZ(col_name="p", preserve="canton")


@pytest.mark.parametrize("line", ["123", "12345"])
def test_invalid_plz_length_raises(line):
    with pytest.raises(ValueError, match="4 digits"):
        FakePLZ(col_name="p")._mask_line(line)
