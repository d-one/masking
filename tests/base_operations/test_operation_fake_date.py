from datetime import date, datetime

import pytest
from masking.base_operations.operation_fake_date import FakeDateBase
from masking.faker.date import FakeDateProvider

DATES = ["2000-01-15", "1999-12-31", "1985-02-28"]
REPEAT = 20


class FakeDate(FakeDateBase):
    def _mask_data(self, data):
        return data


def masked_dates(op: FakeDate, line: str) -> list[date]:
    return [date.fromisoformat(op._mask_line(line)) for _ in range(REPEAT)]


def test_provider():
    assert isinstance(FakeDate(col_name="d").faker, FakeDateProvider)


@pytest.mark.parametrize("line", DATES)
def test_without_preserve_every_part_changes(line):
    real = date.fromisoformat(line)
    for masked in masked_dates(FakeDate(col_name="d"), line):
        assert masked.year != real.year
        assert masked.month != real.month
        assert masked.day != real.day


@pytest.mark.parametrize("preserve", ["year", ("year",)])
@pytest.mark.parametrize("line", DATES)
def test_preserve_year(line, preserve):
    real = date.fromisoformat(line)
    for masked in masked_dates(FakeDate(col_name="d", preserve=preserve), line):
        assert masked.year == real.year
        assert masked.month != real.month
        assert masked.day != real.day


@pytest.mark.parametrize("line", DATES)
def test_preserve_year_and_month(line):
    real = date.fromisoformat(line)
    op = FakeDate(col_name="d", preserve=("year", "month"))
    for masked in masked_dates(op, line):
        assert (masked.year, masked.month) == (real.year, real.month)
        assert masked.day != real.day


def test_datetime_input():
    real = datetime(2000, 5, 17)  # noqa: DTZ001
    masked = date.fromisoformat(
        FakeDate(col_name="d", preserve="year")._mask_line(real)
    )
    assert masked.year == 2000
    assert masked != real.date()


def test_invalid_preserve_raises():
    with pytest.raises(AssertionError, match="Preserve values"):
        FakeDate(col_name="d", preserve="hour")


def test_masked_values_are_unique():
    op = FakeDate(col_name="d", preserve=("year", "month"))
    lines = [f"2000-01-{d:02d}" for d in range(1, 16)]
    masked = [op._check_mask_line(line) for line in lines]
    assert len(set(masked)) == len(lines)
