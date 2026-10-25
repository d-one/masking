from datetime import UTC, date, datetime
from typing import ClassVar

import pytest
from masking.faker.date import FakeDateProvider

REPEAT = 30
CURRENT_YEAR = str(datetime.now(tz=UTC).year)


class TestInit:
    def test_defaults(self):
        provider = FakeDateProvider()
        assert set() == provider._PRESERVED
        assert provider._current_year == CURRENT_YEAR

    @pytest.mark.parametrize(
        ("preserve", "expected"),
        [("year", {"year"}), (("day", "month"), {"day", "month"})],
    )
    def test_preserve(self, preserve, expected):
        assert expected == FakeDateProvider(preserve)._PRESERVED

    def test_preserve_is_per_instance(self):
        FakeDateProvider("year")
        assert set() == FakeDateProvider()._PRESERVED
        assert set() == FakeDateProvider._PRESERVED

    @pytest.mark.parametrize("preserve", ["hour", ("year", "week")])
    def test_invalid_preserve_raises(self, preserve):
        with pytest.raises(AssertionError, match="Preserve values"):
            FakeDateProvider(preserve)


class TestPreservedRanges:
    def test_nothing_preserved(self):
        assert FakeDateProvider().get_preserved_ranges(date(2000, 4, 15)) == {
            "day_start": "01",
            "day_end": "31",
            "month_start": "01",
            "month_end": "12",
            "year_start": "1800",
            "year_end": CURRENT_YEAR,
        }

    def test_preserved_parts_are_fixed(self):
        ranges = FakeDateProvider(("year", "day")).get_preserved_ranges(
            date(1990, 4, 15)
        )
        assert (ranges["year_start"], ranges["year_end"]) == ("1990", "1990")
        assert (ranges["day_start"], ranges["day_end"]) == ("15", "15")
        assert (ranges["month_start"], ranges["month_end"]) == ("01", "12")

    @pytest.mark.parametrize("month", [4, 6, 9, 11])
    def test_months_with_30_days(self, month):
        ranges = FakeDateProvider("month").get_preserved_ranges(date(2000, month, 1))
        assert ranges["day_end"] == "30"

    @pytest.mark.parametrize(
        ("year", "day_end"), [(2000, "29"), (2004, "29"), (1900, "28"), (2001, "28")]
    )
    def test_february(self, year, day_end):
        provider = FakeDateProvider(("year", "month"))
        assert provider.get_preserved_ranges(date(year, 2, 1))["day_end"] == day_end

    def test_preserved_day_is_not_capped(self):
        ranges = FakeDateProvider(("month", "day")).get_preserved_ranges(
            date(2000, 4, 30)
        )
        assert ranges["day_end"] == "30"


class TestDateFromRanges:
    RANGES: ClassVar[dict[str, str]] = {
        "day_start": "1",
        "day_end": "10",
        "month_start": "3",
        "month_end": "3",
        "year_start": "2000",
        "year_end": "2000",
    }

    def test_within_half_open_range(self):
        provider = FakeDateProvider()
        dates = {provider.get_date_from_ranges(self.RANGES) for _ in range(200)}
        assert dates <= {date(2000, 3, d) for d in range(1, 10)}
        assert len(dates) > 1

    def test_empty_range_returns_start(self):
        ranges = {**self.RANGES, "day_end": "1"}
        assert FakeDateProvider().get_date_from_ranges(ranges) == date(2000, 3, 1)


class TestCall:
    @pytest.mark.parametrize("real", ["2000-02-29", datetime(1999, 12, 31)])  # noqa: DTZ001
    def test_every_part_changes(self, real):
        provider = FakeDateProvider()
        real_date = date.fromisoformat(real) if isinstance(real, str) else real.date()
        for _ in range(REPEAT):
            fake = date.fromisoformat(provider(real))
            assert fake.year != real_date.year
            assert fake.month != real_date.month
            assert fake.day != real_date.day
            assert date(1800, 1, 1) <= fake <= date(int(CURRENT_YEAR), 12, 31)

    def test_preserved_year_and_month_are_kept(self):
        provider = FakeDateProvider(("year", "month"))
        for _ in range(REPEAT):
            fake = date.fromisoformat(provider("1990-04-15"))
            assert (fake.year, fake.month) == (1990, 4)
            assert fake.day != 15

    @pytest.mark.xfail(
        strict=True,
        reason="Bug: dates are drawn uniformly between start and end, so a preserved day is not kept unless the month is preserved too.",
    )
    def test_preserved_day_is_kept(self):
        provider = FakeDateProvider(("year", "day"))
        for _ in range(REPEAT):
            fake = date.fromisoformat(provider("1990-04-15"))
            assert (fake.year, fake.day) == (1990, 15)
            assert fake.month != 4

    def test_raises_when_no_different_date_is_found(self):
        provider = FakeDateProvider()
        provider.get_date_from_ranges = lambda _: date(2000, 1, 1)
        with pytest.raises(ValueError, match="too many iterations"):
            provider("2000-01-01")
