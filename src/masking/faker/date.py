from datetime import UTC, datetime
from typing import TYPE_CHECKING, ClassVar

from dateparser import parse
from faker.generator import Generator

from .faker import FakeProvider

if TYPE_CHECKING:
    from faker.generator import Generator


class FakeDateProvider(FakeProvider):
    """Generates fake dates."""

    _DATE_MAX_ITERATIONS: int = 10
    _PRESERVED: ClassVar[set[str]] = set()
    _ADMISSIBLE: ClassVar[set[str]] = {"day", "month", "year"}

    def __init__(self, preserve: str | tuple[str] | None = None) -> None:
        self.generator: Generator = self.get_generator()

        self._PRESERVED = set()

        if preserve is not None:
            if isinstance(preserve, str):
                preserve = (preserve,)

            # Check if the preserve values are admissible
            msg = f"""Preserve values must be {", ".join([f"'{s}'" for s in self._ADMISSIBLE])}."""
            assert all(s in self._ADMISSIBLE for s in preserve), msg  # noqa: S101

            self._PRESERVED = set(preserve)

    @staticmethod
    def read_date_from_isoformat(date_str: str) -> datetime:
        """Read date from an ISO format string.

        Args:
        ----
            date_str (str): "YYYY-MM-DD" format is expected

        Returns:
        -------
            datetime:

        """
        return parse(date_str)

    def get_preserved_ranges(self, real_date: datetime) -> dict[str, str]:
        """Get the preserved ranges for the real date.

        Args:
        ----
            real_date (datetime): Real date to compare with the generated date.

        Returns:
        -------
            dict: Dictionary with the preserved ranges

        """
        dates_range = {
            "day_start": "01",
            "day_end": "31",
            "month_start": "01",
            "month_end": "12",
            "year_start": "1800",
            "year_end": str(datetime.now(tz=UTC).year),
        }

        # Update date_range with preserved values
        for k, v in {k: getattr(real_date, k) for k in self._PRESERVED}.items():
            dates_range[f"{k}_start"] = str(v)
            dates_range[f"{k}_end"] = str(v)

        # Fix the day end, based on the month
        if "day" not in self._PRESERVED:
            if dates_range["month_end"].zfill(2) in {"11", "04", "06", "09"}:
                dates_range["day_end"] = "30"
                return dates_range

            if dates_range["month_end"].zfill(2) == "02":
                # Determine if the year is a leap year
                if (
                    int(dates_range["year_end"]) % 4 == 0
                    and int(dates_range["year_end"]) % 100 != 0
                ) or (
                    int(dates_range["year_end"]) % 100 == 0
                    and int(dates_range["year_end"]) % 400 == 0
                ):
                    dates_range["day_end"] = "29"
                    return dates_range

                dates_range["day_end"] = "28"
                return dates_range

        return dates_range

    def __call__(self, real_date: datetime | str) -> str:
        """Generate a uniqe date that is not equal in month and day.

        The function is recursively called if the generated date is equal to the real date.

        Args:
        ----
            real_date (datetime): Real date to compare with the generated date.

        Raises:
        ------
            Exception: If the function is called more than Constants.DATE_MAX_ITERATIONS times, the exception is raised.

        Returns:
        -------
            datetime: A unique date that is not equal in month and day.

        """
        if not isinstance(real_date, datetime):
            real_date = self.read_date_from_isoformat(real_date)

        dates_range = self.get_preserved_ranges(real_date)
        changed = self._ADMISSIBLE - self._PRESERVED

        for _ in range(self._DATE_MAX_ITERATIONS):
            dt: datetime = self.get_date_from_ranges(dates_range)

            # Find conditions to check
            if all(getattr(real_date, k) != getattr(dt, k) for k in changed):
                return str(dt)

        msg = "Could not generate a unique date: too many iterations."
        raise ValueError(msg)

    def get_date_from_ranges(self, dates_ranges: dict[str, str]) -> datetime:
        """Generate a date from the given year.

        Args:
        ----
            dates_ranges (dict): Dictionary with the date ranges in the form
                        {"year_start": "YYYY", "year_end": "YYYY",
                         "month_start": "MM", "month_end": "MM",
                         "day_start": "DD", "day_end": "DD"}

        Returns:
        -------
            datetime: date generated from the given year.

        """
        start_date = f"{dates_ranges['year_start'].zfill(4)}-{dates_ranges['month_start'].zfill(2)}-{dates_ranges['day_start'].zfill(2)}"
        end_date = f"{dates_ranges['year_end'].zfill(4)}-{dates_ranges['month_end'].zfill(2)}-{dates_ranges['day_end'].zfill(2)}"

        return self.generator.date_time_between_dates(
            datetime.fromisoformat(start_date), datetime.fromisoformat(end_date)
        ).date()
