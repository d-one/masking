from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

from .faker import FakeProvider

if TYPE_CHECKING:
    from faker.generator import Generator


class FakeNameProvider(FakeProvider):
    """Fake name provider."""

    def __init__(self, locale: str = "de_CH", from_file: bool | None = None) -> None:  # noqa: FBT001
        """Initialize the FakeNameProvider.

        Args:
        ----
            locale (str, optional): Country initials such ash 'de_CH'.
            from_file (bool, optional): If True, generate names from loaded files.

        """
        self.generator: Generator = self.get_generator(locale)

        self.firstnames = self._load_firstnames()
        self.lastnames = self._load_lastnames()

        self._from_file = from_file or True

    @property
    def from_file(self) -> bool:
        """Generate the fake names from loaded files."""
        return self._from_file

    @from_file.setter
    def from_file(self, value: bool) -> None:
        """Set whether to generate names from loaded files."""
        self._from_file = value
        if value:
            self.firstnames = self._load_firstnames()
            self.lastnames = self._load_lastnames()

    @staticmethod
    def _load_firstnames() -> pd.DataFrame:
        """Load first names from a predefined source.

        Data are loaded from the reference file of switzerland names:
        https://www.bfs.admin.ch/bfs/en/home/statistics/population/births-deaths/names-switzerland.assetdetail.32208757.html
        """
        file_path = Path(__file__).parent / "sources" / "firstnames2024.feather"
        return pd.read_feather(file_path)

    @staticmethod
    def _load_lastnames() -> pd.DataFrame:
        """Load last names from a predefined source.

        Data are loaded from the reference file of switzerland names:
        https://www.bfs.admin.ch/bfs/en/home/statistics/population/births-deaths/names-switzerland.assetdetail.32208773.html
        """
        file_path = Path(__file__).parent / "sources" / "lastnames2024.feather"
        return pd.read_feather(file_path)

    def get_last_name(self) -> str:
        """Get a fake last name.

        Returns
        -------
            str: Fake last name

        """
        if self.from_file:
            # Generate a random last name from the loaded DataFrame
            return self.lastnames.sample(
                n=1, random_state=self.generator.random.randint(0, 1000)
            ).iloc[0]["LASTNAME"]

        return self.generator.last_name()

    def get_first_name(self) -> str:
        """Get a fake first name.

        Returns
        -------
            str: Fake first name

        """
        if self.from_file:
            # Generate a random first name from the loaded DataFrame
            return self.firstnames.sample(
                n=1, random_state=self.generator.random.randint(0, 1000)
            ).iloc[0]["first_name"]

        return self.generator.first_name()

    def get_male_first_name(self) -> str:
        """Generate male first name.

        Returns
        -------
        str:

        """
        if self.from_file:
            # Generate a random first name from the loaded DataFrame
            male_firstnames = self.firstnames[
                (self.firstnames["male_count"].notna())
                & (self.firstnames["female_count"].isna())
            ]
            return male_firstnames.sample(
                n=1, random_state=self.generator.random.randint(0, len(male_firstnames))
            ).iloc[0]["first_name"]

        return self.generator.first_name_male()

    def get_female_first_name(self) -> str:
        """Generate female first name.

        Returns
        -------
        str:

        """
        if self.from_file:
            # Generate a random first name from the loaded DataFrame
            female_firstnames = self.firstnames[
                (self.firstnames["male_count"].isna())
                & (self.firstnames["female_count"].notna())
            ]
            return female_firstnames.sample(
                n=1,
                random_state=self.generator.random.randint(0, len(female_firstnames)),
            ).iloc[0]["first_name"]

        return self.generator.first_name_female()

    def get_nonbinary_first_name(self) -> str:
        """Generate nonbinary first name.

        Returns
        -------
        str:

        """
        if self.from_file:
            nb_firstnames = self.firstnames[
                (self.firstnames["female_count"].notna())
                & (self.firstnames["male_count"].notna())
            ]
            return nb_firstnames.sample(
                n=1, random_state=self.generator.random.randint(0, len(nb_firstnames))
            ).iloc[0]["first_name"]

        return self.generator.first_name_nonbinary()

    def __call__(self, gender: str | None = None) -> str:
        """Generate a full name.

        Returns
        -------
        str: Full name (first name + last name)

        """
        if not gender:
            return self.get_first_name() + " " + self.get_last_name()

        female_ = {"weiblich", "female", "f", "w"}
        if gender.strip().lower() in female_:
            return self.get_female_first_name() + " " + self.get_last_name()

        male_ = {"männlich", "male", "m"}
        if gender.strip().lower() in male_:
            return self.get_male_first_name() + " " + self.get_last_name()

        nonbinary_ = {"nonbinary", "nb"}
        if gender.strip().lower() in nonbinary_:
            return self.get_nonbinary_first_name() + " " + self.get_last_name()

        msg = f"""Invalid gender: gender should be in the following lists:
        - {female_}
        - {male_}
        - {nonbinary_}
        """
        raise ValueError(msg)
