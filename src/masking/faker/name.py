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
        self._build_name_lists()

        self._from_file = True if from_file is None else from_file

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
            self._build_name_lists()

    def _build_name_lists(self) -> None:
        """Pre-compute the arrays of names to sample from.

        Pandas arrays are kept instead of python lists: they are much faster to
        (un)pickle, which happens for every Spark task.
        """
        male = self.firstnames["male_count"].notna()
        female = self.firstnames["female_count"].notna()
        names = self.firstnames["first_name"]

        self._lastname_list = self.lastnames["LASTNAME"].array
        self._firstname_list = names.array
        self._male_firstname_list = names[male & ~female].array
        self._female_firstname_list = names[~male & female].array
        self._nonbinary_firstname_list = names[male & female].array

    def _choice(self, names: pd.api.extensions.ExtensionArray) -> str:
        return names[self.generator.random.randrange(len(names))]

    def __getstate__(self) -> dict:
        # The source DataFrames are not needed once the name lists are built:
        # dropping them keeps the pickled payload (e.g. Spark tasks) small.
        state = self.__dict__.copy()
        state.pop("firstnames", None)
        state.pop("lastnames", None)
        return state

    def __getattr__(self, name: str) -> pd.DataFrame:
        if name == "firstnames":
            self.firstnames = self._load_firstnames()
            return self.firstnames
        if name == "lastnames":
            self.lastnames = self._load_lastnames()
            return self.lastnames
        raise AttributeError(name)

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
            return self._choice(self._lastname_list)

        return self.generator.last_name()

    def get_first_name(self) -> str:
        """Get a fake first name.

        Returns
        -------
            str: Fake first name

        """
        if self.from_file:
            # Generate a random first name from the loaded DataFrame
            return self._choice(self._firstname_list)

        return self.generator.first_name()

    def get_male_first_name(self) -> str:
        """Generate male first name.

        Returns
        -------
        str:

        """
        if self.from_file:
            # Generate a random first name from the loaded DataFrame
            return self._choice(self._male_firstname_list)

        return self.generator.first_name_male()

    def get_female_first_name(self) -> str:
        """Generate female first name.

        Returns
        -------
        str:

        """
        if self.from_file:
            # Generate a random first name from the loaded DataFrame
            return self._choice(self._female_firstname_list)

        return self.generator.first_name_female()

    def get_nonbinary_first_name(self) -> str:
        """Generate nonbinary first name.

        Returns
        -------
        str:

        """
        if self.from_file:
            return self._choice(self._nonbinary_firstname_list)

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
