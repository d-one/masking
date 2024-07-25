from typing import TYPE_CHECKING

from .faker import FakeProvider

if TYPE_CHECKING:
    from faker.generator import Generator


class FakeNameProvider(FakeProvider):
    """Fake name provider."""

    def __init__(self, locale: str = "de_CH") -> None:
        """Initialize the FakeNameProvider.

        Args:
        ----
            locale (str, optional): Country initials such ash 'de_CH'.

        """
        self.generator: Generator = self.get_generator(locale)

    def get_last_name(self) -> str:
        """Get a fake last name.

        Returns
        -------
            str: Fake last name

        """
        return self.generator.last_name()

    def get_first_name(self) -> str:
        """Get a fake first name.

        Returns
        -------
            str: Fake first name

        """
        return self.generator.first_name()

    def get_male_first_name(self) -> str:
        """Generate male first name.

        Returns
        -------
        str:

        """
        return self.generator.first_name_male()

    def get_female_first_name(self) -> str:
        """Generate female first name.

        Returns
        -------
        str:

        """
        return self.generator.first_name_female()

    def get_nonbinary_first_name(self) -> str:
        """Generate nonbinary first name.

        Returns
        -------
        str:

        """
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
