import random
from typing import ClassVar

from masking.base_operations.operation import Operation
from masking.faker.name import FakeNameProvider


class FakeNameBase(Operation):
    """Mask a column with fake name data."""

    MAX_RETRY_MASK_LINE = 100
    _SEEN_INPUTS: ClassVar[set[str]] = set()

    def __init__(
        self,
        col_name: str,
        locale: str = "de_CH",
        gender: str | None = None,
        name_type: str = "full",
        **kwargs: dict,
    ) -> None:
        """Initialize the HashOperation class.

        Args:
        ----
            col_name (str): column name to be hashed
            locale (str, optional): Country initials such as 'de_CH'.
            gender: the gender of the generation. values must be in {'male','female','m','f',None}.
            name_type: the type of name to be generated. values must be in {'full','first','last'}.
            **kwargs (dict): keyword arguments

        """
        super().__init__(col_name=col_name, **kwargs)
        self.faker = FakeNameProvider(locale=locale)

        self.gender = gender
        valid_genders = {"male", "female", "m", "f", "nonbinary", "nb", None}
        if self.gender not in valid_genders:
            msg = f"Gender variable should be in the following valid values: {valid_genders}. Default is {None}"
            raise ValueError(msg)
        if self.gender is not None:
            self.gender = self.gender.lower().strip()

        self.name_type = name_type.lower().strip()
        valid_name_types = {"full", "first", "last"}
        if self.name_type not in valid_name_types:
            msg = f"Name type variable should be in the following valid values: {valid_name_types}. Default is 'full'."
            raise ValueError(msg)

    @property
    def _needs_unique_values(self) -> bool:
        """Return if the operation needs to produce unique masked values."""
        return True

    def _mask_line_generate(self) -> str:
        """Mask a single line with gender and name type.

        Returns
        -------
            str: masked line

        """
        # If name_type is last, generate a last name
        if self.name_type == "last":
            return self.faker.get_last_name()

        # If name_type is full, generate a full name
        if self.name_type == "full":
            return self.faker(self.gender)

        # If name_type is first, generate a first name
        if self.name_type == "first":
            if not self.gender:
                return self.faker.get_first_name()

            if self.gender[0] == "m":
                return self.faker.get_male_first_name()

            if self.gender[0] == "f":
                return self.faker.get_female_first_name()

            if self.gender[0] == "n":
                return self.faker.get_nonbinary_first_name()

        msg = "Cannot generate new fake name."
        raise ValueError(msg)

    def _mask_line_seen_or_generate(self, line: str) -> str:
        """Mask a single line if it has been seen before or generate a new one.

        Args:
        ----
            line (str): input line

        Returns:
        -------
            str: masked line

        """
        # Add the line to the seen inputs
        self._SEEN_INPUTS.add(line)

        # Randomly decide whether to generate a new name or use a seen one
        to_generate = random.choice([True, False])  # noqa: S311

        if to_generate or not self._SEEN_INPUTS:
            return self._mask_line_generate()

        masked = random.choice(list(self._SEEN_INPUTS))  # noqa: S311
        self._SEEN_INPUTS.remove(masked)
        return masked

    def _mask_line(self, line: str, **kwargs: dict) -> str:  # noqa: ARG002
        """Mask a single line.

        Args:
        ----
            line (str): input line
            **kwargs (dict): additional arguments for the masking operation

        Returns:
        -------
            str: masked line

        """
        masked = self._mask_line_seen_or_generate(line)

        counter = 0
        while masked == line and counter < self.MAX_RETRY_MASK_LINE:
            masked = self._mask_line_seen_or_generate(line)
            counter += 1

        if masked == line:
            msg = f"Unable to mask the line {line} after {self.MAX_RETRY} attempts."
            raise ValueError(msg)

        return masked
