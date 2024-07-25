from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from faker.factory import Factory
from faker.generator import Generator

if TYPE_CHECKING:
    from collections.abc import Callable


class FakeProvider(ABC):
    @classmethod
    def get_generator(cls, locale: str | None = None) -> Generator:
        """Construct generator.

        Args:
        ----
            locale (str): Country initials such as 'de_CH'

        Returns:
        -------
            Generator:

        """
        faker: Callable = Factory.create

        if locale is None:
            fake: Generator = faker()
            return fake

        fake: Generator = faker(locale=locale)
        return fake

    @abstractmethod
    def __call__(self) -> str:
        pass
