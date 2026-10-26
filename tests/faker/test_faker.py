import pytest
from faker.generator import Generator
from masking.faker.faker import FakeProvider


def test_is_abstract():
    with pytest.raises(TypeError):
        FakeProvider()


@pytest.mark.parametrize("locale", [None, "de_CH", "en_US"])
def test_get_generator(locale):
    generator = FakeProvider.get_generator(locale)
    assert isinstance(generator, Generator)
    assert isinstance(generator.last_name(), str)


def test_generators_are_independent():
    assert FakeProvider.get_generator() is not FakeProvider.get_generator()
