import pickle  # noqa: S403
import random

import pytest
from dateparser import parse
from masking.faker.name import FakeNameProvider
from masking.utils.date_parsing import parse_date


@pytest.mark.parametrize(
    "value",
    [
        "1979-04-24",
        "2020-02-29",
        "2020-01-31 10:15",
        "2020-01-31 10:15:59",
        "2020-01-31T10:15:59",
        "2020-01-31T10:15:59.123456",
        "2020-02-30",
        "01.02.2020",
        "31/12/1999",
        "name_5",
        "Müller",
        "",
    ],
)
def test_parse_date_matches_dateparser(value: str) -> None:
    assert parse_date(value) == parse(value)


def test_parse_date_matches_dateparser_random_iso() -> None:
    rng = random.Random(0)  # noqa: S311
    for _ in range(500):
        value = f"{rng.randint(1800, 2100):04d}-{rng.randint(1, 12):02d}-{rng.randint(1, 28):02d}"
        assert parse_date(value) == parse(value)


def test_parse_date_invalid_type() -> None:
    with pytest.raises(TypeError):
        parse_date(None)


def test_fake_name_provider_lists() -> None:
    provider = FakeNameProvider()
    female = set(provider._female_firstname_list)
    male = set(provider._male_firstname_list)

    assert provider.get_male_first_name() in male
    assert provider.get_female_first_name() in female
    assert not female & male
    assert len({provider.get_last_name() for _ in range(5000)}) > 1001


def test_fake_name_provider_pickle() -> None:
    provider = FakeNameProvider()
    restored = pickle.loads(pickle.dumps(provider))  # noqa: S301

    assert "lastnames" not in restored.__dict__
    assert isinstance(restored(), str)
    assert len(restored.lastnames) == len(provider.lastnames)
