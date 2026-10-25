import pickle  # noqa: S403

import pytest
from masking.faker.name import FakeNameProvider

REPEAT = 50


@pytest.fixture(scope="module")
def provider() -> FakeNameProvider:
    return FakeNameProvider()


class TestNameLists:
    def test_lists_are_not_empty(self, provider):
        for names in (
            provider._lastname_list,
            provider._firstname_list,
            provider._male_firstname_list,
            provider._female_firstname_list,
            provider._nonbinary_firstname_list,
        ):
            assert len(names) > 0

    def test_gendered_lists_are_disjoint(self, provider):
        male = set(provider._male_firstname_list)
        female = set(provider._female_firstname_list)
        nonbinary = set(provider._nonbinary_firstname_list)
        assert not male & female
        assert not male & nonbinary
        assert not female & nonbinary
        assert male | female | nonbinary <= set(provider._firstname_list)


class TestGetters:
    @pytest.mark.parametrize(
        ("getter", "names"),
        [
            ("get_last_name", "_lastname_list"),
            ("get_first_name", "_firstname_list"),
            ("get_male_first_name", "_male_firstname_list"),
            ("get_female_first_name", "_female_firstname_list"),
            ("get_nonbinary_first_name", "_nonbinary_firstname_list"),
        ],
    )
    def test_values_come_from_the_lists(self, provider, getter, names):
        allowed = set(getattr(provider, names))
        values = {getattr(provider, getter)() for _ in range(REPEAT)}
        assert values <= allowed
        assert len(values) > 1


class TestCall:
    @pytest.mark.parametrize(
        ("gender", "names"),
        [
            (None, "_firstname_list"),
            ("", "_firstname_list"),
            ("female", "_female_firstname_list"),
            ("weiblich", "_female_firstname_list"),
            (" F ", "_female_firstname_list"),
            ("w", "_female_firstname_list"),
            ("male", "_male_firstname_list"),
            ("Männlich", "_male_firstname_list"),
            ("m", "_male_firstname_list"),
            ("nonbinary", "_nonbinary_firstname_list"),
            ("NB", "_nonbinary_firstname_list"),
        ],
    )
    def test_full_name(self, monkeypatch, gender, names):
        provider = FakeNameProvider()
        monkeypatch.setattr(provider, "get_last_name", lambda: "LAST")
        allowed = set(getattr(provider, names))
        for _ in range(10):
            first, last = provider(gender).rsplit(" ", 1)
            assert first in allowed
            assert last == "LAST"

    def test_invalid_gender_raises(self, provider):
        with pytest.raises(ValueError, match="Invalid gender"):
            provider("x")


class TestFromFile:
    def test_from_file_by_default(self, provider):
        assert provider.from_file is True

    def test_generator_used_when_disabled(self, monkeypatch):
        provider = FakeNameProvider()
        provider.from_file = False
        monkeypatch.setattr(provider.generator, "last_name", lambda: "GEN_LAST")
        monkeypatch.setattr(provider.generator, "first_name", lambda: "GEN_FIRST")
        assert provider.get_last_name() == "GEN_LAST"
        assert provider.get_first_name() == "GEN_FIRST"

    def test_enabling_reloads_lists(self):
        provider = FakeNameProvider()
        provider._firstname_list = []
        provider.from_file = True
        assert len(provider._firstname_list) > 0

    @pytest.mark.xfail(
        strict=True, reason="Bug: `from_file or True` ignores from_file=False."
    )
    def test_from_file_false_in_constructor(self):
        assert FakeNameProvider(from_file=False).from_file is False


class TestPickle:
    def test_source_dataframes_are_not_pickled(self, provider):
        state = provider.__getstate__()
        assert "firstnames" not in state
        assert "lastnames" not in state
        assert "_firstname_list" in state

    def test_round_trip(self, provider):
        clone = pickle.loads(pickle.dumps(provider))  # noqa: S301
        assert clone.get_last_name() in set(provider._lastname_list)
        assert "firstnames" not in clone.__dict__

    def test_source_dataframes_are_reloaded_lazily(self, provider):
        clone = pickle.loads(pickle.dumps(provider))  # noqa: S301
        assert len(clone.firstnames) == len(provider.firstnames)
        assert len(clone.lastnames) == len(provider.lastnames)

    def test_unknown_attribute_raises(self, provider):
        with pytest.raises(AttributeError):
            provider.unknown  # noqa: B018
