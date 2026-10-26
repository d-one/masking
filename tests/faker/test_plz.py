import pytest
from masking.faker.plz import FakePLZProvider

REPEAT = 30
ALL = ("district", "area", "route", "postcode")


@pytest.fixture(scope="module")
def known_plz() -> set[str]:
    return {str(p) for p in FakePLZProvider().files["PLZ"]}


class TestInit:
    def test_files_are_loaded(self):
        files = FakePLZProvider().files
        assert list(files.columns) == ["PLZ"]
        assert len(files) > 1000

    @pytest.mark.parametrize(
        ("preserve", "expected"),
        [("district", {"district"}), (("district", "area"), {"district", "area"})],
    )
    def test_preserve(self, preserve, expected):
        assert expected == FakePLZProvider(preserve)._PRESERVED

    def test_invalid_preserve_raises(self):
        with pytest.raises(ValueError, match="Preserve values"):
            FakePLZProvider("canton")


class TestParsing:
    def test_parse_plz(self):
        assert FakePLZProvider().parse_plz("3436") == {
            "district": "3",
            "area": "4",
            "route": "3",
            "postcode": "6",
        }

    @pytest.mark.parametrize("plz", ["", "123", "12345"])
    def test_invalid_length_raises(self, plz):
        with pytest.raises(ValueError, match="4 digits"):
            FakePLZProvider().parse_plz(plz)

    @pytest.mark.parametrize(
        ("preserve", "expected"),
        [
            (None, {"start": 0, "end": 10000}),
            ("district", {"start": 3000, "end": 4000}),
            (("district", "area"), {"start": 3400, "end": 3500}),
            (("district", "area", "route"), {"start": 3430, "end": 3440}),
            (ALL, {"start": 3436, "end": 3437}),
        ],
    )
    def test_extract_ranges(self, preserve, expected):
        provider = FakePLZProvider(preserve)
        assert provider.extract_ranges("3436") == expected
        assert provider.extract_ranges(provider.parse_plz("3436")) == expected


class TestCall:
    @pytest.mark.parametrize("plz", ["8001", 8001])
    def test_returns_known_plz(self, plz, known_plz):
        provider = FakePLZProvider()
        values = {provider(plz) for _ in range(REPEAT)}
        assert values <= known_plz
        assert len(values) > 1

    def test_preserved_prefix(self, known_plz):
        provider = FakePLZProvider(("district", "area"))
        for _ in range(REPEAT):
            value = provider("8001")
            assert value.startswith("80")
            assert value in known_plz

    @pytest.mark.parametrize("plz", ["0000", 8001])
    def test_preserving_everything_returns_input(self, plz):
        assert FakePLZProvider(ALL)(plz) == str(plz)
