import pytest
from masking.utils.string_handler import generate_unique_name, strip_key


@pytest.mark.parametrize(
    "inputs",
    [  # ("text", "key", "expected_result"),
        ("keytextkey", "key", "text"),
        ("keytext", "key", "text"),
        ("textkey", "key", "text"),
        ("textkeytext", "key", "textkeytext"),
        ("text", "key", "text"),
        ("key", "key", ""),
        ("", "key", ""),
        ("text", "", "text"),
        ("", "", ""),
        ("text", "text", ""),
        ("textkeytext", "text", "key"),
        ("texttextkeytexttext", "text", "key"),
        ("keykeykeytextkeykeykey", "key", "text"),
    ],
)
def test_strip_key(inputs: tuple[str]) -> None:
    """Test if the key is stripped from the text."""
    text, key, expected_result = inputs

    assert strip_key(text, key) == expected_result


@pytest.mark.parametrize(
    ("text", "key"), [(None, "key"), ("text", None), (1, "key"), ("text", 1)]
)
def test_strip_key_non_string_raises(text, key) -> None:
    with pytest.raises(TypeError, match="should be strings"):
        strip_key(text, key)


@pytest.mark.parametrize(
    ("kwargs", "expected"),
    [
        ({}, "column"),
        ({"existing_names": None}, "column"),
        ({"existing_names": []}, "column"),
        ({"seed": "name", "existing_names": ["other"]}, "name"),
        ({"seed": "name", "existing_names": ["name"]}, "name_"),
        ({"seed": "name", "existing_names": ["name", "name_"]}, "name__"),
        ({"seed": "name", "existing_names": ["name_"]}, "name"),
        ({"seed": "name", "existing_names": ["name"], "sep": "-"}, "name-"),
        ({"seed": "name", "existing_names": ["name", "name#"], "sep": "#"}, "name##"),
        ({"seed": "", "existing_names": [""]}, "_"),
        ({"existing_names": ["column"]}, "column_"),
        ({"seed": "a", "existing_names": ["a", "a_"], "max_iter": 5}, "a__"),
    ],
)
def test_generate_unique_name(kwargs, expected) -> None:
    result = generate_unique_name(**kwargs)
    assert result == expected
    assert result not in (kwargs.get("existing_names") or [])


def test_generate_unique_name_does_not_mutate_existing_names() -> None:
    existing = ["name"]
    generate_unique_name("name", existing)
    assert existing == ["name"]


def test_generate_unique_name_max_iter_reached_raises() -> None:
    existing = ["a", "a_", "a__", "a___"]
    with pytest.raises(ValueError, match="maximum number of iterations"):
        generate_unique_name("a", existing, max_iter=3)


def test_generate_unique_name_max_iter_zero_raises() -> None:
    with pytest.raises(ValueError, match="maximum number of iterations"):
        generate_unique_name("a", ["a"], max_iter=0)


def test_generate_unique_name_max_iter_zero_unused_when_seed_unique() -> None:
    assert generate_unique_name("a", [], max_iter=0) == "a"


@pytest.mark.xfail(
    strict=True,
    reason="Bug: off-by-one, raises when a unique name is found on the last allowed iteration.",
)
def test_generate_unique_name_succeeds_on_last_iteration() -> None:
    assert generate_unique_name("a", ["a"], max_iter=1) == "a_"
