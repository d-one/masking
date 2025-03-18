import pytest
from masking.utils.string_handler import strip_key


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
