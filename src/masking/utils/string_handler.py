def strip_key(text: str, key: str) -> str:
    """Strip the key from the text.

    Args:
    ----
        text (str): text to be stripped
        key (str): key to be stripped

    Returns:
    -------
        str: stripped text

    """
    if text.startswith(key):
        text = text[len(key) :]

    if text.endswith(key):
        text = text[: -len(key)]

    return text
