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
    if not all([isinstance(text, str), isinstance(key, str)]):
        msg = f"Both text and key should be strings, got {type(text)} and {type(key)}."
        raise TypeError(msg)

    if not key:
        return text

    if text.startswith(key):
        return strip_key(text[len(key) :], key)

    if text.endswith(key):
        return strip_key(text[: -len(key)], key)

    return text
