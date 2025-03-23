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


def generate_unique_name(
    seed: str = "column",
    existing_names: list | None = None,
    max_iter: int = 1000,
    sep: str = "_",
) -> str:
    """Generate a unique name by appending a number to the seed.

    Args:
    ----
        seed (str, optional): The seed name. Defaults to "column".
        existing_names (list, optional): A list of existing names. Defaults to [].
        max_iter (int, optional): The maximum number of iterations. Defaults to 1000.
        sep (str, optional): The separator between the seed and the number. Defaults to "_".

    Returns:
    -------
        str: A unique name.

    """
    if existing_names is None:
        existing_names = []

    if seed not in existing_names:
        return seed

    iteration = 0
    while (seed in existing_names) and (iteration < max_iter):
        seed += sep
        iteration += 1

    if iteration >= max_iter:
        msg = "The maximum number of iterations has been reached."
        raise ValueError(msg)

    return seed
