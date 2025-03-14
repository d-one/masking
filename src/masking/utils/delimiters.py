DELIMITERS = {
    "<<>>": {"start": "<<", "end": ">>"},
    "<>": {"start": "<", "end": ">"},
    "[[]]": {"start": "[[", "end": "]]"},
    "[]": {"start": "[", "end": "]"},
    "()": {"start": "(", "end": ")"},
    "(())": {"start": "((", "end": "))"},
    "''": {"start": "'", "end": "'"},
    '""': {"start": '"', "end": '"'},
}


def get_delimiter(delimiter: str) -> dict:
    # Split the delimiter in the middle. if the delimiter has an odd number of chars, then complete it to the right with an empty char
    left_delimiter = delimiter[: len(delimiter) // 2]
    right_delimiter = (
        delimiter[len(delimiter) // 2 :] + delimiter[len(delimiter) // 2]
        if len(delimiter) % 2 != 0
        else delimiter[len(delimiter) // 2 :]
    )
    return {"start": left_delimiter, "end": right_delimiter}
