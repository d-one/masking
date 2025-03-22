import pytest


@pytest.fixture(
    scope="module",
    params=[
        (
            {
                "key1": "value1",
                "key2": "",
                "key3": {
                    "Key4": "value4",
                    "key5": "",
                    "Key6": {"Key7": "value7", "key8": ""},
                },
            },
            [
                "key1",
                "key2",
                ["key3", "Key4"],
                ["key3", "key5"],
                ["key3", "Key6", "Key7"],
                ["key3", "Key6", "key8"],
            ],
            ["value1", "", "value4", "", "value7", ""],
        ),
        (None, [""], [""]),
        ("string", [""], ["string"]),
        ([{"key1": "value1"}], [[0, "key1"]], ["value1"]),
        (
            {
                "Key1": "value1",
                "key2": "",
                "key3": {
                    "Key4": "value4",
                    "key5": "",
                    "key6": {"Key7": "value7", "key8": ""},
                },
                "key9": [
                    {
                        "key10": "value10",
                        "key11": "",
                        "Key12": {
                            "key13": "value13",
                            "key14": "",
                            "key15": {"Key16": "value16", "key17": ""},
                        },
                    },
                    "value17",
                    ["value18", {"Key19": None}],
                ],
            },
            [
                "Key1",
                "key2",
                ["key3", "Key4"],
                ["key3", "key5"],
                ["key3", "key6", "Key7"],
                ["key3", "key6", "key8"],
                ["key9", 0, "key10"],
                ["key9", 0, "key11"],
                ["key9", 0, "Key12", "key13"],
                ["key9", 0, "Key12", "key14"],
                ["key9", 0, "Key12", "key15", "Key16"],
                ["key9", 0, "Key12", "key15", "key17"],
                ["key9", 1],
                ["key9", 2, 0],
                ["key9", 2, 1, "Key19"],
            ],
            [
                "value1",
                "",
                "value4",
                "",
                "value7",
                "",
                "value10",
                "",
                "value13",
                "",
                "value16",
                "",
                "value17",
                "value18",
                "",
            ],
        ),
    ],
)
def multi_nested_dict_and_path_and_value(
    request: pytest.FixtureRequest,
) -> dict | str | list | None:
    return request.param


@pytest.fixture(scope="module", params=[True, False])
def case_sensitivity(request: pytest.FixtureRequest) -> bool:
    return request.param


@pytest.fixture(scope="module", params=[".", ".[$].", ",", "~"])
def path_separator(request: pytest.FixtureRequest) -> str:
    return request.param


@pytest.fixture(
    scope="module",
    params=[
        {"start": "[", "end": "]"},
        {"start": "{", "end": "}"},
        {"start": "(", "end": ")"},
        {"start": "<", "end": ">"},
    ],
)
def list_index(request: pytest.FixtureRequest) -> dict:
    return request.param
