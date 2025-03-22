from masking.utils.multi_nested_dict import MultiNestedDictHandler


def test_handler_has_path_separator(path_separator: str) -> None:
    # Check that the path_separator attribute is equal to the path_separator parameter
    handler = MultiNestedDictHandler(path_separator=path_separator)
    assert handler.path_separator == path_separator


def test_handler_has_case_sensitivity(case_sensitivity: bool) -> None:  # noqa: FBT001
    # Check that the case_sensitive attribute is equal to the case_sensitivity parameter
    handler = MultiNestedDictHandler(case_sensitive=case_sensitivity)
    assert handler.case_sensitive == case_sensitivity


def test_handler_constructs_path_and_value(
    multi_nested_dict_and_path_and_value: dict | str | list | None,
    path_separator: str,
    list_index: dict,
) -> None:
    # Check that the handler constructs the path and value correctly
    handler = MultiNestedDictHandler(path_separator=path_separator)

    data, expected_paths, expected_values = multi_nested_dict_and_path_and_value

    for v, (path, value) in enumerate(handler._find_leaf_path_and_value(data)):
        assert value == expected_values[v]

        if isinstance(expected_paths[v], list) and any(
            c in expected_paths[v] for c in (0, 1, 2)
        ):
            expected_path = path_separator.join(
                expected_paths[v][i]
                if isinstance(expected_paths[v][i], str)
                else f"{list_index['start']}{expected_paths[v][i]}{list_index['end']}"
                for i in range(len(expected_paths[v]))
            )
            return

        expected_path = path_separator.join(
            expected_paths[v]
            if isinstance(expected_paths[v], list)
            else [expected_paths[v]]
        )
        assert path == expected_path
