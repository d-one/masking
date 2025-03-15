from collections.abc import Callable, Generator
from typing import ClassVar

from masking.utils.string_handler import strip_key


class MultiNestedDictHandler:
    path_separator: ClassVar[str] = ".[$]."
    list_index: ClassVar[dict[str, str]] = {"start": "[", "end": "]"}
    deny_tag: ClassVar[str] = {"start": "deny<", "end": ">"}
    case_sensitive: ClassVar[bool] = False

    def __init__(  # noqa: PLR0913
        self,
        allow_keys: list[str | Callable[[str], bool]] | None = None,
        deny_keys: list[str | Callable[[str], bool]] | None = None,
        sep: str | None = None,
        list_index: dict | None = None,
        deny_tag: dict | None = None,
        case_sensitive: bool | None = None,  # noqa: FBT001
        **kwargs: dict,
    ) -> None:
        """Initialize the MultiNestedDict.

        Args:
        ----
            allow_keys (list): The allow keys to use
            deny_keys (list): The deny keys to use
            sep (str): The path separator
            list_index (dict): The list index
            deny_tag (dict): The deny tag
            case_sensitive (bool): The case sensitivity to use
            **kwargs (dict): keyword arguments

        """
        super().__init__(**kwargs)
        self.path_separator = sep if sep is not None else self.path_separator
        self.list_index = list_index if list_index is not None else self.list_index
        self.deny_tag = deny_tag if deny_tag is not None else self.deny_tag
        self.case_sensitive = (
            case_sensitive if case_sensitive is not None else self.case_sensitive
        )

        self.allow_keys = []
        self.allow_keys_callable = []
        if allow_keys is not None:
            self.allow_keys = [
                strip_key(k, self.path_separator).lower().strip()
                if not self.case_sensitive
                else strip_key(k, self.path_separator).strip()
                for k in allow_keys
                if isinstance(k, str)
            ]
            self.allow_keys_callable = [
                k for k in allow_keys if isinstance(k, Callable)
            ]

        self.deny_keys = []
        self.deny_keys_callable = []
        if deny_keys is not None:
            self.deny_keys = [
                strip_key(k, self.path_separator).lower().strip()
                if not self.case_sensitive
                else strip_key(k, self.path_separator).strip()
                for k in deny_keys
                if isinstance(k, str)
            ]
            self.deny_keys_callable = [k for k in deny_keys if isinstance(k, Callable)]

    def _deny_path(self, deny: str) -> str:
        return f"{self.deny_tag['start']}{deny}{self.deny_tag['end']}"

    def _is_denied_path(self, deny: str) -> bool:
        return deny.startswith(self.deny_tag["start"]) and deny.endswith(
            self.deny_tag["end"]
        )

    def _undeny_path(self, deny: str) -> str:
        """Remove the deny tag from the path.

        Args:
        ----
            deny (str): The deny path

        """
        return deny[len(self.deny_tag["start"]) : -len(self.deny_tag["end"])]

    # ------ SKIP ------

    def _has_match_callable(
        self, key: str, parent: str = "", mode: str = "allow"
    ) -> bool:
        """Check if the key has a match in the keys_list.

        Args:
        ----
            key (str): key to be checked
            parent (str): parent key
            mode (str): mode to be used

        Returns:
        -------
            bool: True if the key is in the keys_list, False otherwise

        """
        if mode == "allow":
            return any(
                check_key(self.path_separator.join([parent, key]) if parent else key)
                for check_key in self.allow_keys_callable
            )

        if mode == "deny":
            return any(
                check_key(self.path_separator.join([parent, key]) if parent else key)
                for check_key in self.deny_keys_callable
            )

        msg = f"Mode {mode} is not supported: use 'allow' or 'deny'."
        raise ValueError(msg)

    def _has_match_str(self, key: str, parent: str = "", mode: str = "allow") -> bool:
        """Check if the key has a match in the keys_list.

        The following rules are apply to keys in the keys_list:
        - If the key starts with "*", the key is a wildcard and the match is done
          by the end of the key.
        - If the key ends with "*", the key is a wildcard and the match is done
            by the start of the key.
        - If the key contains "[*]", the key is a wildcard and the match is done
            by ignoring the index of the list.
        - If the key contains "[k]" where k is a number, the key is a wildcard and the
            match is done by ignoring the index of the list that is different from k.

        Args:
        ----
            key (str): key to be checked
            parent (str): parent key
            mode (str): mode to be used

        Returns:
        -------
            bool: True if the key is in the keys_list, False otherwise

        """
        key_path = self.path_separator.join([parent, key]) if parent else key
        key_path_parts = key_path.split(self.path_separator)
        keys_list = self.allow_keys if mode == "allow" else self.deny_keys

        for allowed_key in keys_list:
            if not any([
                allowed_key.startswith("*"),
                allowed_key.startswith("[*]"),
                allowed_key.startswith(key_path),
            ]):
                continue

            allowed_parts = allowed_key.split(self.path_separator)

            # Handle the case where the key is a wildcard starting with "*"
            if allowed_parts[0] == "*":
                relevant_parts = allowed_parts[1:]
                if self.path_separator.join(
                    key_path_parts[-len(relevant_parts) :]
                ) == self.path_separator.join(relevant_parts):
                    return True
                continue

            # Handle the case where the key is a wildcard ending with "*"
            if allowed_parts[-1] == "*":
                relevant_parts = allowed_parts[:-1]
                if self.path_separator.join(
                    key_path_parts[: len(relevant_parts)]
                ) == self.path_separator.join(relevant_parts):
                    return True
                continue

            # Handle the case where the key is a wildcard containing "[*]"
            if "[*]" in allowed_key:
                allowed_parts_no_index = [
                    part_clean
                    for part in allowed_key.split("[*]")
                    if (part_clean := strip_key(part, self.path_separator))
                ]

                if all(key_part in key_path for key_part in allowed_parts_no_index):
                    return True
                continue

            # Default case
            if allowed_key == key_path:
                return True

        return False

    def _is_skippable(self, key: str, parent: str = "") -> bool:
        """Check if the line is skippable.

        Args:
        ----
            key (str): key to be checked
            parent (str): parent key

        Returns:
        -------
            bool: True if the key is in the allow_keys list, False otherwise

        """
        return any([
            self._has_match_callable(key, parent, mode="allow"),
            self._has_match_str(key, parent, mode="allow"),
        ])

    def _is_denied(self, key: str, parent: str = "") -> bool:
        """Check if the line is denied.

        Args:
        ----
            key (str): key to be checked
            parent (str): parent key

        Returns:
        -------
            bool: True if the key is in the deny_keys list, False otherwise

        """
        return any([
            self._has_match_callable(key, parent, mode="deny"),
            self._has_match_str(key, parent, mode="deny"),
        ])

    # ------ PATH ------

    def _index_to_path(self, index: int) -> str:
        """Convert the index to the path.

        Args:
        ----
            index (int): The index to convert

        Returns:
        -------
            str: The path of the index

        """
        return f"{self.list_index['start']}{index}{self.list_index['end']}"

    def _is_list_index(self, key: str) -> bool:
        """Check if the key is a list index.

        Returns true if the index consists of list_index['start'] + ind + list_index['end'],
        where ind is an integer.

        Args:
        ----
            key (str): The key to check

        Returns:
        -------
            bool: True if the key is a list index, False otherwise

        """
        return (
            key.startswith(self.list_index["start"])
            and key.endswith(self.list_index["end"])
            and self._get_list_index(key).isnumeric()
        )

    def _get_list_index(self, key: str) -> str:
        """Get the index from the key.

        Args:
        ----
            key (str): The key to get the index from

        Returns:
        -------
            str: The index of the key

        """
        return key[len(self.list_index["start"]) : -len(self.list_index["end"])]

    def _list_index_to_path(self, key: str) -> str:
        """Convert the list index to the path.

        Args:
        ----
            key (str): The key to convert

        Returns:
        -------
            str: The path of the key

        """
        return f"{self.list_index['start']}{key}{self.list_index['end']}"

    def _get_index(self, key: str) -> str | int:
        """Get the index from the key.

        Args:
        ----
            key (str): The key to get the index from

        Returns:
        -------
            str | int: The index of the key

        """
        if self._is_list_index(key):
            return int(self._get_list_index(key))
        return key

    def _get_leaf(self, data: dict | list | str, path: str | list) -> str | dict | list:
        """Get the value from the nested dictionary.

        Args:
        ----
            data (dict|list|str): The data to get the value from
            path (str): The path to the value

        Returns:
        -------
            str|list: The value of the path

        """
        if isinstance(data, str):
            return data

        if isinstance(path, str):
            path = path.split(self.path_separator)

        d = data
        for key in path:
            try:
                d = d[self._get_index(key)]
            except Exception as e:
                msg = f"Key {key} not found in {d} with data {data}"
                raise KeyError(msg) from e
        return d

    def _set_leaf(
        self, data: str | dict | list, path: str | list, value: str
    ) -> str | dict | list:
        """Set the value in the nested dictionary.

        Args:
        ----
            data (str|dict|list): The data to set the value in
            path (str): The path to the value
            value (str): The value to set

        """
        if not path:
            self.data = value
            return None

        if isinstance(path, str):
            path = path.split(self.path_separator)

        d = data
        for key in path[:-1]:
            try:
                d = d[self._get_index(key)]
            except Exception as e:
                msg = f"Key {key} not found in {d}"
                raise KeyError(msg) from e

        try:
            d[self._get_index(path[-1])] = value
        except Exception as e:
            msg = f"Key {path[-1]} not found in {d}"
            raise KeyError(msg) from e

        return data

    def _find_leaf_path(
        self, data: str | dict | list, parent: str = ""
    ) -> Generator[str, None, None]:
        """Find the leaf path in the nested dictionary.

        Args:
        ----
            data (str|dict|list): The data to find the leaf path in
            parent (str): The parent path

        Returns:
        -------
            Generator: The leaf path

        """
        if isinstance(data, dict):
            for key, value in data.items():
                key_path = f"{parent}{self.path_separator}{key}" if parent else key

                if self._is_skippable(
                    key.lower().strip() if not self.case_sensitive else key.strip(),
                    parent.lower().strip()
                    if not self.case_sensitive
                    else parent.strip(),
                ):
                    continue

                if self._is_denied(
                    key.lower().strip() if not self.case_sensitive else key.strip(),
                    parent.lower().strip()
                    if not self.case_sensitive
                    else parent.strip(),
                ):
                    yield self._deny_path(key_path)
                    continue

                yield from self._find_leaf_path(data=value, parent=key_path)

            return

        if isinstance(data, list):
            for index, value in enumerate(data):
                key_path = (
                    f"{parent}{self.path_separator}{self._list_index_to_path(index)}"
                    if parent
                    else self._list_index_to_path(index)
                )
                yield from self._find_leaf_path(data=value, parent=key_path)
            return

        if isinstance(data, str):
            yield parent
            return

        if data is None:
            yield parent
            return

        msg = f"Data type {type(data)} not supported"
        raise TypeError(msg)

    def _find_leaf_path_and_value(
        self, data: str | dict | list | None, parent: str = ""
    ) -> Generator[tuple[str, str], None, None]:
        """Find the leaf path and value in the nested dictionary.

        Args:
        ----
            data (str|dict|list): The data to find the leaf path in
            parent (str): The parent path

        Returns:
        -------
            Generator: The leaf path and value

        """
        if isinstance(data, dict):
            for key, value in data.items():
                key_path = f"{parent}{self.path_separator}{key}" if parent else key

                if self._is_skippable(
                    key.lower().strip() if not self.case_sensitive else key.strip(),
                    parent.lower().strip()
                    if not self.case_sensitive
                    else parent.strip(),
                ):
                    continue

                if self._is_denied(
                    key.lower().strip() if not self.case_sensitive else key.strip(),
                    parent.lower().strip()
                    if not self.case_sensitive
                    else parent.strip(),
                ):
                    yield (self._deny_path(key_path), value)
                    continue

                yield from self._find_leaf_path_and_value(data=value, parent=key_path)

            return

        if isinstance(data, list):
            for index, value in enumerate(data):
                key_path = (
                    f"{parent}{self.path_separator}{self._list_index_to_path(index)}"
                    if parent
                    else self._list_index_to_path(index)
                )
                yield from self._find_leaf_path_and_value(data=value, parent=key_path)
            return

        if isinstance(data, str):
            yield (parent, data)
            return

        if data is None:
            yield (parent, "")
            return

        msg = f"Data type {type(data)} not supported"
        raise TypeError(msg)

    def _find_leaf_path_and_value_batch(
        self, data: list[str | dict | list], parent: str = ""
    ) -> Generator[tuple[str, str], None, None]:
        """Find the leaf path and value in the nested dictionary.

        Args:
        ----
            data (str|dict|list): The data to find the leaf path in
            parent (str): The parent path

        Returns:
        -------
            Generator: The leaf path and value

        """
        for d in data:
            yield from self._find_leaf_path_and_value(data=d, parent=parent)
