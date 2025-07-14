import json
from collections import deque
from collections.abc import Callable, Generator

from masking.utils.string_handler import strip_key


class MultiNestedDictHandler:
    path_separator: str
    list_index: dict[str, str]
    deny_tag: dict[str, str]
    case_sensitive: bool
    allow_keys: list[str | Callable[[str], bool]]
    allow_keys_callable: list[Callable[[str], bool]]

    def __init__(  # noqa: PLR0913
        self,
        allow_keys: list[str] | None = None,
        deny_keys: list[str] | None = None,
        path_separator: str | None = None,
        list_index: dict[str, str] | None = None,
        deny_tag: dict[str, str] | None = None,
        case_sensitive: bool | None = None,  # noqa: FBT001
        **kwargs: dict,
    ) -> None:
        """Initialize the MultiNestedDict.

        Args:
        ----
            allow_keys (list): The allow keys to use
            deny_keys (list): The deny keys to use
            path_separator (str): The path separator to use
            list_index (dict): The list index to use
            deny_tag (dict): The deny tag to use
            case_sensitive (bool): The case sensitivity to use
            **kwargs (dict): keyword arguments

        """
        super().__init__(**kwargs)

        self.path_separator = path_separator or ".[$]."
        self.list_index = list_index or {"start": "[", "end": "]"}
        self.deny_tag = deny_tag or {"start": "deny<", "end": ">"}
        self.case_sensitive = case_sensitive if case_sensitive is not None else False

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
        return strip_key(strip_key(deny, self.deny_tag["end"]), self.deny_tag["start"])

    @staticmethod
    def _parse_line(line: str | dict | list | None) -> dict | list | str:
        """Parse a line into a dictionary.

        Args:
        ----
            line (str | dict): input line as a string or dictionary

        Returns:
        -------
            dict | list | str: parsed line as a dictionary or list, or original string if parsing fails

        """
        if line is None:
            return ""

        if isinstance(line, dict | list):
            return line

        try:
            return json.loads(line, strict=False)
        except json.JSONDecodeError:
            pass

        return line

    @staticmethod
    def _dump_line(line: dict | list | str) -> str:
        """Dump a line into a string.

        Args:
        ----
            line (dict | list | str): input line as a dictionary or list

        Returns:
        -------
            str: dumped line as a string

        """
        if isinstance(line, dict | list):
            return json.dumps(line, ensure_ascii=False, indent=4)

        if isinstance(line, str):
            return line

        msg = f"Unsupported type {type(line)} for dumping."
        raise TypeError(msg)

    # ------ SKIP ------

    def _has_match_callable(
        self, key: str, parent: str | None = None, mode: str = "allow"
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
                check_key(
                    self.path_separator.join([parent, key])
                    if parent is not None
                    else key
                )
                for check_key in self.allow_keys_callable
            )

        if mode == "deny":
            return any(
                check_key(
                    self.path_separator.join([parent, key])
                    if parent is not None
                    else key
                )
                for check_key in self.deny_keys_callable
            )

        msg = f"Mode {mode} is not supported: use 'allow' or 'deny'."
        raise ValueError(msg)

    def _has_match_str(
        self, key: str, parent: str | None = None, mode: str = "allow"
    ) -> bool:
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
        key_path = (
            self.path_separator.join([parent, key]) if parent is not None else key
        )
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

    def _is_skippable(self, key: str, parent: str | None = None) -> bool:
        """Check if the line is skippable.

        Args:
        ----
            key (str): key to be checked
            parent (str): parent key

        Returns:
        -------
            bool: True if the key is in the allow_keys list, False otherwise

        """
        p = parent
        if (p is not None) and (not self.case_sensitive):
            p = p.lower().strip()

        return any([
            self._has_match_callable(key, p, mode="allow"),
            self._has_match_str(key, p, mode="allow"),
        ])

    def _is_denied(self, key: str, parent: str | None = None) -> bool:
        """Check if the line is denied.

        Args:
        ----
            key (str): key to be checked
            parent (str): parent key

        Returns:
        -------
            bool: True if the key is in the deny_keys list, False otherwise

        """
        p = parent
        if (p is not None) and (not self.case_sensitive):
            p = p.lower().strip()

        return any([
            self._has_match_callable(key, p, mode="deny"),
            self._has_match_str(key, p, mode="deny"),
        ])

    # ------ PATH ------

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
        return strip_key(
            strip_key(key, self.list_index["end"]), self.list_index["start"]
        )

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

    def _get_leaf(
        self, data: dict | list | str | None, path: str | list | None
    ) -> str | dict | list:
        """Get the value from the nested dictionary.

        Args:
        ----
            data (dict|list|str): The data to get the value from
            path (str): The path to the value

        Returns:
        -------
            str|list: The value of the path

        """
        if isinstance(data, str):  # or data is None:
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

    def _get_undenied_and_denied_paths(
        self, line: dict | str
    ) -> tuple[Generator[str, None, None]]:
        """Get the paths to mask and deny.

        Args:
        ----
            line (dict): input dictionary

        Returns:
        -------
            tuple[Generator[str, None, None]]: The paths to mask and deny

        """
        mask_queue = deque()
        deny_queue = deque()

        def mask_generator() -> Generator[str, None, None]:
            """Generate for the paths to mask.

            Pops from left of the queue and yields the path.
            """
            while mask_queue:
                yield mask_queue.popleft()

        def deny_generator() -> Generator[str, None, None]:
            """Generate for the paths to deny.

            Pops from left of the queue and yields the path.
            """
            while deny_queue:
                yield deny_queue.popleft()

        # Fill the queues with paths
        for leaf in self._find_leaf_path(line):
            if self._is_denied_path(leaf):
                deny_queue.append(self._undeny_path(leaf))
                continue

            mask_queue.append(leaf)

        # Return the generators
        return mask_generator(), deny_generator()

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
        self, data: str | dict | list | None, parent: str | None = None
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
                key_path = (
                    f"{parent}{self.path_separator}{key}" if parent is not None else key
                )

                if self._is_skippable(
                    key.lower().strip() if not self.case_sensitive else key.strip(),
                    parent,
                ):
                    continue

                if self._is_denied(
                    key.lower().strip() if not self.case_sensitive else key.strip(),
                    parent,
                ):
                    yield self._deny_path(key_path)
                    continue

                yield from self._find_leaf_path(data=value, parent=key_path)

            return

        if isinstance(data, list):
            for index, value in enumerate(data):
                key_path = (
                    f"{parent}{self.path_separator}{self._list_index_to_path(index)}"
                    if parent is not None
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
        self, data: str | dict | list | None, parent: str | None = None
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
                key_path = (
                    f"{parent}{self.path_separator}{key}" if parent is not None else key
                )

                if self._is_skippable(
                    key.lower().strip() if not self.case_sensitive else key.strip(),
                    parent,
                ):
                    continue

                if self._is_denied(
                    key.lower().strip() if not self.case_sensitive else key.strip(),
                    parent,
                ):
                    yield (self._deny_path(key_path), value)
                    continue

                yield from self._find_leaf_path_and_value(data=value, parent=key_path)

            return

        if isinstance(data, list):
            for index, value in enumerate(data):
                key_path = (
                    f"{parent}{self.path_separator}{self._list_index_to_path(index)}"
                    if parent is not None
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
        self, data: list[str | dict | list], parent: str | None = None
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

    def _find_leaf_path_batch(
        self, data: list[str | dict | list], parent: str | None = None
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
        for d in data:
            yield from self._find_leaf_path(data=d, parent=parent)
