import random
from collections.abc import Hashable, Iterator


class RandomSet:
    """Set supporting O(1) add, remove and uniform random choice."""

    def __init__(self) -> None:
        self._items: list[Hashable] = []
        self._index: dict[Hashable, int] = {}

    def add(self, item: Hashable) -> None:
        """Add an item to the set."""
        if item not in self._index:
            self._index[item] = len(self._items)
            self._items.append(item)

    def remove(self, item: Hashable) -> None:
        """Remove an item from the set.

        Raises
        ------
            KeyError: if the item is not in the set

        """
        i = self._index.pop(item)
        last = self._items.pop()
        if i < len(self._items):
            self._items[i] = last
            self._index[last] = i

    def choice(self) -> Hashable:
        """Return a random item of the set.

        Returns
        -------
            Hashable: random item

        """
        return random.choice(self._items)  # noqa: S311

    def __contains__(self, item: Hashable) -> bool:
        return item in self._index

    def __len__(self) -> int:
        return len(self._items)

    def __iter__(self) -> Iterator[Hashable]:
        return iter(self._items)
