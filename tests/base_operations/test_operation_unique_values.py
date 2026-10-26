import itertools
import pickle  # noqa: S403
import time

from masking.base_operations.operation import Operation
from masking.utils.random_set import RandomSet


class _UniqueOp(Operation):
    """Operation returning values from a fixed sequence and requiring uniqueness."""

    def __init__(self, values: list[str], **kwargs: dict) -> None:
        super().__init__(col_name="c", **kwargs)
        self._values = itertools.cycle(values)

    @property
    def _needs_unique_values(self) -> bool:
        return True

    def _mask_line(self, line: str, **kwargs: dict) -> str:  # noqa: ARG002
        return next(self._values)

    def _mask_data(self, data):
        return data


class _CounterOp(_UniqueOp):
    def __init__(self) -> None:
        Operation.__init__(self, col_name="c")
        self._counter = itertools.count()

    def _mask_line(self, line: str, **kwargs: dict) -> str:  # noqa: ARG002
        return f"m{next(self._counter)}"


def test_collisions_are_retried() -> None:
    op = _UniqueOp(["a", "a", "b"])
    assert op._check_mask_line("x") == "a"
    assert op._check_mask_line("y") == "b"
    assert op.concordance_table == {"x": "a", "y": "b"}


def test_invalidation_releases_the_cached_table() -> None:
    op = _UniqueOp(["a", "b"])
    op._check_mask_line("x")
    assert op._masked_values_source is op.concordance_table

    op.update_concordance_table({"k": "z"})
    assert op._masked_values is None
    assert op._masked_values_source is None

    op._check_mask_line("y")
    op.concordance_table = {}
    op._cast_concordance_table()
    assert op._masked_values_source is None


def test_cache_is_not_pickled() -> None:
    op = _CounterOp()
    op._check_mask_line("x")
    assert op._masked_values == {"m0"}

    state = op.__getstate__()
    assert not set(Operation._MASKED_VALUES_CACHE) & state.keys()

    clone = pickle.loads(pickle.dumps(op))  # noqa: S301
    assert clone._masked_values is None
    assert clone.concordance_table == {"x": "m0"}
    assert clone._get_masked_values() == {"m0"}


def test_existing_table_values_are_avoided() -> None:
    op = _UniqueOp(["a", "b"], concordance_table={"k": "a"})
    assert op._check_mask_line("x") == "b"


def test_update_concordance_table_is_respected() -> None:
    op = _UniqueOp(["a", "b"])
    op._get_masked_values()
    op.update_concordance_table({"k": "a"})
    assert op._check_mask_line("x") == "b"


def test_external_modification_is_respected() -> None:
    op = _UniqueOp(["a", "b"])
    op._get_masked_values()
    op.concordance_table["k"] = "a"
    assert op._check_mask_line("x") == "b"

    op.concordance_table = {"k2": "b"}
    op._values = itertools.cycle(["b", "c"])
    assert op._check_mask_line("y") == "c"


def test_unique_masking_is_linear() -> None:
    op = _CounterOp()
    n = 50_000
    start = time.perf_counter()
    for i in range(n):
        op._check_mask_line(str(i))
    assert time.perf_counter() - start < 2
    assert len(set(op.concordance_table.values())) == n


def test_random_set() -> None:
    s = RandomSet()
    for i in range(10):
        s.add(i)
    s.add(3)
    assert len(s) == 10
    s.remove(0)
    s.remove(9)
    assert 0 not in s
    assert 9 not in s
    assert sorted(s) == list(range(1, 9))
    assert {s.choice() for _ in range(500)} == set(range(1, 9))
    while len(s):
        s.remove(s.choice())
    assert not s


def test_overwritten_entry_is_respected() -> None:
    op = _UniqueOp(["a", "target", "b"])
    assert op._check_mask_line("x") == "a"

    # Same table length, different masked value
    op.update_concordance_table({"x": "target"})
    op._values = itertools.cycle(["target", "a"])
    assert op._check_mask_line("y") == "a"
