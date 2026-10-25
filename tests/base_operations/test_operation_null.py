import pytest
from masking.base_operations.operation_null import NullOperationBase


class NullOperation(NullOperationBase):
    def _mask_data(self, data):
        return data


@pytest.mark.parametrize("line", ["a", "", 5, 1.5])
def test_default_condition_nulls_everything(line):
    assert NullOperation(col_name="c")._mask_line(line) is None


def test_condition_controls_masking():
    op = NullOperation(col_name="c", condition=lambda s: s.startswith("x"))
    assert op._mask_line("xa") is None
    assert op._mask_line("b") == "b"


def test_condition_receives_and_returns_strings():
    seen = []
    op = NullOperation(col_name="c", condition=lambda s: seen.append(s) or False)
    assert op._mask_line(5) == "5"
    assert seen == ["5"]


@pytest.mark.parametrize("bad", ["not callable", 1, True])
def test_invalid_condition_raises(bad):
    with pytest.raises(TypeError, match="Invalid condition"):
        NullOperation(col_name="c", condition=bad)


def test_check_mask_line_returns_none():
    op = NullOperation(col_name="c")
    assert op._check_mask_line("a") is None
    assert op._check_mask_line("b") is None
    assert op._check_mask_line(None) is None
