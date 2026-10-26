import itertools

import pytest
from masking.base_operations.operation_fake import FakerOperation


class SequenceFaker(FakerOperation):
    """Faker operation returning values from a fixed sequence."""

    def __init__(self, outputs, provider=None, **kwargs) -> None:
        super().__init__(col_name="c", provider=provider, **kwargs)
        self.outputs = iter(outputs)

    def _mask_like_faker(self, line):  # noqa: ARG002
        return next(self.outputs)

    def _mask_data(self, data):
        return data


def test_provider_is_stored():
    provider = object()
    assert SequenceFaker([], provider=provider).faker is provider


def test_needs_unique_values():
    assert SequenceFaker([])._needs_unique_values is True


def test_mask_line_retries_until_value_changes():
    assert SequenceFaker(["a", "a", "b"])._mask_line("a") == "b"


def test_mask_line_raises_when_value_never_changes():
    sentinel = "Sentinel Clear Value"
    op = SequenceFaker(itertools.repeat(sentinel))
    op.MAX_RETRY_MASK_LINE = 5
    with pytest.raises(ValueError, match="after 5 attempts for column c") as exc:
        op._mask_line(sentinel)
    assert sentinel not in str(exc.value)


def test_check_mask_line_records_unique_values():
    op = SequenceFaker(["b", "c"], concordance_table={"k": "b"})
    assert op._check_mask_line("x") == "c"
    assert op.concordance_table == {"k": "b", "x": "c"}


def test_same_input_reuses_recorded_value():
    op = SequenceFaker(["b", "c"])
    assert op._check_mask_line("x") == "b"
    assert op._check_mask_line("x") == "b"
