import itertools

import pandas as pd
import pytest
from masking.base_operations.operation import Operation


class Recorder(Operation):
    """Operation recording calls to _mask_line and returning queued outputs."""

    def __init__(
        self, outputs: list | None = None, *, unique: bool = False, **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.outputs = list(outputs or [])
        self.unique = unique
        self.calls = []

    @property
    def _needs_unique_values(self) -> bool:
        return self.unique

    def _mask_line(self, line, **kwargs):
        self.calls.append((line, kwargs))
        return self.outputs.pop(0) if self.outputs else f"m_{line}"

    def _mask_data(self, data):
        return [self._check_mask_line(v) for v in data]


class Plain(Operation):
    def _mask_line(self, line, **kwargs):  # noqa: ARG002
        return line

    def _mask_data(self, data):
        return data


def make(**kwargs) -> Recorder:
    return Recorder(col_name="c", **kwargs)


class TestAttributes:
    def test_needs_unique_values_defaults_to_false(self):
        assert Plain(col_name="c")._needs_unique_values is False

    def test_update_col_name(self):
        op = make()
        op.update_col_name("other")
        assert op.col_name == "other"
        assert op.serving_columns == ["other"]

    def test_kwargs_not_consumed_raise(self):
        with pytest.raises(TypeError):
            Plain(col_name="c", unknown=1)


class TestCastConcordanceTable:
    def test_none_becomes_empty_dict(self):
        assert Operation.cast_concordance_table(None) == {}

    def test_empty_dict_is_returned_as_is(self):
        table = {}
        assert Operation.cast_concordance_table(table) is table

    def test_pandas_dataframe(self):
        table = pd.DataFrame({"clear_values": ["a", "b"], "masked_values": ["A", "B"]})
        assert Operation.cast_concordance_table(table) == {"a": "A", "b": "B"}

    def test_pandas_dataframe_missing_columns_raises(self):
        with pytest.raises(TypeError, match="clear_values"):
            Operation.cast_concordance_table(pd.DataFrame({"a": [1]}))

    def test_dict_with_non_string_values_raises(self):
        with pytest.raises(TypeError):
            Operation.cast_concordance_table({"a": 1})

    @pytest.mark.parametrize("bad", [[("a", "b")], "a", 1])
    def test_unsupported_type_raises(self, bad):
        with pytest.raises(TypeError):
            Operation.cast_concordance_table(bad)


class TestUpdateConcordanceTable:
    def test_merges_into_existing_table(self):
        op = make(concordance_table={"a": "A"})
        op.update_concordance_table({"b": "B"})
        assert op.concordance_table == {"a": "A", "b": "B"}

    def test_overrides_existing_entries(self):
        op = make(concordance_table={"a": "A"})
        op.update_concordance_table({"a": "Z"})
        assert op.concordance_table == {"a": "Z"}

    @pytest.mark.parametrize("bad", [{1: "A"}, {"a": 1}, {"a": None}])
    def test_non_string_entries_raise(self, bad):
        with pytest.raises(TypeError):
            make().update_concordance_table(bad)


class TestCheckMaskLine:
    def test_none_is_not_masked(self):
        op = make()
        assert op._check_mask_line(None) is None
        assert op.calls == []

    def test_concordance_table_hit_skips_masking(self):
        op = make(concordance_table={"a": "A"})
        assert op._check_mask_line("a") == "A"
        assert op.calls == []

    def test_kwargs_are_forwarded_to_mask_line(self):
        op = make()
        op._check_mask_line("a", additional_values={"x": 1}, extra=2)
        assert op.calls == [("a", {"additional_values": {"x": 1}, "extra": 2})]

    def test_non_unique_operation_does_not_record_values(self):
        op = make()
        assert op._check_mask_line("a") == "m_a"
        assert op.concordance_table == {}

    def test_unique_operation_records_values(self):
        op = make(unique=True)
        op._check_mask_line("a")
        assert op.concordance_table == {"a": "m_a"}

    def test_masked_values_of_the_table_are_avoided(self):
        op = make(outputs=["A", "other"], concordance_table={"k": "A"})
        assert op._check_mask_line("a") == "other"
        assert len(op.calls) == 2

    def test_max_retry_raises(self):
        op = make(concordance_table={"k": "dup"})
        op.MAX_RETRY = 3
        op.outputs = list(itertools.repeat("dup", 10))
        with pytest.raises(ValueError, match="Maximum number of retries"):
            op._check_mask_line("a")
        assert len(op.calls) == 4


class TestCall:
    def test_casts_table_and_masks_data(self):
        op = make()
        op.concordance_table = pd.DataFrame({
            "clear_values": ["a"],
            "masked_values": ["A"],
        })
        assert op(["a", "b"]) == ["A", "m_b"]
        assert op.concordance_table == {"a": "A"}
