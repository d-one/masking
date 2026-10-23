from unittest.mock import ANY, Mock, call

import pandas as pd
import polars as pl
import pytest
from masking.base_concordance_table.concordance_table import ConcordanceTableBase
from masking.base_operations.operation import Operation
from masking.mask.operations.operation import PandasOperation
from masking.mask.pipeline import ConcordanceTable as PandasConcordanceTable
from masking.mask.pipeline import MaskDataFramePipeline as PandasPipeline
from masking.mask_polars.operations.operation import PolarsOperation
from masking.mask_polars.pipeline import ConcordanceTable as PolarsConcordanceTable
from masking.mask_polars.pipeline import MaskDataFramePipeline as PolarsPipeline
from pyspark.sql import DataFrame as SparkDataFrame

COL = "name"
NAMES = ["Alice", "Bob", "Carol", "Alice", "Dave"]
PRESCRIBED = {"Alice": "PRESCRIBED_ALICE", "Carol": "PRESCRIBED_CAROL"}


def fake_mask(line: str) -> str:
    return f"masked_{line}"


# Minimal concrete table: the masking operation returns a clear -> masked dict.
class DictConcordanceTable(ConcordanceTableBase):
    def _build_concordance_table(self, data):
        return self.masking_operation(data)


# Mocked masking pipeline that knows nothing about concordance tables.
def mock_operation(col_name: str = COL, masked: dict | None = None) -> Mock:
    op = Mock(spec=Operation)
    op.col_name = col_name
    op.serving_columns = [col_name]
    op.return_value = masked if masked is not None else {}
    # Snapshot the handed-over tables: the base class mutates the same dict afterwards
    op.handed_over = []
    op.update_concordance_table.side_effect = lambda t: op.handed_over.append(dict(t))
    return op


# Real backend operation whose line-level masking is a Mock.
def make_operation(backend: str, mask_fn=fake_mask, **kwargs) -> Operation:
    base = PandasOperation if backend == "pandas" else PolarsOperation

    class MockedLineOperation(base):
        def _mask_line(self, line, **_):
            return self.mask_line(line)

    op = MockedLineOperation(col_name=COL, **kwargs)
    op.mask_line = Mock(side_effect=mask_fn)
    return op


BACKENDS = {
    "pandas": (PandasConcordanceTable, PandasPipeline, pd.DataFrame),
    "polars": (PolarsConcordanceTable, PolarsPipeline, pl.DataFrame),
}


@pytest.fixture(params=list(BACKENDS))
def backend(request):
    return request.param


def frame(backend: str, values: list | None = None):
    return BACKENDS[backend][2]({COL: list(values if values is not None else NAMES)})


def column(data) -> list:
    return [None if pd.isna(v) else v for v in data[COL].to_list()]


def masked_inputs(op: Operation) -> list[str]:
    return [c.args[0] for c in op.mask_line.call_args_list]


# ---------------------------------------------------------------------------
# __init__ / properties / clear
# ---------------------------------------------------------------------------


class TestInit:
    def test_is_abstract(self):
        with pytest.raises(TypeError, match="abstract"):
            ConcordanceTableBase(masking_operation=mock_operation())

    def test_default_concordance_table_is_empty_dict(self):
        table = DictConcordanceTable(masking_operation=mock_operation())
        assert table.concordance_table == {}

    def test_none_concordance_table_is_empty_dict(self):
        table = DictConcordanceTable(mock_operation(), concordance_table=None)
        assert table.concordance_table == {}

    def test_default_tables_are_not_shared(self):
        t1 = DictConcordanceTable(mock_operation())
        t2 = DictConcordanceTable(mock_operation())
        assert t1.concordance_table is not t2.concordance_table

    def test_prescribed_concordance_table_is_stored(self):
        prescribed = dict(PRESCRIBED)
        table = DictConcordanceTable(mock_operation(), concordance_table=prescribed)
        assert table.concordance_table is prescribed

    def test_masking_operation_is_stored(self):
        op = mock_operation()
        assert DictConcordanceTable(op).masking_operation is op

    def test_column_name_delegates_to_operation(self):
        assert DictConcordanceTable(mock_operation("col_x")).column_name == "col_x"

    def test_serving_columns_delegates_to_operation(self):
        op = mock_operation()
        op.serving_columns = [COL, "birthdate"]
        assert DictConcordanceTable(op).serving_columns == [COL, "birthdate"]

    def test_clear_concordance_table(self):
        table = DictConcordanceTable(mock_operation(), dict(PRESCRIBED))
        table.clear_concordance_table()
        assert table.concordance_table == {}


# ---------------------------------------------------------------------------
# _cast_concordance_table
# ---------------------------------------------------------------------------


# Mock a Spark DataFrame supporting ct_frame.select(c).rdd.flatMap(f).collect().
def spark_frame_mock(columns: dict[str, list]) -> Mock:
    ct_frame = Mock(spec=SparkDataFrame)

    def select(name):
        if name not in columns:
            msg = f"Column {name} does not exist"
            raise ValueError(msg)
        selected = Mock()
        selected.rdd.flatMap.return_value.collect.return_value = columns[name]
        return selected

    ct_frame.select.side_effect = select
    return ct_frame


class TestCastConcordanceTable:
    def test_none_becomes_empty_dict(self):
        table = DictConcordanceTable(mock_operation())
        table.concordance_table = None
        table._cast_concordance_table()
        assert table.concordance_table == {}

    def test_dict_is_unchanged(self):
        prescribed = dict(PRESCRIBED)
        table = DictConcordanceTable(mock_operation(), prescribed)
        table._cast_concordance_table()
        assert table.concordance_table is prescribed

    def test_pandas_dataframe_becomes_dict(self):
        ct_frame = pd.DataFrame({
            "clear_values": list(PRESCRIBED),
            "masked_values": list(PRESCRIBED.values()),
        })
        table = DictConcordanceTable(mock_operation(), ct_frame)
        table._cast_concordance_table()
        assert table.concordance_table == PRESCRIBED

    def test_pandas_dataframe_extra_columns_ignored(self):
        ct_frame = pd.DataFrame({
            "clear_values": ["a"],
            "masked_values": ["b"],
            "other": ["c"],
        })
        table = DictConcordanceTable(mock_operation(), ct_frame)
        table._cast_concordance_table()
        assert table.concordance_table == {"a": "b"}

    @pytest.mark.parametrize(
        "columns", [["clear_values"], ["masked_values"], ["clear", "masked"]]
    )
    def test_pandas_dataframe_missing_columns_raises(self, columns):
        ct_frame = pd.DataFrame({c: ["x"] for c in columns})
        table = DictConcordanceTable(mock_operation(), ct_frame)
        with pytest.raises(ValueError, match="Invalid concordance table"):
            table._cast_concordance_table()

    def test_spark_dataframe_becomes_dict(self):
        ct_frame = spark_frame_mock({
            "clear_values": list(PRESCRIBED),
            "masked_values": list(PRESCRIBED.values()),
        })
        table = DictConcordanceTable(mock_operation(), ct_frame)
        table._cast_concordance_table()
        assert table.concordance_table == PRESCRIBED
        assert ct_frame.select.call_args_list == [
            call("clear_values"),
            call("masked_values"),
        ]

    def test_spark_dataframe_missing_columns_raises(self):
        ct_frame = spark_frame_mock({"clear_values": ["a"]})
        table = DictConcordanceTable(mock_operation(), ct_frame)
        with pytest.raises(ValueError, match="Invalid concordance table"):
            table._cast_concordance_table()


# ---------------------------------------------------------------------------
# __call__ contract with a fully mocked masking pipeline
# ---------------------------------------------------------------------------


class TestCallContract:
    def test_prescribed_table_is_handed_to_operation_before_masking(self):
        op = mock_operation(masked={"Bob": "masked_Bob"})
        table = DictConcordanceTable(op, dict(PRESCRIBED))

        table("data")

        assert op.handed_over == [PRESCRIBED]
        assert op.mock_calls == [call.update_concordance_table(ANY), call("data")]

    def test_dataframe_prescription_is_cast_before_handing_over(self):
        op = mock_operation()
        ct_frame = pd.DataFrame({
            "clear_values": list(PRESCRIBED),
            "masked_values": list(PRESCRIBED.values()),
        })

        DictConcordanceTable(op, ct_frame)("data")

        assert op.handed_over == [PRESCRIBED]

    def test_empty_table_is_handed_over(self):
        op = mock_operation()
        DictConcordanceTable(op)("data")
        assert op.handed_over == [{}]

    def test_result_merges_prescribed_and_new_values(self):
        op = mock_operation(masked={"Bob": "masked_Bob", "Dave": "masked_Dave"})
        table = DictConcordanceTable(op, dict(PRESCRIBED))

        result = table("data")

        assert result == {**PRESCRIBED, "Bob": "masked_Bob", "Dave": "masked_Dave"}
        assert result is table.concordance_table

    def test_prescribed_values_kept_when_absent_from_data(self):
        op = mock_operation(masked={})
        result = DictConcordanceTable(op, dict(PRESCRIBED))("data")
        assert result == PRESCRIBED

    def test_values_accumulate_across_calls(self):
        op = mock_operation()
        table = DictConcordanceTable(op, dict(PRESCRIBED))

        op.return_value = {"Bob": "masked_Bob"}
        table("batch_1")
        op.return_value = {"Dave": "masked_Dave"}
        result = table("batch_2")

        assert result == {**PRESCRIBED, "Bob": "masked_Bob", "Dave": "masked_Dave"}
        # Values learned in the first batch are prescribed for the second one
        assert op.handed_over == [PRESCRIBED, {**PRESCRIBED, "Bob": "masked_Bob"}]

    def test_clear_removes_prescriptions_for_next_call(self):
        op = mock_operation(masked={"Bob": "masked_Bob"})
        table = DictConcordanceTable(op, dict(PRESCRIBED))

        table.clear_concordance_table()
        result = table("data")

        assert op.handed_over == [{}]
        assert result == {"Bob": "masked_Bob"}

    def test_operation_errors_propagate(self):
        op = mock_operation()
        op.update_concordance_table.side_effect = TypeError("invalid table")
        with pytest.raises(TypeError, match="invalid table"):
            DictConcordanceTable(op, {1: "x"})("data")
        op.assert_not_called()

    @pytest.mark.xfail(
        strict=True,
        reason="ConcordanceTableBase relies on the operation to honour prescribed "
        "values: whatever the operation returns overrides the prescription.",
    )
    def test_prescription_wins_over_operation_output(self):
        op = mock_operation(masked={"Alice": "other", "Bob": "masked_Bob"})
        result = DictConcordanceTable(op, dict(PRESCRIBED))("data")
        assert result["Alice"] == PRESCRIBED["Alice"]


# ---------------------------------------------------------------------------
# Prescribed values override masking (pandas / polars backends)
# ---------------------------------------------------------------------------


class TestPrescribedValuesOverrideMasking:
    def test_prescribed_values_are_not_masked(self, backend):
        op = make_operation(backend)
        table = BACKENDS[backend][0](op, dict(PRESCRIBED))

        result = table(frame(backend))

        assert result == {**PRESCRIBED, "Bob": "masked_Bob", "Dave": "masked_Dave"}
        assert sorted(set(masked_inputs(op))) == ["Bob", "Dave"]

    def test_all_values_prescribed_never_runs_masking(self, backend):
        op = make_operation(backend)
        prescribed = {n: f"P_{n}" for n in NAMES}

        result = BACKENDS[backend][0](op, prescribed)(frame(backend))

        assert result == prescribed
        op.mask_line.assert_not_called()

    def test_no_prescription_masks_everything(self, backend):
        op = make_operation(backend)
        result = BACKENDS[backend][0](op)(frame(backend))
        assert result == {n: f"masked_{n}" for n in NAMES}
        assert set(masked_inputs(op)) == set(NAMES)

    def test_dataframe_prescription(self, backend):
        op = make_operation(backend)
        prescribed = pd.DataFrame({
            "clear_values": list(PRESCRIBED),
            "masked_values": list(PRESCRIBED.values()),
        })

        result = BACKENDS[backend][0](op, prescribed)(frame(backend))

        assert result == {**PRESCRIBED, "Bob": "masked_Bob", "Dave": "masked_Dave"}
        assert sorted(set(masked_inputs(op))) == ["Bob", "Dave"]

    def test_prescription_overrides_operation_own_table(self, backend):
        op = make_operation(
            backend, concordance_table={"Alice": "FROM_OPERATION", "Bob": "OP_BOB"}
        )

        result = BACKENDS[backend][0](op, dict(PRESCRIBED))(frame(backend))

        assert result["Alice"] == "PRESCRIBED_ALICE"
        assert result["Bob"] == "OP_BOB"
        assert sorted(set(masked_inputs(op))) == ["Dave"]

    def test_identity_prescription_keeps_clear_value(self, backend):
        op = make_operation(backend)

        result = BACKENDS[backend][0](op, {"Alice": "Alice"})(frame(backend))

        assert result["Alice"] == "Alice"
        assert "Alice" not in masked_inputs(op)

    def test_new_values_do_not_collide_with_prescribed_masks(self, backend):
        # The first masking attempt for Bob collides with a prescribed masked value
        attempts = iter(["PRESCRIBED_ALICE", "masked_Bob"])
        op = make_operation(backend, mask_fn=lambda _: next(attempts))

        result = BACKENDS[backend][0](op, dict(PRESCRIBED))(
            frame(backend, ["Alice", "Bob"])
        )

        assert result == {**PRESCRIBED, "Bob": "masked_Bob"}
        assert masked_inputs(op) == ["Bob", "Bob"]

    def test_non_string_prescription_raises(self, backend):
        op = make_operation(backend)
        with pytest.raises(TypeError, match="Invalid concordance table"):
            BACKENDS[backend][0](op, {1: "one"})(frame(backend))
        op.mask_line.assert_not_called()


# ---------------------------------------------------------------------------
# clear_concordance_table also resets the masking operation
# ---------------------------------------------------------------------------


class TestClearResetsOperation:
    def test_mocked_operation_table_is_reset(self):
        op = mock_operation()
        table = DictConcordanceTable(op, dict(PRESCRIBED))
        op.concordance_table = {"stale": "value"}

        table.clear_concordance_table()

        assert op.concordance_table == {}

    def test_operation_forgets_handed_over_prescriptions(self, backend):
        op = make_operation(backend)
        table = BACKENDS[backend][0](op, dict(PRESCRIBED))
        table(frame(backend))
        assert op.concordance_table == PRESCRIBED

        table.clear_concordance_table()

        assert op.concordance_table == {}
        assert table(frame(backend, ["Alice"])) == {"Alice": "masked_Alice"}
        assert masked_inputs(op).count("Alice") == 1

    def test_operation_keeps_its_own_initial_table(self, backend):
        own = {"Bob": "OWN_BOB"}
        op = make_operation(backend, concordance_table=dict(own))
        table = BACKENDS[backend][0](op, dict(PRESCRIBED))
        table(frame(backend))

        table.clear_concordance_table()

        assert op.concordance_table == own
        result = table(frame(backend, ["Alice", "Bob"]))
        assert result == {"Alice": "masked_Alice", "Bob": "OWN_BOB"}

    def test_operation_forgets_learned_values(self, backend):
        op = make_operation(backend)
        type(op)._needs_unique_values = property(lambda _: True)
        table = BACKENDS[backend][0](op)
        table(frame(backend, ["Bob"]))
        assert op.concordance_table == {"Bob": "masked_Bob"}

        table.clear_concordance_table()

        assert op.concordance_table == {}

    def test_repeated_clears_restore_the_same_snapshot(self, backend):
        op = make_operation(backend, concordance_table={"Bob": "OWN_BOB"})
        table = BACKENDS[backend][0](op)

        table.clear_concordance_table()
        op.concordance_table["Dave"] = "mutated"
        table.clear_concordance_table()

        assert op.concordance_table == {"Bob": "OWN_BOB"}


# ---------------------------------------------------------------------------
# Prescribed values through the full masking pipeline
# ---------------------------------------------------------------------------


class TestPipelineWithPrescription:
    @pytest.mark.parametrize("workers", [1, 2])
    def test_column_uses_prescribed_values(self, backend, workers):
        op = make_operation(backend)
        pipeline = BACKENDS[backend][1](
            {COL: {"masking_operation": op, "concordance_table": dict(PRESCRIBED)}},
            workers=workers,
        )

        masked = pipeline(frame(backend))

        assert column(masked) == [
            "PRESCRIBED_ALICE",
            "masked_Bob",
            "PRESCRIBED_CAROL",
            "PRESCRIBED_ALICE",
            "masked_Dave",
        ]
        assert sorted(set(masked_inputs(op))) == ["Bob", "Dave"]

    def test_missing_values_are_untouched(self, backend):
        op = make_operation(backend)
        pipeline = BACKENDS[backend][1]({
            COL: {"masking_operation": op, "concordance_table": dict(PRESCRIBED)}
        })

        masked = pipeline(frame(backend, ["Alice", None, "Bob"]))

        assert column(masked) == ["PRESCRIBED_ALICE", None, "masked_Bob"]
        assert None not in masked_inputs(op)

    def test_clear_concordance_tables_drops_prescription(self, backend):
        op = make_operation(backend)
        pipeline = BACKENDS[backend][1]({
            COL: {"masking_operation": op, "concordance_table": dict(PRESCRIBED)}
        })

        pipeline.clear_concordance_tables()
        masked = pipeline(frame(backend, ["Alice"]))

        assert column(masked) == ["masked_Alice"]
        assert masked_inputs(op) == ["Alice"]
