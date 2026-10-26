from functools import reduce
from unittest.mock import Mock, patch

import pandas as pd
import polars as pl
import pytest
from masking.base_concordance_table.concordance_table import ConcordanceTableBase
from masking.base_pipeline.pipeline import MaskDataFramePipelineBase
from masking.mask.operations.operation import PandasOperation
from masking.mask.pipeline import ConcordanceTable as PandasConcordanceTable
from masking.mask.pipeline import MaskDataFramePipeline as PandasPipeline
from masking.mask_polars.operations.operation import PolarsOperation
from masking.mask_polars.pipeline import MaskDataFramePipeline as PolarsPipeline

COL = "name"
OTHER = "city"

BACKENDS = {
    "pandas": (PandasPipeline, PandasOperation, pd.DataFrame),
    "polars": (PolarsPipeline, PolarsOperation, pl.DataFrame),
}


# Deterministic masking function that records the composition, e.g. g(f(a)).
def tag(name: str):
    return lambda line, _=None: f"{name}({line})"


def identity(line, _=None):
    return line


# Masking function mapping some values and leaving the others untouched.
def mapping(table: dict):
    return lambda line, _=None: table.get(line, line)


# Real backend operation whose line-level masking is a Mock.
def make_operation(
    backend: str,
    mask_fn=None,
    col_name: str = COL,
    serving_columns: list[str] | None = None,
    **kwargs,
):
    class MockedLineOperation(BACKENDS[backend][1]):
        @property
        def serving_columns(self):
            return serving_columns or [self.col_name]

        def _mask_line(self, line, additional_values=None, **_):
            return self.mask_line(line, additional_values or {})

    op = MockedLineOperation(col_name=col_name, **kwargs)
    op.mask_line = Mock(side_effect=mask_fn or tag("f"))
    return op


def step(op, table: dict | None = None) -> dict:
    cfg = {"masking_operation": op}
    if table is not None:
        cfg["concordance_table"] = table
    return cfg


def make_pipeline(backend: str, configuration: dict, workers: int = 1):
    return BACKENDS[backend][0](configuration, workers=workers)


def frame(backend: str, data: dict):
    return BACKENDS[backend][2](data)


def column(data, name: str = COL) -> list:
    return [None if pd.isna(v) else v for v in data[name].to_list()]


def inputs(op) -> list:
    return [c.args[0] for c in op.mask_line.call_args_list]


@pytest.fixture(params=list(BACKENDS))
def backend(request):
    return request.param


@pytest.fixture(params=[1, 2], ids=["serial", "parallel"])
def workers(request):
    return request.param


# Concrete base pipeline used to test the backend-agnostic logic in isolation.
class StubPipeline(MaskDataFramePipelineBase):
    _get_unique_values = staticmethod(Mock())
    _filter_data = staticmethod(Mock())
    _substitute_masked_values = staticmethod(Mock())
    _null_values = staticmethod(Mock())
    _copy_column = staticmethod(Mock())
    _rename_columns = staticmethod(Mock())


def mock_table(col_name: str = COL, serving: list[str] | None = None) -> Mock:
    table = Mock(spec=ConcordanceTableBase)
    table.column_name = col_name
    table.serving_columns = serving or [col_name]
    table.nulls_values = False
    table.masking_operation = Mock()
    table.masking_operation.col_name = col_name
    table.masking_operation.serving_columns = serving or [col_name]
    return table


def mock_dtype() -> Mock:
    # ConcordanceTable factory returning mocks bound to the configured operation.

    def build(masking_operation, **_):
        table = mock_table(
            masking_operation.col_name, masking_operation.serving_columns
        )
        table.masking_operation = masking_operation
        return table

    return Mock(side_effect=build)


def mock_op(col_name: str = COL, serving: list[str] | None = None) -> Mock:
    op = Mock()
    op.col_name = col_name
    op.serving_columns = serving or [col_name]
    return op


# ===========================================================================
# Single functions
# ===========================================================================


class TestInit:
    def test_is_abstract(self):
        with pytest.raises(TypeError, match="abstract"):
            MaskDataFramePipelineBase({})

    def test_single_operation_config(self):
        dtype, op = mock_dtype(), mock_op()
        pipeline = StubPipeline({COL: {"masking_operation": op, "x": 1}}, dtype=dtype)

        dtype.assert_called_once_with(masking_operation=op, x=1)
        assert len(pipeline.col_pipelines) == 1
        assert pipeline.col_pipelines[0].masking_operation is op
        assert dict(pipeline.output_columns_mapping) == {
            COL: {"col_name": COL, "serving_columns": [COL], "col_pipeline_index": 0}
        }

    def test_list_config_builds_ordered_pipelines(self):
        dtype = mock_dtype()
        ops = [mock_op(), mock_op(serving=[COL, OTHER])]
        pipeline = StubPipeline(
            {COL: [{"masking_operation": o} for o in ops]}, dtype=dtype
        )

        assert len(pipeline.col_pipelines) == 1
        assert [p.masking_operation for p in pipeline.col_pipelines[0]] == ops
        mapping_ = pipeline.output_columns_mapping[COL]
        assert mapping_["col_name"] == COL
        assert sorted(mapping_["serving_columns"]) == sorted([COL, OTHER])

    def test_multiple_columns_indexes(self):
        pipeline = StubPipeline(
            {
                "a": {"masking_operation": mock_op("a")},
                "b": [{"masking_operation": mock_op("b")}],
            },
            dtype=mock_dtype(),
        )
        assert pipeline.output_columns_mapping["a"]["col_pipeline_index"] == 0
        assert pipeline.output_columns_mapping["b"]["col_pipeline_index"] == 1

    def test_output_column_different_from_operation_column(self):
        pipeline = StubPipeline(
            {"masked": {"masking_operation": mock_op(COL)}}, dtype=mock_dtype()
        )
        assert pipeline.output_columns_mapping["masked"]["col_name"] == COL

    def test_list_with_different_column_names_raises(self):
        with pytest.raises(ValueError, match="same column name"):
            StubPipeline(
                {
                    COL: [
                        {"masking_operation": mock_op(COL)},
                        {"masking_operation": mock_op(OTHER)},
                    ]
                },
                dtype=mock_dtype(),
            )

    @pytest.mark.parametrize("bad", ["not a dict", 1, None, [{"a": 1}]])
    def test_list_with_invalid_entry_raises(self, bad):
        with pytest.raises(ValueError, match="Invalid configuration"):
            StubPipeline(
                {COL: [{"masking_operation": mock_op()}, bad]}, dtype=mock_dtype()
            )

    def test_empty_list_raises(self):
        with pytest.raises(ValueError, match="empty list"):
            StubPipeline({COL: []}, dtype=mock_dtype())

    @pytest.mark.parametrize("bad", ["string", 1, None, ("tuple",)])
    def test_invalid_config_type_raises(self, bad):
        with pytest.raises(ValueError, match="Invalid configuration"):
            StubPipeline({COL: bad}, dtype=mock_dtype())

    def test_workers(self):
        assert StubPipeline({}, workers=3).workers == 3
        assert StubPipeline({}).workers == 1

    def test_concordance_tables_are_per_instance(self):
        p1, p2 = StubPipeline({}), StubPipeline({})
        p1.concordance_tables[COL] = {"a": "b"}
        assert p2.concordance_tables == {}


class TestClearConcordanceTables:
    def test_clears_single_and_composed_pipelines(self):
        pipeline = StubPipeline(
            {
                "a": {"masking_operation": mock_op("a")},
                "b": [
                    {"masking_operation": mock_op("b")},
                    {"masking_operation": mock_op("b")},
                ],
            },
            dtype=mock_dtype(),
        )
        pipeline.concordance_tables = {"a": {"x": "y"}}

        pipeline.clear_concordance_tables()

        single, composed = pipeline.col_pipelines
        single.clear_concordance_table.assert_called_once_with()
        for pipe in composed:
            pipe.clear_concordance_table.assert_called_once_with()
        assert pipeline.concordance_tables == {}


class TestColumnsOrdering:
    def test_get_data_columns_order(self, backend):
        data = frame(backend, {"b": [1], "a": [2], "c": [3]})
        order = MaskDataFramePipelineBase._get_data_columns_order(data)
        assert order == {"b": 0, "a": 1, "c": 2}

    def test_impose_ordering(self, backend):
        data = frame(backend, {"c": [3], "a": [1], "b": [2]})
        ordered = MaskDataFramePipelineBase._impose_ordering(
            data, {"a": 0, "b": 1, "c": 2}
        )
        assert list(ordered.columns) == ["a", "b", "c"]


class TestFilterData:
    def test_single_pipeline(self):
        pipeline = StubPipeline({})
        table = mock_table(COL, serving=[COL, OTHER])

        with patch.object(StubPipeline, "_filter_data") as filter_data:
            result = pipeline.filter_data(table, "data")

        filter_data.assert_called_once_with(
            "data", col_name=COL, necessary_columns=[COL, OTHER]
        )
        assert result is filter_data.return_value

    def test_composed_pipeline_uses_union_of_serving_columns(self):
        pipeline = StubPipeline({})
        tables = [mock_table(COL, [COL]), mock_table(COL, [COL, OTHER])]

        with patch.object(StubPipeline, "_filter_data") as filter_data:
            pipeline.filter_data(tables, "data")

        (data, col_name, necessary), _ = filter_data.call_args
        assert (data, col_name) == ("data", COL)
        assert sorted(necessary) == [OTHER, COL]

    def test_backend_drops_nulls_and_duplicates(self, backend):
        pipeline = make_pipeline(backend, {COL: step(make_operation(backend))})
        data = frame(backend, {COL: ["a", None, "a", "b"], OTHER: ["x", "y", "x", "z"]})

        filtered = pipeline.filter_data(pipeline.col_pipelines[0], data)

        assert list(filtered.columns) == [COL]
        assert sorted(column(filtered)) == ["a", "b"]


class TestGetUniqueValues:
    def test_backend_unique_values(self, backend):
        data = frame(backend, {COL: ["b", None, "a", "b"]})
        assert sorted(BACKENDS[backend][0]._get_unique_values(data, COL)) == ["a", "b"]

    def test_backend_empty(self, backend):
        data = frame(backend, {COL: pl.Series([], dtype=pl.Utf8).to_list()})
        assert BACKENDS[backend][0]._get_unique_values(data, COL) == []


class TestRunPipeline:
    def test_single_pipeline_returns_its_table(self):
        table = mock_table()
        table.return_value = {"a": "b"}

        result = StubPipeline({})._run_pipeline(table, "data")

        table.assert_called_once_with("data")
        assert result == {"a": "b"}

    def test_run_pipelines_dispatches_on_workers(self):
        serial = StubPipeline({}, workers=1)
        parallel = StubPipeline({}, workers=2)
        with (
            patch.object(StubPipeline, "_run_pipelines_serial") as run_serial,
            patch.object(StubPipeline, "_run_pipelines_parallel") as run_parallel,
        ):
            serial._run_pipelines("data")
            run_serial.assert_called_once_with("data")
            run_parallel.assert_not_called()

            parallel._run_pipelines("data")
            run_parallel.assert_called_once_with("data")
            assert run_serial.call_count == 1

    def test_serial_and_parallel_give_same_tables(self, backend):
        data = {"a": ["x", "y", None], "b": ["u", "v", "u"], "c": ["1", "2", "3"]}
        tables = []
        for n_workers in (1, 3):
            pipeline = make_pipeline(
                backend,
                {
                    "a": step(make_operation(backend, tag("f"), "a")),
                    "b": [
                        step(make_operation(backend, tag("f"), "b")),
                        step(make_operation(backend, tag("g"), "b")),
                    ],
                    "c": step(make_operation(backend, tag("h"), "c")),
                },
                workers=n_workers,
            )
            pipeline._run_pipelines(frame(backend, data))
            tables.append(pipeline.concordance_tables)

        assert (
            tables[0]
            == tables[1]
            == {
                "a": {"x": "f(x)", "y": "f(y)"},
                "b": {"u": "g(f(u))", "v": "g(f(v))"},
                "c": {"1": "h(1)", "2": "h(2)", "3": "h(3)"},
            }
        )


class TestPreprocessAndPostProcess:
    def test_same_output_column_is_untouched(self, backend):
        pipeline = make_pipeline(backend, {COL: step(make_operation(backend))})
        data = frame(backend, {COL: ["a"]})
        assert list(pipeline._preprocess_data(data).columns) == [COL]

    def test_new_output_column_is_copied(self, backend):
        pipeline = make_pipeline(backend, {"masked": step(make_operation(backend))})
        data = pipeline._preprocess_data(frame(backend, {COL: ["a", "b"]}))
        assert list(data.columns) == [COL, "masked"]
        assert column(data, "masked") == ["a", "b"]

    def test_output_column_used_as_serving_column_raises(self, backend):
        pipeline = make_pipeline(
            backend,
            {OTHER: step(make_operation(backend, serving_columns=[COL, OTHER]))},
        )
        with pytest.raises(ValueError, match="cannot be used"):
            pipeline._preprocess_data(frame(backend, {COL: ["a"], OTHER: ["b"]}))

    def test_post_process_swaps_masked_and_clear_columns(self, backend):
        pipeline = make_pipeline(backend, {"masked": step(make_operation(backend))})
        data = frame(backend, {COL: ["M"], "other": [1], "masked": ["clear"]})

        result = pipeline._post_process_data(data)

        assert list(result.columns) == [COL, "other", "masked"]
        assert column(result, COL) == ["clear"]
        assert column(result, "masked") == ["M"]


class TestCall:
    def test_single_operation_end_to_end(self, backend, workers):
        op = make_operation(backend)
        pipeline = make_pipeline(backend, {COL: step(op)}, workers)
        data = frame(backend, {"id": [1, 2, 3], COL: ["a", None, "a"]})

        masked = pipeline(data)

        assert list(masked.columns) == ["id", COL]
        assert column(masked) == ["f(a)", None, "f(a)"]
        assert column(masked, "id") == [1, 2, 3]
        assert pipeline.concordance_tables == {COL: {"a": "f(a)"}}

    def test_new_output_column_keeps_clear_values(self, backend, workers):
        pipeline = make_pipeline(
            backend, {"masked": step(make_operation(backend))}, workers
        )
        masked = pipeline(frame(backend, {COL: ["a", "b"], "id": [1, 2]}))

        assert list(masked.columns) == [COL, "id", "masked"]
        assert column(masked) == ["a", "b"]
        assert column(masked, "masked") == ["f(a)", "f(b)"]


# ===========================================================================
# Composition of operations
# ===========================================================================


DATA = ["a", "b", "c", "a", None, "b"]


def composed_pipeline(backend, workers, *steps, output_column: str = COL):
    return make_pipeline(backend, {output_column: list(steps)}, workers)


class TestCompositionDataFlow:
    def test_first_operation_receives_clear_values(self, backend, workers):
        op1, op2 = make_operation(backend, tag("f")), make_operation(backend, tag("g"))
        composed_pipeline(backend, workers, step(op1), step(op2))(
            frame(backend, {COL: DATA})
        )
        assert sorted(set(inputs(op1))) == ["a", "b", "c"]

    def test_second_operation_receives_first_operation_output(self, backend, workers):
        op1, op2 = make_operation(backend, tag("f")), make_operation(backend, tag("g"))
        composed_pipeline(backend, workers, step(op1), step(op2))(
            frame(backend, {COL: DATA})
        )
        assert sorted(set(inputs(op2))) == ["f(a)", "f(b)", "f(c)"]

    def test_each_operation_table_is_keyed_by_its_own_input(self, backend, workers):
        op1, op2 = make_operation(backend, tag("f")), make_operation(backend, tag("g"))
        pipeline = composed_pipeline(backend, workers, step(op1), step(op2))

        pipeline(frame(backend, {COL: DATA}))

        stage1, stage2 = pipeline.col_pipelines[0]
        assert stage1.concordance_table == {x: f"f({x})" for x in "abc"}
        assert stage2.concordance_table == {f"f({x})": f"g(f({x}))" for x in "abc"}

    def test_serving_columns_available_to_every_operation(self, backend, workers):
        op1 = make_operation(backend, tag("f"))
        op2 = make_operation(
            backend,
            lambda line, extra: f"g({line},{extra[OTHER]})",
            serving_columns=[COL, OTHER],
        )
        pipeline = composed_pipeline(backend, workers, step(op1), step(op2))

        masked = pipeline(frame(backend, {COL: ["a", "b"], OTHER: ["x", "y"]}))

        assert column(masked) == ["g(f(a),x)", "g(f(b),y)"]
        assert column(masked, OTHER) == ["x", "y"]


class TestCompositionMappings:
    def test_two_operations(self, backend, workers):
        pipeline = composed_pipeline(
            backend,
            workers,
            step(make_operation(backend, tag("f"))),
            step(make_operation(backend, tag("g"))),
        )

        masked = pipeline(frame(backend, {COL: DATA}))

        assert column(masked) == [
            "g(f(a))",
            "g(f(b))",
            "g(f(c))",
            "g(f(a))",
            None,
            "g(f(b))",
        ]
        assert pipeline.concordance_tables[COL] == {x: f"g(f({x}))" for x in "abc"}

    @pytest.mark.parametrize(
        "functions",
        [
            [str.upper],
            [str.upper, lambda s: "".join(reversed(s))],
            [lambda s: s * 2, str.upper, lambda s: f"<{s}>"],
            [lambda s: f"{s}1", lambda s: f"{s}2", lambda s: f"{s}3", str.upper],
        ],
        ids=["1-op", "2-ops", "3-ops", "4-ops"],
    )
    def test_table_is_functional_composition(self, backend, workers, functions):
        values = ["ab", "cd", "ef", "ab", "gh"]
        pipeline = composed_pipeline(
            backend,
            workers,
            *[
                step(make_operation(backend, lambda line, _=None, fn=fn: fn(line)))
                for fn in functions
            ],
        )

        masked = pipeline(frame(backend, {COL: values}))

        def compose(x):
            return reduce(lambda acc, fn: fn(acc), functions, x)

        expected = {x: compose(x) for x in set(values)}
        assert pipeline.concordance_tables[COL] == expected
        assert column(masked) == [expected[x] for x in values]

    def test_values_untouched_by_first_operation_go_through_second(
        self, backend, workers
    ):
        op1 = make_operation(backend, mapping({"a": "f(a)"}))
        op2 = make_operation(backend, tag("g"))
        pipeline = composed_pipeline(backend, workers, step(op1), step(op2))

        masked = pipeline(frame(backend, {COL: ["a", "b"]}))

        assert column(masked) == ["g(f(a))", "g(b)"]
        assert pipeline.concordance_tables[COL] == {"a": "g(f(a))", "b": "g(b)"}

    def test_intermediate_value_equal_to_untouched_clear_value(self, backend, workers):
        # op1 maps a -> b and leaves b untouched: both reach op2 as "b"
        op1 = make_operation(backend, mapping({"a": "b"}))
        op2 = make_operation(backend, tag("g"))
        pipeline = composed_pipeline(backend, workers, step(op1), step(op2))

        masked = pipeline(frame(backend, {COL: ["a", "b"]}))

        assert column(masked) == ["g(b)", "g(b)"]
        assert pipeline.concordance_tables[COL] == {"a": "g(b)", "b": "g(b)"}

    def test_first_operation_chained_mapping(self, backend, workers):
        # op1 maps a -> b and b -> c; op2 must see b and c, not chain them again
        op1 = make_operation(backend, mapping({"a": "b", "b": "c"}))
        op2 = make_operation(backend, tag("g"))
        pipeline = composed_pipeline(backend, workers, step(op1), step(op2))

        pipeline(frame(backend, {COL: ["a", "b"]}))

        assert pipeline.concordance_tables[COL] == {"a": "g(b)", "b": "g(c)"}

    def test_first_operation_swaps_values(self, backend, workers):
        op1 = make_operation(backend, mapping({"a": "b", "b": "a"}))
        op2 = make_operation(backend, tag("g"))
        pipeline = composed_pipeline(backend, workers, step(op1), step(op2))

        masked = pipeline(frame(backend, {COL: ["a", "b"]}))

        assert column(masked) == ["g(b)", "g(a)"]

    def test_first_operation_collisions(self, backend, workers):
        op1 = make_operation(backend, lambda *_: "z")
        op2 = make_operation(backend, tag("g"))
        pipeline = composed_pipeline(backend, workers, step(op1), step(op2))

        masked = pipeline(frame(backend, {COL: ["a", "b"]}))

        assert column(masked) == ["g(z)", "g(z)"]
        assert set(inputs(op2)) == {"z"}

    def test_identity_operations(self, backend, workers):
        op1 = make_operation(backend, tag("f"))
        pipeline = composed_pipeline(
            backend,
            workers,
            step(make_operation(backend, identity)),
            step(op1),
            step(make_operation(backend, identity)),
        )

        pipeline(frame(backend, {COL: ["a", "b"]}))

        assert pipeline.concordance_tables[COL] == {"a": "f(a)", "b": "f(b)"}

    def test_composition_back_to_clear_value_is_dropped(self, backend, workers):
        op1 = make_operation(backend, mapping({"a": "x"}))
        op2 = make_operation(backend, mapping({"x": "a", "b": "y"}))
        pipeline = composed_pipeline(backend, workers, step(op1), step(op2))

        masked = pipeline(frame(backend, {COL: ["a", "b"]}))

        assert column(masked) == ["a", "y"]
        assert pipeline.concordance_tables[COL] == {"b": "y"}

    def test_null_values_never_reach_operations(self, backend, workers):
        op1, op2 = make_operation(backend, tag("f")), make_operation(backend, tag("g"))
        pipeline = composed_pipeline(backend, workers, step(op1), step(op2))

        masked = pipeline(frame(backend, {COL: [None, "a", None]}))

        assert column(masked) == [None, "g(f(a))", None]
        assert None not in inputs(op1) + inputs(op2)


class TestCompositionPrescribedTables:
    def test_first_operation_prescription_flows_into_second(self, backend, workers):
        op1, op2 = make_operation(backend, tag("f")), make_operation(backend, tag("g"))
        pipeline = composed_pipeline(
            backend, workers, step(op1, {"a": "P1"}), step(op2)
        )

        masked = pipeline(frame(backend, {COL: ["a", "b"]}))

        assert column(masked) == ["g(P1)", "g(f(b))"]
        assert "a" not in inputs(op1)
        assert sorted(set(inputs(op2))) == ["P1", "f(b)"]

    def test_second_operation_prescription_on_intermediate_value(
        self, backend, workers
    ):
        op1, op2 = make_operation(backend, tag("f")), make_operation(backend, tag("g"))
        pipeline = composed_pipeline(
            backend, workers, step(op1), step(op2, {"f(a)": "P2"})
        )

        masked = pipeline(frame(backend, {COL: ["a", "b"]}))

        assert column(masked) == ["P2", "g(f(b))"]
        assert pipeline.concordance_tables[COL] == {"a": "P2", "b": "g(f(b))"}
        assert "f(a)" not in inputs(op2)

    def test_second_operation_prescription_on_clear_value_is_ignored(
        self, backend, workers
    ):
        # op2 never sees "a" (it receives f(a)), so this entry must not apply
        op1, op2 = make_operation(backend, tag("f")), make_operation(backend, tag("g"))
        pipeline = composed_pipeline(
            backend, workers, step(op1), step(op2, {"a": "P2"})
        )

        masked = pipeline(frame(backend, {COL: ["a", "b"]}))

        assert column(masked) == ["g(f(a))", "g(f(b))"]
        assert pipeline.concordance_tables[COL] == {"a": "g(f(a))", "b": "g(f(b))"}

    def test_prescriptions_on_both_operations(self, backend, workers):
        op1, op2 = make_operation(backend, tag("f")), make_operation(backend, tag("g"))
        pipeline = composed_pipeline(
            backend,
            workers,
            step(op1, {"a": "P1", "b": "P1b"}),
            step(op2, {"P1": "P2", "f(c)": "P2c"}),
        )

        masked = pipeline(frame(backend, {COL: ["a", "b", "c", "d"]}))

        assert column(masked) == ["P2", "g(P1b)", "P2c", "g(f(d))"]
        assert sorted(set(inputs(op1))) == ["c", "d"]
        assert sorted(set(inputs(op2))) == ["P1b", "f(d)"]

    def test_prescribed_entries_absent_from_data_are_not_composed(
        self, backend, workers
    ):
        op1, op2 = make_operation(backend, tag("f")), make_operation(backend, tag("g"))
        pipeline = composed_pipeline(
            backend, workers, step(op1, {"unseen": "P1"}), step(op2, {"other": "P2"})
        )

        pipeline(frame(backend, {COL: ["a"]}))

        assert pipeline.concordance_tables[COL] == {"a": "g(f(a))"}

    def test_fully_prescribed_chain_never_runs_masking(self, backend, workers):
        op1, op2 = make_operation(backend, tag("f")), make_operation(backend, tag("g"))
        pipeline = composed_pipeline(
            backend,
            workers,
            step(op1, {"a": "x", "b": "y"}),
            step(op2, {"x": "X", "y": "Y"}),
        )

        masked = pipeline(frame(backend, {COL: ["a", "b", "a"]}))

        assert column(masked) == ["X", "Y", "X"]
        op1.mask_line.assert_not_called()
        op2.mask_line.assert_not_called()

    def test_operation_own_table_is_overridden_by_prescription(self, backend, workers):
        op1 = make_operation(backend, tag("f"), concordance_table={"a": "OWN"})
        op2 = make_operation(backend, tag("g"))
        pipeline = composed_pipeline(
            backend, workers, step(op1, {"a": "P1"}), step(op2)
        )

        masked = pipeline(frame(backend, {COL: ["a"]}))

        assert column(masked) == ["g(P1)"]


class TestCompositionAcrossRuns:
    def test_tables_accumulate_and_stay_consistent(self, backend, workers):
        op1, op2 = make_operation(backend, tag("f")), make_operation(backend, tag("g"))
        pipeline = composed_pipeline(backend, workers, step(op1), step(op2))

        first = pipeline(frame(backend, {COL: ["a"]}))
        second = pipeline(frame(backend, {COL: ["a", "b"]}))

        assert column(first) == ["g(f(a))"]
        assert column(second) == ["g(f(a))", "g(f(b))"]
        assert pipeline.concordance_tables[COL] == {"a": "g(f(a))", "b": "g(f(b))"}
        # Previously masked values are reused, not masked again
        assert inputs(op1).count("a") == 1
        assert inputs(op2).count("f(a)") == 1

    def test_clear_concordance_tables_restarts_composition(self, backend, workers):
        op1, op2 = make_operation(backend, tag("f")), make_operation(backend, tag("g"))
        pipeline = composed_pipeline(
            backend, workers, step(op1, {"a": "P1"}), step(op2)
        )

        pipeline(frame(backend, {COL: ["a"]}))
        pipeline.clear_concordance_tables()

        assert pipeline.concordance_tables == {}
        assert all(p.concordance_table == {} for p in pipeline.col_pipelines[0])

        masked = pipeline(frame(backend, {COL: ["b"]}))

        assert pipeline.concordance_tables == {COL: {"b": "g(f(b))"}}
        assert column(masked) == ["g(f(b))"]

    def test_clear_concordance_tables_forgets_prescriptions(self, backend, workers):
        op1, op2 = make_operation(backend, tag("f")), make_operation(backend, tag("g"))
        pipeline = composed_pipeline(
            backend, workers, step(op1, {"a": "P1"}), step(op2)
        )

        pipeline(frame(backend, {COL: ["a"]}))
        pipeline.clear_concordance_tables()

        assert column(pipeline(frame(backend, {COL: ["a"]}))) == ["g(f(a))"]
        assert inputs(op1) == ["a"]

    def test_clear_keeps_operations_own_tables(self, backend, workers):
        op1 = make_operation(backend, tag("f"), concordance_table={"b": "OWN"})
        op2 = make_operation(backend, tag("g"))
        pipeline = composed_pipeline(
            backend, workers, step(op1, {"a": "P1"}), step(op2)
        )

        pipeline(frame(backend, {COL: ["a", "b"]}))
        pipeline.clear_concordance_tables()
        masked = pipeline(frame(backend, {COL: ["a", "b"]}))

        assert column(masked) == ["g(f(a))", "g(OWN)"]
        assert inputs(op1) == ["a"]


class TestCompositionInPipeline:
    def test_composed_and_single_columns_are_independent(self, backend, workers):
        pipeline = make_pipeline(
            backend,
            {
                COL: [
                    step(make_operation(backend, tag("f"))),
                    step(make_operation(backend, tag("g"))),
                ],
                OTHER: step(make_operation(backend, tag("h"), OTHER)),
            },
            workers,
        )

        masked = pipeline(frame(backend, {COL: ["a", "b"], OTHER: ["a", "x"]}))

        assert column(masked) == ["g(f(a))", "g(f(b))"]
        assert column(masked, OTHER) == ["h(a)", "h(x)"]
        assert pipeline.concordance_tables == {
            COL: {"a": "g(f(a))", "b": "g(f(b))"},
            OTHER: {"a": "h(a)", "x": "h(x)"},
        }

    def test_composed_pipeline_into_new_output_column(self, backend, workers):
        pipeline = composed_pipeline(
            backend,
            workers,
            step(make_operation(backend, tag("f"))),
            step(make_operation(backend, tag("g"))),
            output_column="masked",
        )

        masked = pipeline(frame(backend, {COL: ["a", "b"]}))

        assert list(masked.columns) == [COL, "masked"]
        assert column(masked) == ["a", "b"]
        assert column(masked, "masked") == ["g(f(a))", "g(f(b))"]

    def test_run_pipeline_returns_composition_directly(self, backend):
        pipeline = composed_pipeline(
            backend,
            1,
            step(make_operation(backend, tag("f"))),
            step(make_operation(backend, tag("g"))),
        )
        composed = pipeline.col_pipelines[0]
        col_data = pipeline.filter_data(composed, frame(backend, {COL: ["a", "b"]}))

        assert pipeline._run_pipeline(composed, col_data) == {
            "a": "g(f(a))",
            "b": "g(f(b))",
        }

    def test_composition_uses_concordance_table_class(self, backend):
        pipeline = composed_pipeline(
            backend,
            1,
            step(make_operation(backend, tag("f"))),
            step(make_operation(backend, tag("g"))),
        )
        assert all(
            isinstance(p, ConcordanceTableBase) for p in pipeline.col_pipelines[0]
        )
        if backend == "pandas":
            assert all(
                isinstance(p, PandasConcordanceTable) for p in pipeline.col_pipelines[0]
            )
