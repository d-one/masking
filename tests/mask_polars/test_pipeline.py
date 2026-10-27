import datetime as dt
import hashlib
from decimal import Decimal

import polars as pl
import pytest
from masking.mask_polars.operations.operation_hash import HashOperation
from masking.mask_polars.operations.operation_med_stats import MedStatsOperation
from masking.mask_polars.operations.operation_null import NullOperation
from masking.mask_polars.pipeline import ConcordanceTable, MaskDataFramePipeline


def sha256(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def table(col: str = "c") -> ConcordanceTable:
    return ConcordanceTable(masking_operation=HashOperation(col_name=col))


class TestConcordanceTable:
    def test_build_keeps_only_changed_values(self):
        concordance = ConcordanceTable(
            masking_operation=MedStatsOperation(col_name="p")
        )
        data = pl.DataFrame({"p": ["1000", "0000"]})
        assert concordance._build_concordance_table(data) == {"1000": "VD02"}

    def test_build_returns_dict(self):
        data = pl.DataFrame({"c": ["a", "b"]})
        assert table()._build_concordance_table(data) == {
            "a": sha256("a"),
            "b": sha256("b"),
        }


class TestStaticMethods:
    def test_get_unique_values_keeps_order(self):
        data = pl.DataFrame({"c": ["b", None, "a", "b"]})
        assert MaskDataFramePipeline._get_unique_values(data, "c") == ["b", "a"]

    def test_substitute_masked_values(self):
        data = pl.DataFrame({"a": ["x", "y", None], "b": ["x", "x", "x"]})
        out = MaskDataFramePipeline._substitute_masked_values(
            data, [table("a"), [table("b"), table("b")]], {"a": {"x": "X"}, "b": {}}
        )
        assert out["a"].to_list() == ["X", "y", None]
        assert out["b"].to_list() == ["x", "x", "x"]

    def test_substitute_keys_keep_column_type(self):
        data = pl.DataFrame({"a": [1, 2, None]})
        out = MaskDataFramePipeline._substitute_masked_values(
            data, [table("a")], {"a": {1: "one"}}
        )
        assert out["a"].dtype == pl.Utf8
        assert out["a"].to_list() == ["one", "2", None]

    def test_copy_column(self):
        out = MaskDataFramePipeline._copy_column(pl.DataFrame({"a": [1, 2]}), "a", "b")
        assert out.to_dict(as_series=False) == {"a": [1, 2], "b": [1, 2]}

    def test_rename_columns_can_swap(self):
        data = pl.DataFrame({"a": [1], "b": [2]})
        out = MaskDataFramePipeline._rename_columns(data, {"a": "b", "b": "a"})
        assert out.to_dict(as_series=False) == {"b": [1], "a": [2]}


class TestPipeline:
    def test_null_operation_nulls_the_column(self):
        data = pl.DataFrame({"c": ["a", None, "b"], "o": [1, 2, 3]})
        pipeline = MaskDataFramePipeline({
            "c": {"masking_operation": NullOperation(col_name="c")}
        })
        out = pipeline(data)
        assert out["c"].to_list() == [None, None, None]
        assert out["o"].to_list() == [1, 2, 3]
        assert pipeline.concordance_tables == {"c": {}}

    def test_null_operation_concordance_table_is_empty(self):
        table = ConcordanceTable(masking_operation=NullOperation(col_name="c"))
        assert table(pl.DataFrame({"c": ["a"]})) == {}

    def test_null_operation_in_new_column_keeps_input(self):
        data = pl.DataFrame({"c": ["a", "b"]})
        pipeline = MaskDataFramePipeline({
            "out": {
                "masking_operation": NullOperation(
                    col_name="c", condition=lambda v: v == "a"
                )
            }
        })
        for _ in range(2):
            out = pipeline(data)
            assert out["c"].to_list() == ["a", "b"]
            assert out["out"].to_list() == [None, "b"]

    @pytest.mark.parametrize(
        "column",
        [
            pl.Series(
                [dt.datetime(2024, 1, 1, 12), dt.datetime(2000, 1, 1)],  # noqa: DTZ001
                dtype=pl.Datetime("ns"),
            ),
            pl.Series([Decimal("1.10"), Decimal("2.00")], dtype=pl.Decimal(10, 2)),
        ],
        ids=["datetime_ns", "decimal"],
    )
    def test_conditional_null_matches_the_column_type(self, column: pl.Series):
        selected = str(column[0])
        pipeline = MaskDataFramePipeline({
            "c": {
                "masking_operation": NullOperation(
                    col_name="c", condition=lambda v: v == selected
                )
            }
        })
        out = pipeline(pl.DataFrame({"c": column}))
        assert out["c"].to_list() == [None, column[1]]
        assert out["c"].dtype == column.dtype

    def test_end_to_end(self):
        data = pl.DataFrame({"p": ["1000", "0000", None, "1000"], "o": [1, 2, 3, 4]})
        pipeline = MaskDataFramePipeline({
            "p": {"masking_operation": MedStatsOperation(col_name="p")}
        })
        out = pipeline(data)
        assert out["p"].to_list() == ["VD02", "0000", None, "VD02"]
        assert out["o"].to_list() == [1, 2, 3, 4]
        assert pipeline.concordance_tables == {"p": {"1000": "VD02"}}

    def test_output_in_new_column(self):
        data = pl.DataFrame({"p": ["1000", "0000"]})
        pipeline = MaskDataFramePipeline({
            "region": {"masking_operation": MedStatsOperation(col_name="p")}
        })
        out = pipeline(data)
        assert out["p"].to_list() == ["1000", "0000"]
        assert out["region"].to_list() == ["VD02", "0000"]

    @pytest.mark.parametrize("workers", [1, 2])
    def test_multiple_columns(self, workers):
        data = pl.DataFrame({"a": ["x", "y"], "b": ["1000", "1000"]})
        pipeline = MaskDataFramePipeline(
            {
                "a": {"masking_operation": HashOperation(col_name="a")},
                "b": {"masking_operation": MedStatsOperation(col_name="b")},
            },
            workers=workers,
        )
        assert pipeline(data).to_dict(as_series=False) == {
            "a": [sha256("x"), sha256("y")],
            "b": ["VD02", "VD02"],
        }

    def test_prescribed_concordance_table(self):
        pipeline = MaskDataFramePipeline({
            "a": {
                "masking_operation": HashOperation(col_name="a"),
                "concordance_table": {"x": "X"},
            }
        })
        out = pipeline(pl.DataFrame({"a": ["x", "y"]}))
        assert out["a"].to_list() == ["X", sha256("y")]

    def test_integer_column(self):
        pipeline = MaskDataFramePipeline({
            "p": {"masking_operation": MedStatsOperation(col_name="p")}
        })
        out = pipeline(pl.DataFrame({"p": [1000, 0, None]}))
        assert out["p"].to_list() == ["VD02", "0", None]
        assert pipeline.concordance_tables == {"p": {1000: "VD02"}}
