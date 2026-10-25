import hashlib

import pandas as pd
import pytest
from masking.mask.operations.operation_hash import HashOperation
from masking.mask.operations.operation_med_stats import MedStatsOperation
from masking.mask.pipeline import ConcordanceTable, MaskDataFramePipeline


def sha256(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def values(column: pd.Series) -> list:
    return [None if pd.isna(v) else v for v in column]


def table(col: str = "c") -> ConcordanceTable:
    return ConcordanceTable(masking_operation=HashOperation(col_name=col))


class TestConcordanceTable:
    def test_build_keeps_only_changed_values(self):
        concordance = ConcordanceTable(
            masking_operation=MedStatsOperation(col_name="p")
        )
        data = pd.DataFrame({"p": ["1000", "0000"]})
        assert concordance._build_concordance_table(data) == {"1000": "VD02"}

    def test_build_returns_dict(self):
        data = pd.DataFrame({"c": ["a", "b"]})
        assert table()._build_concordance_table(data) == {
            "a": sha256("a"),
            "b": sha256("b"),
        }


class TestStaticMethods:
    def test_substitute_masked_values(self):
        data = pd.DataFrame({"a": ["x", "y", None], "b": ["x", "x", "x"]})
        out = MaskDataFramePipeline._substitute_masked_values(
            data, [table("a"), [table("b"), table("b")]], {"a": {"x": "X"}, "b": {}}
        )
        assert values(out["a"]) == ["X", "y", None]
        assert out["b"].tolist() == ["x", "x", "x"]

    def test_substitute_composed_pipeline_uses_its_column(self):
        data = pd.DataFrame({"b": ["x", "y"]})
        out = MaskDataFramePipeline._substitute_masked_values(
            data, [[table("b"), table("b")]], {"b": {"y": "Y"}}
        )
        assert out["b"].tolist() == ["x", "Y"]

    def test_copy_column(self):
        out = MaskDataFramePipeline._copy_column(pd.DataFrame({"a": [1, 2]}), "a", "b")
        assert out.to_dict("list") == {"a": [1, 2], "b": [1, 2]}

    def test_rename_columns_can_swap(self):
        data = pd.DataFrame({"a": [1], "b": [2]})
        out = MaskDataFramePipeline._rename_columns(data, {"a": "b", "b": "a"})
        assert out.to_dict("list") == {"b": [1], "a": [2]}


class TestPipeline:
    def test_end_to_end(self):
        data = pd.DataFrame({"p": ["1000", "0000", None, "1000"], "o": [1, 2, 3, 4]})
        pipeline = MaskDataFramePipeline({
            "p": {"masking_operation": MedStatsOperation(col_name="p")}
        })
        out = pipeline(data)
        assert values(out["p"]) == ["VD02", "0000", None, "VD02"]
        assert out["o"].tolist() == [1, 2, 3, 4]
        assert pipeline.concordance_tables == {"p": {"1000": "VD02"}}

    def test_output_in_new_column(self):
        data = pd.DataFrame({"p": ["1000", "0000"]})
        pipeline = MaskDataFramePipeline({
            "region": {"masking_operation": MedStatsOperation(col_name="p")}
        })
        out = pipeline(data)
        assert out["p"].tolist() == ["1000", "0000"]
        assert out["region"].tolist() == ["VD02", "0000"]

    @pytest.mark.parametrize("workers", [1, 2])
    def test_multiple_columns(self, workers):
        data = pd.DataFrame({"a": ["x", "y"], "b": ["1000", "1000"]})
        pipeline = MaskDataFramePipeline(
            {
                "a": {"masking_operation": HashOperation(col_name="a")},
                "b": {"masking_operation": MedStatsOperation(col_name="b")},
            },
            workers=workers,
        )
        out = pipeline(data)
        assert out.to_dict("list") == {
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
        out = pipeline(pd.DataFrame({"a": ["x", "y"]}))
        assert out["a"].tolist() == ["X", sha256("y")]
