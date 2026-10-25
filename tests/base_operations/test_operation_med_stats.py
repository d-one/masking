from unittest.mock import patch

import pytest
from masking.base_operations.operation_med_stats import MedStatsOperationBase


class MedStatsOperation(MedStatsOperationBase):
    def _mask_data(self, data):
        return data


@pytest.fixture(scope="module")
def op() -> MedStatsOperation:
    return MedStatsOperation(col_name="plz")


def test_lookup_table_maps_strings_to_strings(op):
    table = op._MEDSTATS_LOOKUP_TABLE
    assert len(table) > 1000
    assert all(isinstance(k, str) and isinstance(v, str) for k, v in table.items())


@pytest.mark.parametrize("line", ["1000", 1000, " 1000 "])
def test_known_plz_is_mapped_to_region(op, line):
    assert op._mask_line(line) == "VD02"


@pytest.mark.parametrize("line", ["0000", "abc", ""])
def test_unknown_plz_is_returned_unchanged(op, line):
    assert op._mask_line(line) == line


def test_non_unique_values_allowed(op):
    assert op._needs_unique_values is False


@pytest.mark.xfail(
    strict=True,
    reason="Bug: the lookup table is cached on the instance, so every instance re-reads the file.",
)
def test_lookup_table_is_loaded_once():
    with patch.object(
        MedStatsOperationBase, "_get_medstats_lookup_table", return_value={"1": "A"}
    ) as load:
        MedStatsOperation(col_name="a")
        MedStatsOperation(col_name="b")
    assert load.call_count == 1
