import pytest
from masking.base_operations.operation_string_match import StringMatchOperationBase

TEXT = "JOHN met Doe, born 31.01.2000, see Johnny"


class StringMatch(StringMatchOperationBase):
    def _mask_data(self, data):
        return data


def make(**kwargs) -> StringMatch:
    return StringMatch(col_name="t", pii_cols=["first", "last", "dob"], **kwargs)


VALUES = {"first": "John", "last": "Doe", "dob": "2000-01-31"}


def test_serving_columns_include_pii_columns():
    assert make().serving_columns == ["t", "first", "last", "dob"]
    assert StringMatch(col_name="t").serving_columns == ["t"]


def test_pii_values_are_masked_case_insensitively_as_whole_words():
    assert make()._mask_line(TEXT, additional_values=VALUES) == (
        "<PATIENT_DATA> met <PATIENT_DATA>, born <PATIENT_DATA>, see Johnny"
    )


# Presidio's anonymizer merges entities of the same type separated by whitespace.
def test_adjacent_matches_are_merged():
    assert make()._mask_line("John Doe", additional_values=VALUES) == "<PATIENT_DATA>"


@pytest.mark.parametrize(
    "written", ["31.01.2000", "2000-01-31", "31-01-2000", "31/01/2000", "2000/01/31"]
)
def test_dates_are_matched_in_several_formats(written):
    op = StringMatch(col_name="t", pii_cols=["dob"])
    assert op._mask_line(
        f"born {written}.", additional_values={"dob": "2000-01-31"}
    ) == ("born <PATIENT_DATA>.")


def test_no_match_is_unchanged():
    assert make()._mask_line("nothing", additional_values=VALUES) == "nothing"


def test_without_pii_columns_is_unchanged():
    assert StringMatch(col_name="t")._mask_line(TEXT) == TEXT


def test_allow_list():
    op = make(allow_list=["John"])
    assert op._mask_line("John and Doe", additional_values=VALUES) == (
        "John and <PATIENT_DATA>"
    )


def test_masking_function():
    op = make(masking_function=lambda s: "#" * len(s))
    assert op._mask_line("John and Doe", additional_values=VALUES) == "#### and ###"


def test_check_mask_line_forwards_additional_values():
    op = make()
    assert op._check_mask_line("Doe", additional_values=VALUES) == "<PATIENT_DATA>"
