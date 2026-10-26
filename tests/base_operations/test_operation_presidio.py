import pytest
from masking.base_operations.operation_presidio import MaskPresidioBase
from presidio_analyzer import RecognizerResult

EMAIL = "john.doe@example.com"
TEXT = f"Mail {EMAIL} now"


class MaskPresidio(MaskPresidioBase):
    def _mask_data(self, data):
        return data


@pytest.fixture()
def make(blank_analyzer):
    def _make(**kwargs) -> MaskPresidio:
        return MaskPresidio(col_name="t", analyzer=blank_analyzer, **kwargs)

    return _make


def test_is_abstract(blank_analyzer):
    with pytest.raises(TypeError):
        MaskPresidioBase(col_name="t", analyzer=blank_analyzer)


def test_detected_entity_is_replaced(make):
    assert make()._mask_line(TEXT) == "Mail <EMAIL_ADDRESS> now"


def test_text_without_entities_is_unchanged(make):
    assert make()._mask_line("nothing to see") == "nothing to see"


def test_masking_function(make):
    assert make(masking_function=lambda s: "#" * len(s))._mask_line(TEXT) == (
        f"Mail {'#' * len(EMAIL)} now"
    )


def test_allow_list(make):
    assert make(allow_list=[EMAIL])._mask_line(TEXT) == TEXT


def test_pii_entities_restrict_detection(make):
    assert make(pii_entities=["PERSON"])._mask_line(TEXT) == TEXT


def test_given_entities_skip_detection(make):
    op = make()
    op.analyzer = None
    result = RecognizerResult("PERSON", start=0, end=4, score=1.0)
    assert op._mask_line(TEXT, entities=[result]) == f"<PERSON> {EMAIL} now"
    assert op._mask_line(TEXT, entities=[]) == TEXT


def test_check_mask_line_forwards_entities(make):
    op = make()
    assert op._check_mask_line(TEXT, entities=[]) == TEXT
