import json

import pytest
from masking.base_operations.operation_dict import DictOperationBase


def stars(value: str) -> str:
    return "*" * len(value)


class DictOperation(DictOperationBase):
    """Dict operation replacing every leaf to mask with '<M>'."""

    masking_function = staticmethod(stars)

    def _handle_masking_paths(self, line, leaf_to_mask, **kwargs):
        self.handled_kwargs = kwargs
        for leaf in list(leaf_to_mask):
            self._set_leaf(line, leaf, "<M>")
        return line

    def _mask_data(self, data):
        return data


def mask(op: DictOperation, line) -> object:
    return json.loads(op._mask_line(line))


def test_is_abstract():
    with pytest.raises(TypeError):
        DictOperationBase(col_name="c")


def test_json_string_in_json_string_out():
    out = DictOperation(col_name="c")._mask_line('{"a": "x"}')
    assert isinstance(out, str)
    assert json.loads(out) == {"a": "<M>"}


def test_dict_input_is_dumped():
    assert mask(DictOperation(col_name="c"), {"a": "x", "b": {"c": 1}}) == {
        "a": "<M>",
        "b": {"c": "<M>"},
    }


def test_list_input():
    assert mask(DictOperation(col_name="c"), '["x", {"a": 1}]') == ["<M>", {"a": "<M>"}]


def test_denied_leaf_is_masked_with_masking_function():
    op = DictOperation(col_name="c", deny_keys=["secret"])
    assert mask(op, '{"a": "x", "secret": "abc"}') == {"a": "<M>", "secret": "***"}


def test_denied_subtree_is_masked_as_json():
    op = DictOperation(col_name="c", deny_keys=["secret"])
    subtree = {"z": "ab"}
    assert mask(op, {"secret": subtree}) == {"secret": stars(json.dumps(subtree))}


def test_allowed_keys_are_kept():
    op = DictOperation(col_name="c", allow_keys=["a"])
    assert mask(op, '{"a": "x", "b": "y"}') == {"a": "x", "b": "<M>"}


def test_non_ascii_is_kept():
    op = DictOperation(col_name="c", allow_keys=["a"])
    assert op._mask_line('{"a": "Zürich"}') == '{"a": "Zürich"}'


def test_explicit_paths_are_used():
    op = DictOperation(col_name="c")
    out = op._mask_line(
        '{"a": "x", "b": "y", "s": "abc"}',
        leaf_to_mask=iter(["a"]),
        leaf_to_deny=iter(["s"]),
    )
    assert json.loads(out) == {"a": "<M>", "b": "y", "s": "***"}


def test_kwargs_are_forwarded_to_handle_masking_paths():
    op = DictOperation(col_name="c")
    op._check_mask_line('{"a": "x"}', additional_values={"k": "v"})
    assert op.handled_kwargs == {"additional_values": {"k": "v"}}
