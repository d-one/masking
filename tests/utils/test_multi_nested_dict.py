import math

import pytest
from masking.utils.multi_nested_dict import MultiNestedDictHandler

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SEP = ".[$]."


@pytest.fixture()
def handler():
    return MultiNestedDictHandler()


@pytest.fixture()
def handler_with_sep(path_separator):
    return MultiNestedDictHandler(path_separator=path_separator)


# ---------------------------------------------------------------------------
# __init__ / defaults
# ---------------------------------------------------------------------------


class TestInit:
    def test_default_path_separator(self, handler):
        assert handler.path_separator == ".[$]."

    def test_custom_path_separator(self):
        h = MultiNestedDictHandler(path_separator="~")
        assert h.path_separator == "~"

    def test_default_list_index(self, handler):
        assert handler.list_index == {"start": "[", "end": "]"}

    def test_default_deny_tag(self, handler):
        assert handler.deny_tag == {"start": "deny<", "end": ">"}

    def test_default_case_sensitive_false(self, handler):
        assert handler.case_sensitive is False

    def test_case_sensitive_true(self):
        h = MultiNestedDictHandler(case_sensitive=True)
        assert h.case_sensitive is True

    def test_allow_keys_stored_lowercase_when_case_insensitive(self):
        h = MultiNestedDictHandler(allow_keys=["FooBar"], case_sensitive=False)
        assert h.allow_keys == ["foobar"]

    def test_allow_keys_preserved_when_case_sensitive(self):
        h = MultiNestedDictHandler(allow_keys=["FooBar"], case_sensitive=True)
        assert h.allow_keys == ["FooBar"]

    def test_deny_keys_stored(self):
        h = MultiNestedDictHandler(deny_keys=["secret"])
        assert h.deny_keys == ["secret"]

    def test_callable_allow_keys_stored(self):
        fn = lambda k: k == "x"  # noqa: E731
        h = MultiNestedDictHandler(allow_keys=[fn])
        assert h.allow_keys == []
        assert h.allow_keys_callable == [fn]

    def test_callable_deny_keys_stored(self):
        fn = lambda k: k == "x"  # noqa: E731
        h = MultiNestedDictHandler(deny_keys=[fn])
        assert h.deny_keys == []
        assert h.deny_keys_callable == [fn]


# ---------------------------------------------------------------------------
# _LEAF_TYPE
# ---------------------------------------------------------------------------


class TestLeafType:
    def test_default_leaf_type(self):
        assert (str, int, float, bool) == MultiNestedDictHandler._LEAF_TYPE

    def test_str_is_leaf(self, handler):
        paths = list(handler._find_leaf_path("hello", parent="k"))
        assert paths == ["k"]

    def test_int_is_leaf(self, handler):
        paths = list(handler._find_leaf_path(42, parent="k"))
        assert paths == ["k"]

    def test_float_is_leaf(self, handler):
        paths = list(handler._find_leaf_path(math.pi, parent="k"))
        assert paths == ["k"]

    def test_bool_is_leaf(self, handler):
        paths = list(handler._find_leaf_path(True, parent="k"))
        assert paths == ["k"]

    def test_none_is_leaf(self, handler):
        paths = list(handler._find_leaf_path(None, parent="k"))
        assert paths == ["k"]

    def test_unsupported_type_raises(self, handler):
        with pytest.raises(TypeError, match="not supported"):
            list(handler._find_leaf_path(object(), parent="k"))

    def test_custom_leaf_type_subclass(self):
        class CustomHandler(MultiNestedDictHandler):
            _LEAF_TYPE = str

        h = CustomHandler()
        # int should now raise TypeError since only str is a leaf
        with pytest.raises(TypeError, match="not supported"):
            list(h._find_leaf_path(42, parent="k"))


# ---------------------------------------------------------------------------
# _parse_line / _dump_line
# ---------------------------------------------------------------------------


class TestParseDump:
    def test_parse_none_returns_empty_string(self):
        assert MultiNestedDictHandler._parse_line(None) == ""

    def test_parse_dict_returns_same(self):
        d = {"a": 1}
        assert MultiNestedDictHandler._parse_line(d) is d

    def test_parse_list_returns_same(self):
        lst = [1, 2]
        assert MultiNestedDictHandler._parse_line(lst) is lst

    def test_parse_json_string(self):
        assert MultiNestedDictHandler._parse_line('{"a": 1}') == {"a": 1}

    def test_parse_plain_string_returns_string(self):
        assert MultiNestedDictHandler._parse_line("hello") == "hello"

    def test_dump_dict(self):
        assert MultiNestedDictHandler._dump_line({"a": 1}) == '{"a": 1}'

    def test_dump_list(self):
        assert MultiNestedDictHandler._dump_line([1, 2]) == "[1, 2]"

    def test_dump_string(self):
        assert MultiNestedDictHandler._dump_line("hello") == "hello"

    def test_dump_unsupported_raises(self):
        with pytest.raises(TypeError, match="Unsupported type"):
            MultiNestedDictHandler._dump_line(42)


# ---------------------------------------------------------------------------
# deny path helpers
# ---------------------------------------------------------------------------


class TestDenyPath:
    def test_deny_path_wraps(self, handler):
        assert handler._deny_path("a") == "deny<a>"

    def test_is_denied_path_true(self, handler):
        assert handler._is_denied_path("deny<a>") is True

    def test_is_denied_path_false(self, handler):
        assert handler._is_denied_path("a") is False

    def test_undeny_path(self, handler):
        assert handler._undeny_path("deny<a>") == "a"


# ---------------------------------------------------------------------------
# list index helpers
# ---------------------------------------------------------------------------


class TestListIndex:
    def test_is_list_index_true(self, handler):
        assert handler._is_list_index("[0]") is True
        assert handler._is_list_index("[123]") is True

    def test_is_list_index_false_non_numeric(self, handler):
        assert handler._is_list_index("[abc]") is False

    def test_is_list_index_false_no_brackets(self, handler):
        assert handler._is_list_index("0") is False

    def test_get_list_index(self, handler):
        assert handler._get_list_index("[5]") == "5"

    def test_list_index_to_path(self, handler):
        assert handler._list_index_to_path(3) == "[3]"

    def test_get_index_numeric(self, handler):
        assert handler._get_index("[7]") == 7

    def test_get_index_string(self, handler):
        assert handler._get_index("key") == "key"


# ---------------------------------------------------------------------------
# _find_leaf_path
# ---------------------------------------------------------------------------


class TestFindLeafPath:
    def test_flat_dict(self, handler):
        data = {"a": "v1", "b": "v2"}
        paths = list(handler._find_leaf_path(data))
        assert paths == ["a", "b"]

    def test_nested_dict(self, handler):
        data = {"a": {"b": "v"}}
        paths = list(handler._find_leaf_path(data))
        assert paths == [f"a{SEP}b"]

    def test_list_values(self, handler):
        data = {"a": ["x", "y"]}
        paths = list(handler._find_leaf_path(data))
        assert paths == [f"a{SEP}[0]", f"a{SEP}[1]"]

    def test_none_value_yields_path(self, handler):
        data = {"a": None}
        paths = list(handler._find_leaf_path(data))
        assert paths == ["a"]

    def test_int_value_yields_path(self, handler):
        data = {"a": 42}
        paths = list(handler._find_leaf_path(data))
        assert paths == ["a"]

    def test_bool_value_yields_path(self, handler):
        data = {"a": True}
        paths = list(handler._find_leaf_path(data))
        assert paths == ["a"]

    def test_deeply_nested(self, handler):
        data = {"a": {"b": {"c": {"d": "val"}}}}
        paths = list(handler._find_leaf_path(data))
        assert paths == [SEP.join(["a", "b", "c", "d"])]

    def test_mixed_list_and_dict(self, handler):
        data = {"items": [{"name": "Alice"}, {"name": "Bob"}]}
        paths = list(handler._find_leaf_path(data))
        assert paths == [f"items{SEP}[0]{SEP}name", f"items{SEP}[1]{SEP}name"]

    def test_top_level_list(self, handler):
        data = ["a", "b"]
        paths = list(handler._find_leaf_path(data))
        assert paths == ["[0]", "[1]"]

    def test_top_level_string(self, handler):
        paths = list(handler._find_leaf_path("hello"))
        assert paths == [None]

    def test_custom_separator(self):
        h = MultiNestedDictHandler(path_separator="~")
        data = {"a": {"b": "v"}}
        paths = list(h._find_leaf_path(data))
        assert paths == ["a~b"]

    def test_empty_dict(self, handler):
        assert list(handler._find_leaf_path({})) == []

    def test_empty_list(self, handler):
        assert list(handler._find_leaf_path([])) == []


# ---------------------------------------------------------------------------
# _find_leaf_path_and_value
# ---------------------------------------------------------------------------


class TestFindLeafPathAndValue:
    def test_flat_dict(self, handler):
        data = {"a": "v1", "b": "v2"}
        result = list(handler._find_leaf_path_and_value(data))
        assert result == [("a", "v1"), ("b", "v2")]

    def test_nested_dict(self, handler):
        data = {"a": {"b": "val"}}
        result = list(handler._find_leaf_path_and_value(data))
        assert result == [(f"a{SEP}b", "val")]

    def test_none_value_yields_empty_string(self, handler):
        data = {"a": None}
        result = list(handler._find_leaf_path_and_value(data))
        assert result == [("a", "")]

    def test_int_value(self, handler):
        data = {"a": 42}
        result = list(handler._find_leaf_path_and_value(data))
        assert result == [("a", 42)]

    def test_float_value(self, handler):
        data = {"a": math.pi}
        result = list(handler._find_leaf_path_and_value(data))
        assert result == [("a", math.pi)]

    def test_bool_value(self, handler):
        data = {"a": False}
        result = list(handler._find_leaf_path_and_value(data))
        assert result == [("a", False)]

    def test_list_values(self, handler):
        data = {"a": ["x", "y"]}
        result = list(handler._find_leaf_path_and_value(data))
        assert result == [(f"a{SEP}[0]", "x"), (f"a{SEP}[1]", "y")]

    def test_top_level_string(self, handler):
        result = list(handler._find_leaf_path_and_value("hello"))
        assert result == [(None, "hello")]

    def test_complex_nested(self, handler):
        data = {
            "name": "Alice",
            "address": {"city": "Zurich", "zip": 8000},
            "tags": ["a", "b"],
        }
        result = list(handler._find_leaf_path_and_value(data))
        assert result == [
            ("name", "Alice"),
            (f"address{SEP}city", "Zurich"),
            (f"address{SEP}zip", 8000),
            (f"tags{SEP}[0]", "a"),
            (f"tags{SEP}[1]", "b"),
        ]


# ---------------------------------------------------------------------------
# _find_leaf_path_batch / _find_leaf_path_and_value_batch
# ---------------------------------------------------------------------------


class TestBatch:
    def test_find_leaf_path_batch(self, handler):
        data = [{"a": "1"}, {"b": "2"}]
        paths = list(handler._find_leaf_path_batch(data))
        assert paths == ["a", "b"]

    def test_find_leaf_path_and_value_batch(self, handler):
        data = [{"a": "1"}, {"b": "2"}]
        result = list(handler._find_leaf_path_and_value_batch(data))
        assert result == [("a", "1"), ("b", "2")]


# ---------------------------------------------------------------------------
# _get_leaf
# ---------------------------------------------------------------------------


class TestGetLeaf:
    def test_get_leaf_flat(self, handler):
        data = {"a": "value"}
        assert handler._get_leaf(data, "a") == "value"

    def test_get_leaf_nested(self, handler):
        data = {"a": {"b": "value"}}
        assert handler._get_leaf(data, f"a{SEP}b") == "value"

    def test_get_leaf_list(self, handler):
        data = {"items": ["x", "y", "z"]}
        assert handler._get_leaf(data, f"items{SEP}[1]") == "y"

    def test_get_leaf_returns_leaf_type_directly(self, handler):
        assert handler._get_leaf("hello", "anything") == "hello"
        assert handler._get_leaf(42, "anything") == 42
        assert handler._get_leaf(True, "anything") is True

    def test_get_leaf_path_as_list(self, handler):
        data = {"a": {"b": "value"}}
        assert handler._get_leaf(data, ["a", "b"]) == "value"

    def test_get_leaf_missing_key_raises(self, handler):
        data = {"a": "value"}
        with pytest.raises(KeyError):
            handler._get_leaf(data, "missing")


# ---------------------------------------------------------------------------
# _set_leaf
# ---------------------------------------------------------------------------


class TestSetLeaf:
    def test_set_leaf_flat(self, handler):
        data = {"a": "old"}
        result = handler._set_leaf(data, "a", "new")
        assert result["a"] == "new"

    def test_set_leaf_nested(self, handler):
        data = {"a": {"b": "old"}}
        result = handler._set_leaf(data, f"a{SEP}b", "new")
        assert result["a"]["b"] == "new"

    def test_set_leaf_list(self, handler):
        data = {"items": ["a", "b", "c"]}
        result = handler._set_leaf(data, f"items{SEP}[1]", "B")
        assert result["items"][1] == "B"

    def test_set_leaf_path_as_list(self, handler):
        data = {"a": {"b": "old"}}
        result = handler._set_leaf(data, ["a", "b"], "new")
        assert result["a"]["b"] == "new"

    def test_set_leaf_modifies_in_place(self, handler):
        data = {"a": "old"}
        result = handler._set_leaf(data, "a", "new")
        assert result is data

    def test_set_leaf_missing_key_adds(self, handler):
        data = {"a": "value"}
        result = handler._set_leaf(data, "missing", "v")
        assert result["missing"] == "v"

    def test_set_leaf_missing_intermediate_key_raises(self, handler):
        data = {"a": "value"}
        with pytest.raises(KeyError):
            handler._set_leaf(data, f"x{SEP}y", "v")


# ---------------------------------------------------------------------------
# allow / deny / skip logic
# ---------------------------------------------------------------------------


class TestAllowDenySkip:
    def test_allow_keys_skip_paths(self):
        h = MultiNestedDictHandler(allow_keys=["skip_me"])
        data = {"skip_me": "secret", "keep": "visible"}
        paths = list(h._find_leaf_path(data))
        assert paths == ["keep"]

    def test_deny_keys_mark_paths(self):
        h = MultiNestedDictHandler(deny_keys=["denied"])
        data = {"denied": "secret", "keep": "visible"}
        paths = list(h._find_leaf_path(data))
        assert "keep" in paths
        denied = [p for p in paths if h._is_denied_path(p)]
        assert len(denied) == 1

    def test_allow_keys_case_insensitive(self):
        h = MultiNestedDictHandler(allow_keys=["SkipMe"], case_sensitive=False)
        data = {"skipme": "v1", "keep": "v2"}
        paths = list(h._find_leaf_path(data))
        assert paths == ["keep"]

    def test_allow_keys_case_sensitive(self):
        h = MultiNestedDictHandler(allow_keys=["SkipMe"], case_sensitive=True)
        data = {"skipme": "v1", "SkipMe": "v2", "keep": "v3"}
        paths = list(h._find_leaf_path(data))
        assert "keep" in paths
        assert "skipme" in paths

    def test_deny_nested_path(self):
        h = MultiNestedDictHandler(deny_keys=[f"a{SEP}b"])
        data = {"a": {"b": "secret", "c": "visible"}}
        paths = list(h._find_leaf_path(data))
        assert any(h._is_denied_path(p) for p in paths)
        non_denied = [p for p in paths if not h._is_denied_path(p)]
        assert f"a{SEP}c" in non_denied

    def test_callable_allow_key(self):
        h = MultiNestedDictHandler(allow_keys=[lambda k: k.startswith("skip")])
        data = {"skip_this": "v1", "keep": "v2"}
        paths = list(h._find_leaf_path(data))
        assert paths == ["keep"]

    def test_callable_deny_key(self):
        h = MultiNestedDictHandler(deny_keys=[lambda k: k == "denied"])
        data = {"denied": "v1", "keep": "v2"}
        paths = list(h._find_leaf_path(data))
        denied = [p for p in paths if h._is_denied_path(p)]
        assert len(denied) == 1

    def test_wildcard_star_prefix(self):
        h = MultiNestedDictHandler(allow_keys=[f"*{SEP}secret"])
        data = {"a": {"secret": "v1", "public": "v2"}}
        paths = list(h._find_leaf_path(data))
        assert paths == [f"a{SEP}public"]

    def test_wildcard_star_suffix(self):
        h = MultiNestedDictHandler(allow_keys=[f"a{SEP}*"])
        data = {"a": {"b": "v1", "c": "v2"}, "d": "v3"}
        paths = list(h._find_leaf_path(data))
        assert paths == ["d"]


# ---------------------------------------------------------------------------
# _get_undenied_and_denied_paths
# ---------------------------------------------------------------------------


class TestUndeniedAndDeniedPaths:
    def test_no_deny(self, handler):
        data = {"a": "v1", "b": "v2"}
        mask_gen, deny_gen = handler._get_undenied_and_denied_paths(data)
        assert list(mask_gen) == ["a", "b"]
        assert list(deny_gen) == []

    def test_with_deny(self):
        h = MultiNestedDictHandler(deny_keys=["secret"])
        data = {"secret": "hidden", "public": "visible"}
        mask_gen, deny_gen = h._get_undenied_and_denied_paths(data)
        masks = list(mask_gen)
        denies = list(deny_gen)
        assert "public" in masks
        assert "secret" in denies

    def test_all_denied(self):
        h = MultiNestedDictHandler(deny_keys=["a", "b"])
        data = {"a": "v1", "b": "v2"}
        mask_gen, deny_gen = h._get_undenied_and_denied_paths(data)
        assert list(mask_gen) == []
        assert sorted(deny_gen) == ["a", "b"]


# ---------------------------------------------------------------------------
# _has_match_str: wildcard patterns
# ---------------------------------------------------------------------------


class TestHasMatchStr:
    def test_exact_match(self):
        h = MultiNestedDictHandler(allow_keys=["a"])
        assert h._has_match_str("a", mode="allow") is True
        assert h._has_match_str("b", mode="allow") is False

    def test_star_prefix_match(self):
        h = MultiNestedDictHandler(allow_keys=[f"*{SEP}key"])
        assert h._has_match_str("key", parent="anything", mode="allow") is True

    def test_star_suffix_match(self):
        h = MultiNestedDictHandler(allow_keys=[f"root{SEP}*"])
        # suffix wildcard matches when key_path is a prefix of the pattern
        assert h._has_match_str("root", mode="allow") is True

    def test_bracket_wildcard_via_find_leaf(self):
        h = MultiNestedDictHandler(allow_keys=[f"items{SEP}[*]{SEP}name"])
        # bracket wildcard is exercised through full tree traversal
        data = {"items": [{"name": "Alice", "age": 30}]}
        paths = list(h._find_leaf_path(data))
        assert f"items{SEP}[0]{SEP}age" in paths

    def test_no_match(self):
        h = MultiNestedDictHandler(allow_keys=["xyz"])
        assert h._has_match_str("abc", mode="allow") is False


# ---------------------------------------------------------------------------
# _has_match_callable
# ---------------------------------------------------------------------------


class TestHasMatchCallable:
    def test_allow_mode(self):
        h = MultiNestedDictHandler(allow_keys=[lambda k: k == "target"])
        assert h._has_match_callable("target", mode="allow") is True
        assert h._has_match_callable("other", mode="allow") is False

    def test_deny_mode(self):
        h = MultiNestedDictHandler(deny_keys=[lambda k: "secret" in k])
        assert h._has_match_callable("secret", mode="deny") is True
        assert h._has_match_callable("public", mode="deny") is False

    def test_invalid_mode_raises(self, handler):
        with pytest.raises(ValueError, match="not supported"):
            handler._has_match_callable("key", mode="invalid")

    def test_with_parent(self):
        h = MultiNestedDictHandler(allow_keys=[lambda k: k == f"parent{SEP}child"])
        assert h._has_match_callable("child", parent="parent", mode="allow") is True


# ---------------------------------------------------------------------------
# path separator parametrized
# ---------------------------------------------------------------------------


@pytest.fixture(params=[".", "~", ",", ".[$]."])
def path_separator(request):
    return request.param


class TestPathSeparatorParametrized:
    def test_nested_paths_use_separator(self, path_separator):
        h = MultiNestedDictHandler(path_separator=path_separator)
        data = {"a": {"b": "v"}}
        paths = list(h._find_leaf_path(data))
        assert paths == [f"a{path_separator}b"]

    def test_deeply_nested_paths_use_separator(self, path_separator):
        h = MultiNestedDictHandler(path_separator=path_separator)
        data = {"a": {"b": {"c": "v"}}}
        paths = list(h._find_leaf_path(data))
        assert paths == [path_separator.join(["a", "b", "c"])]

    def test_get_and_set_roundtrip(self, path_separator):
        h = MultiNestedDictHandler(path_separator=path_separator)
        data = {"a": {"b": "old"}}
        path = f"a{path_separator}b"
        assert h._get_leaf(data, path) == "old"
        h._set_leaf(data, path, "new")
        assert h._get_leaf(data, path) == "new"


# ---------------------------------------------------------------------------
# list_index parametrized
# ---------------------------------------------------------------------------


class TestListIndexParametrized:
    @pytest.mark.parametrize(
        "list_index",
        [
            {"start": "[", "end": "]"},
            {"start": "(", "end": ")"},
            {"start": "{", "end": "}"},
        ],
    )
    def test_list_paths_use_custom_index(self, list_index):
        h = MultiNestedDictHandler(list_index=list_index)
        data = {"items": ["a", "b"]}
        paths = list(h._find_leaf_path(data))
        s, e = list_index["start"], list_index["end"]
        assert paths == [f"items{SEP}{s}0{e}", f"items{SEP}{s}1{e}"]


# ---------------------------------------------------------------------------
# roundtrip: parse -> find paths -> get/set -> dump
# ---------------------------------------------------------------------------


class TestRoundtrip:
    def test_json_roundtrip(self, handler):
        raw = '{"name": "Alice", "address": {"city": "Zurich"}}'
        parsed = handler._parse_line(raw)

        paths_and_values = list(handler._find_leaf_path_and_value(parsed))
        assert ("name", "Alice") in paths_and_values
        assert (f"address{SEP}city", "Zurich") in paths_and_values

        handler._set_leaf(parsed, "name", "Bob")
        assert handler._get_leaf(parsed, "name") == "Bob"

        dumped = handler._dump_line(parsed)
        reparsed = handler._parse_line(dumped)
        assert reparsed["name"] == "Bob"

    def test_roundtrip_with_list(self, handler):
        data = {"tags": ["alpha", "beta"]}
        for path, _ in handler._find_leaf_path_and_value(data):
            handler._set_leaf(data, path, "MASKED")
        assert data == {"tags": ["MASKED", "MASKED"]}

    def test_roundtrip_with_numeric_leaves(self, handler):
        data = {"count": 5, "ratio": 0.5, "active": True}
        result = list(handler._find_leaf_path_and_value(data))
        assert result == [("count", 5), ("ratio", 0.5), ("active", True)]

        for path, _ in result:
            handler._set_leaf(data, path, "MASKED")
        assert all(v == "MASKED" for v in data.values())
