import random
import sys
from concurrent.futures import ThreadPoolExecutor

import pytest
from masking.base_operations import operation_fake_name
from masking.base_operations.operation_fake_name import FakeNameBase
from masking.faker.name import FakeNameProvider
from masking.utils.random_set import RandomSet


class FakeName(FakeNameBase):
    def _mask_data(self, data):
        return data


@pytest.fixture(autouse=True)
def generated_names(monkeypatch) -> RandomSet:
    pool = RandomSet()
    monkeypatch.setattr(FakeNameBase, "_GENERATED_NAMES", pool)
    return pool


@pytest.fixture()
def _force_reuse(monkeypatch) -> None:
    real_choice = random.choice

    def choice(seq):
        return False if seq == [True, False] else real_choice(seq)

    monkeypatch.setattr(operation_fake_name.random, "choice", choice)


# Operation whose provider returns tagged names, to check which generator is used.
def make(**kwargs) -> FakeName:
    op = FakeName(col_name="n", reuse_existing=False, **kwargs)
    op.faker.get_last_name = lambda: "LAST"
    op.faker.get_first_name = lambda: "FIRST"
    op.faker.get_male_first_name = lambda: "MALE"
    op.faker.get_female_first_name = lambda: "FEMALE"
    op.faker.get_nonbinary_first_name = lambda: "NONBINARY"
    return op


class TestInit:
    def test_defaults(self):
        op = FakeName(col_name="n")
        assert isinstance(op.faker, FakeNameProvider)
        assert op.gender is None
        assert op.name_type == "full"
        assert op.reuse_existing is True

    @pytest.mark.parametrize("gender", ["x", "m f", "unknown", 1])
    def test_invalid_gender_raises(self, gender):
        with pytest.raises(ValueError, match="Gender"):
            FakeName(col_name="n", gender=gender)

    @pytest.mark.parametrize("name_type", ["middle", ""])
    def test_invalid_name_type_raises(self, name_type):
        with pytest.raises(ValueError, match="Name type"):
            FakeName(col_name="n", name_type=name_type)

    def test_name_type_is_normalized(self):
        assert FakeName(col_name="n", name_type=" First ").name_type == "first"

    def test_invalid_reuse_existing_raises(self):
        with pytest.raises(ValueError, match="Reuse existing"):
            FakeName(col_name="n", reuse_existing="yes")

    @pytest.mark.parametrize(
        ("gender", "expected"), [("Male", "male"), (" F ", "f"), ("NB", "nb")]
    )
    def test_gender_is_normalized(self, gender, expected):
        assert FakeName(col_name="n", gender=gender).gender == expected


class TestGeneration:
    @pytest.mark.parametrize(
        ("kwargs", "expected"),
        [
            ({"name_type": "last"}, "LAST"),
            ({"name_type": "first"}, "FIRST"),
            ({"name_type": "first", "gender": "m"}, "MALE"),
            ({"name_type": "first", "gender": "male"}, "MALE"),
            ({"name_type": "first", "gender": "f"}, "FEMALE"),
            ({"name_type": "first", "gender": "female"}, "FEMALE"),
            ({"name_type": "first", "gender": "nb"}, "NONBINARY"),
            ({"name_type": "first", "gender": "nonbinary"}, "NONBINARY"),
        ],
    )
    def test_name_type_and_gender_dispatch(self, kwargs, expected):
        assert make(**kwargs)._mask_line("Clear") == expected

    @pytest.mark.parametrize("gender", [None, "f", "m", "nb"])
    def test_full_name_uses_provider_with_gender(self, gender):
        op = make(gender=gender)
        op.faker = lambda g: f"full-{g}"
        assert op._mask_line("Clear") == f"full-{gender}"

    def test_real_provider_full_name(self):
        masked = FakeName(col_name="n", reuse_existing=False)._mask_line("Clear")
        assert isinstance(masked, str)
        assert " " in masked
        assert masked != "Clear"

    def test_without_reuse_names_are_not_pooled(self, generated_names):
        make()._mask_line("Clear")
        assert len(generated_names) == 0


class TestReuseExisting:
    @pytest.mark.usefixtures("_force_reuse")
    def test_generated_name_is_reused_and_removed(self, generated_names):
        op = make()
        op.reuse_existing = True
        generated_names.add("Bob")

        assert op._mask_line("Anna") == "Bob"
        assert "Bob" not in generated_names

    def test_new_name_is_pooled_but_input_is_not(self, monkeypatch, generated_names):
        monkeypatch.setattr(operation_fake_name.random, "choice", lambda _: True)
        op = make(name_type="last")
        op.reuse_existing = True

        assert op._mask_line("Anna") == "LAST"
        assert "LAST" in generated_names
        assert "Anna" not in generated_names

    def test_masked_values_never_contain_clear_values(self):
        clear = [f"Clear{i}" for i in range(200)]
        ops = [FakeName(col_name=c) for c in ("a", "b")]
        masked = [op._mask_line(name) for name in clear for op in ops]
        assert not set(masked) & set(clear)

    @pytest.mark.usefixtures("_force_reuse")
    def test_generated_names_are_reused_across_columns(self, generated_names):
        first, second = FakeName(col_name="a"), FakeName(col_name="b")
        name = first._mask_line("Anna")
        assert name in generated_names
        assert second._mask_line("Bob") == name

    def test_rejected_clear_value_is_not_pooled(self, monkeypatch, generated_names):
        monkeypatch.setattr(operation_fake_name.random, "choice", lambda _: True)
        op = make(name_type="last")
        op.reuse_existing = True
        names = iter(["Anna", "Clara"])
        op.faker.get_last_name = lambda: next(names)

        assert op._mask_line("Anna") == "Clara"
        assert "Anna" not in generated_names

    def test_pool_is_shared_between_instances(self, generated_names):
        assert FakeName(col_name="a")._GENERATED_NAMES is generated_names
        assert FakeName(col_name="b")._GENERATED_NAMES is generated_names

    def test_concurrent_workers_keep_pool_consistent(self, generated_names):
        interval = sys.getswitchinterval()
        sys.setswitchinterval(1e-6)
        ops = [make(name_type="last") for _ in range(8)]
        for op in ops:
            op.reuse_existing = True

        def work(worker: int) -> None:
            for i in range(2000):
                ops[worker]._mask_line(f"name-{(worker * 1000 + i) % 3000}")

        try:
            with ThreadPoolExecutor(max_workers=len(ops)) as executor:
                for future in [executor.submit(work, w) for w in range(len(ops))]:
                    future.result()
        finally:
            sys.setswitchinterval(interval)

        assert len(generated_names._items) == len(generated_names._index)
        assert all(
            generated_names._index[x] == i for i, x in enumerate(generated_names._items)
        )
