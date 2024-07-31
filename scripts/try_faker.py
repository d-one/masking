from masking.mask.operations.fake.date import FakeDateProvider
from masking.mask.operations.fake.name import FakeNameProvider
from masking.mask.operations.fake.plz import FakePLZProvider


def fake_date():
    fake_date = FakeDateProvider(preserve=("year", "month"))

    date_str = "2000-02-01"
    print("Date string:", date_str)

    date = fake_date(date_str)

    print("Fake date:")
    print(date)


def fake_name():
    fake_name = FakeNameProvider()

    print("Fake name:")
    print(fake_name())


def fake_plz():
    fake_plz = FakePLZProvider()

    real_plz = "8055"

    print("Real PLZ:")
    print(real_plz)

    print("Fake PLZ:")
    print(fake_plz(real_plz))
    print()


if __name__ == "__main__":
    fake_date()
    fake_name()
    fake_plz()
