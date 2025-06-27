import tempfile
import zipfile
from pathlib import Path
from types import TracebackType
from typing import TYPE_CHECKING, ClassVar
from urllib.request import urlretrieve

import pandas as pd

from .faker import FakeProvider

if TYPE_CHECKING:
    from faker.generator import Generator


class FakePLZProvider(FakeProvider):
    """Fake PLZ provider.

    Every PLZ in Switzerland is defined by 4 digits, and take the following form:

    3436 Zollbrück
    3 = district (Bern)
    34 = area (Burgdorf)
    343 = route (Burgdorf - Langnau)
    3436 = post office number (Zollbrück)
    """

    _DOWNLOAD_URL: str = "https://data.geo.admin.ch/ch.swisstopo-vd.ortschaftenverzeichnis_plz/ortschaftenverzeichnis_plz/ortschaftenverzeichnis_plz_2056.csv.zip"
    _PRESERVED: ClassVar[set[str]] = set()
    _ADMISSIBLE: ClassVar[set[str]] = {
        0: "district",
        1: "area",
        2: "route",
        3: "postcode",
    }

    def __init__(
        self, preserve: str | tuple[str] | None = None, locale: str = "de_CH"
    ) -> None:
        """Initialize the FakePLZProvider.

        Args:
        ----
            locale (str, optional): Country initials such ash 'de_CH'.
            preserve (str|tuple, optional): The values to preserve. Defaults to "district".

        """
        self.generator: Generator = self.get_generator(locale)

        # self.files: pd.DataFrame = self._download()           noqa: ERA001
        file_path = Path(__file__).parent / "sources" / "AMTOVZ_CSV_LV95.csv"
        self.files: pd.DataFrame = pd.read_csv(file_path, sep=";", usecols=["PLZ"])

        if preserve is not None:
            if isinstance(preserve, str):
                preserve = (preserve,)

            # Check if the preserve values are admissible
            msg = f"""Preserve values must be {", ".join([f"'{s}'" for s in self._ADMISSIBLE.values()])}."""
            assert all(s in self._ADMISSIBLE.values() for s in preserve), msg  # noqa: S101

            self._PRESERVED = set(preserve)

    def _download(self) -> pd.DataFrame:
        """Download the PLZ data."""
        # Define a temporary file
        with tempfile.TemporaryFile() as tmp:
            _, f = tempfile.mkstemp()  # Create a temporary file
            files = None

            try:
                # Download the file
                urlretrieve(self._DOWNLOAD_URL, f)  # noqa: S310

                # Ectract the zip file
                with zipfile.ZipFile(f, "r") as zip_ref:
                    member_to_unzip = next(
                        iter({z for z in zip_ref.namelist() if z.endswith(".csv")})
                    )
                    # find the first csv file in the list of unzipped files

                    # Extract the file
                    with zip_ref.open(member_to_unzip) as member:
                        # Write the file to the temporary file
                        tmp.write(member.read())

                    # Reset the file pointer to the beginning of the file
                    tmp.seek(0)

                    # Read the file
                    files = pd.read_csv(tmp, sep=";", usecols=["PLZ"])

            except Exception as e:
                msg = f"Could not download the file: {e}"
                raise ValueError(msg) from e
            finally:
                Path(f).unlink()

            return files

    def parse_plz(self, plz: str) -> dict[str, str]:
        """Define a dict for the input plz.

        Args:
        ----
            plz (str): A PLZ.

        Returns:
        -------
            dict: A dictionary with the district, area, route and postcode.

        """
        # Make sure plz is of 4 digits
        assert len(plz) == len(  # noqa: S101
            self._ADMISSIBLE
        ), f"PLZ must be of {len(self._ADMISSIBLE)} digits."

        return {"district": plz[0], "area": plz[1], "route": plz[2], "postcode": plz[3]}

    def extract_ranges(self, plz: str | dict[str, str]) -> dict[str, str]:
        """Extract the ranges of the plz.

        Args:
        ----
            plz (str|dict): A PLZ. If a string is passed, it is parsed into a dictionary.
                            If a dictionary is passed, it is assumed to have the keys: district, area, route and postcode (e.g. the self_ADMISSIBLE).

        Returns:
        -------
            dict: A dictionary with the start and end of the ranges.

        """
        plz_dict = plz if isinstance(plz, dict) else self.parse_plz(plz)

        ranges = {k: {"start": "0", "end": "9"} for k in self._ADMISSIBLE.values()}

        for k in self._PRESERVED:
            ranges[k]["start"] = plz_dict[k]
            ranges[k]["end"] = plz_dict[k]

        return {
            "start": int(
                "".join([
                    ranges[self._ADMISSIBLE[k]]["start"]
                    for k in range(len(self._ADMISSIBLE))
                ])
            ),
            "end": int(
                "".join([
                    ranges[self._ADMISSIBLE[k]]["end"]
                    for k in range(len(self._ADMISSIBLE))
                ])
            )
            + 1,
        }

    def __call__(self, plz: str | int) -> str:
        """Generate a random PLZ.

        The function generates a random PLZ that is not equal to the input PLZ, based on the preserved values.
        If the input PLZ is not found in the list of available PLZs, it is returned as is.

        Args:
        ----
            plz (str|int): A PLZ.

        Returns:
        -------
            str: A random PLZ.

        """
        if not isinstance(plz, str):
            plz = str(plz)

        plz_dict = self.parse_plz(plz)

        if self._PRESERVED == self._ADMISSIBLE:
            return plz

        # Find ranges of admissible values
        ranges = self.extract_ranges(plz_dict)

        # Generate the PLZ-provider
        admissible = self.files["PLZ"][
            (self.files["PLZ"] >= ranges["start"]) & (self.files["PLZ"] < ranges["end"])
        ].tolist()
        return str(self.generator.random_element(admissible))

    def __exit__(
        self,
        typ: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool:
        self.files.close()
        return True
