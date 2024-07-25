import argparse
import time
from pathlib import Path

from masking.mask.pipeline import MaskDataFramePipeline
from pandas import read_csv

# Parse command-line arguments
parser = argparse.ArgumentParser(description="Mask data in a CSV file.")
parser.add_argument(
    "-d",
    "--data",
    type=str,
    help="Path to the CSV file.",
    default=Path(__file__).parent / "../.." / "MaskingData.csv",
)

args = parser.parse_args()

# Load the data
path = Path(args.data).resolve()

if not path.exists():
    msg = f"File not found: {path}"
    raise FileNotFoundError(msg)


def measure_execution_time(config: dict) -> float:
    pipeline = MaskDataFramePipeline(config)
    data = read_csv(path)

    start_time = time.time()
    data = pipeline(data)

    data.to_csv(path.parent / "MaskedData.csv", index=False)

    # Print Masked Data und Concordance Tables
    for col_name, concordance_table in pipeline.concordance_tables.items():
        concordance_table.to_csv(
            path.parent / f"{col_name}_concordance_table.csv", index=False
        )

    return time.time() - start_time


config = {
    # "Beschrieb": {
    #     "masking": "presidio",
    #     "config": {
    #         "col_name": "Beschrieb",
    #         # "masking_function": lambda x: hash_string(
    #         #     x, "new_secret", method=hashlib.sha256
    #         # ),
    #         "masking_function": lambda x: FakeNameProvider().__call__(),
    #     },
    # },
    "Name": {"masking": "hash", "config": {"col_name": "Name", "secret": "my_secret"}}
    # "Vorname": {
    #     "masking": "hash",
    #     "config": {"col_name": "Vorname", "secret": "my_secret"},
    # },
    # "Geburtsdatum": {
    #     "masking": "fake_date",
    #     "config": {"col_name": "Geburtsdatum", "preserve": ("year", "month")},
    # },
    # "PLZ": {
    #     "masking": "fake_plz",
    #     "config": {"col_name": "PLZ", "preserve": ("district", "area")},
    # },
}

times = [measure_execution_time(config) for _ in range(1)]
print("Execution time: %s seconds" % (sum(times) / len(times)))
