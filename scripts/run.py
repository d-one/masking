import argparse
from pathlib import Path

from masking.pipeline import MaskDataFramePipeline
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

data = read_csv(path)

config = {
    "Name": ("hash", {"col_name": "Name", "secret": "my_secret"}),
    "Vorname": ("hash", {"col_name": "Vorname", "secret": "my_new_secret"}),
}

pipeline = MaskDataFramePipeline(config)
data = pipeline(data)
