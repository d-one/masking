import argparse
from pathlib import Path

from demasking.pipeline import DeMaskTablePipeline
from pandas import read_csv

# Parse command-line arguments
parser = argparse.ArgumentParser(description="Mask data in a CSV file.")
parser.add_argument(
    "-d",
    "--data",
    type=str,
    help="Path to the CSV file.",
    default=Path(__file__).parent / "../.." / "MaskedData.csv",
)

parser.add_argument(
    "-c",
    "--concordance_table",
    type=str,
    help="Path to the concordance table (CSV file). Concordance table can be either a path to all the concordance tables or a specific concordance table (.csv file)",
    default=Path(__file__).parent / "../.." / ".",
)

args = parser.parse_args()

# Load the data
path = Path(args.data).resolve()

if not path.exists():
    msg = f"File not found: {path}"
    raise FileNotFoundError(msg)

# Load the concordance table
concordance_table_path = Path(args.concordance_table).resolve()
concordance_tables = [concordance_table_path]

# Check if the concordance table is a directory or a file
if concordance_table_path.is_dir():
    concordance_tables = list(concordance_table_path.glob("*_concordance_table.csv"))

# Read the data
data = read_csv(path)

# Read the concordance tables
concordance_tables = {
    str(concordance_table.name.split("_concordance_table")[0]): read_csv(
        concordance_table
    )
    for concordance_table in concordance_tables
}

# Demasking logicc
config = {
    # "Name": {"masking": "hash", "config": {"col_name": "Name"}},
    # "Vorname": {"masking": "hash"},
    "Geburtsdatum": {"masking": "fake_date"},
    "PLZ": {"masking": "fake_plz"},
    # "Beschrieb": {"masking": "entities"},
}

pipeline = DeMaskTablePipeline(config=config, concordance_tables=concordance_tables)

d = pipeline(data)

print(d)

# Save the demasked data
d.to_csv(path.parent / "DeMaskedData.csv", index=False)

# Compare to the original data
original_data = read_csv(Path(__file__).parent / "../.." / "MaskingData.csv")

for col in original_data.columns:
    conditions = original_data[col].compare(d[col])

    message = f"""
    Column {col} is not equal to the original data.
    Conditions: {conditions}
    """

    assert conditions.empty, message

print("All columns are equal to the original data.")
