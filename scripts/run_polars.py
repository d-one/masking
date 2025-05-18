import argparse
import shutil
import time
from pathlib import Path

import pandas as pd
import polars as pl
from masking.mask_polars.operations.operation_hash import HashOperation
from masking.mask_polars.pipeline import MaskDataFramePipeline

# Parse command-line arguments
parser = argparse.ArgumentParser(description="Mask data in a CSV file.")
parser.add_argument("-d", "--data", type=str, help="Path to the CSV file.")

args = parser.parse_args()

# Load the data
path = Path(args.data).resolve()

if not path.exists():
    msg = f"File not found: {path}"
    raise FileNotFoundError(msg)


def measure_execution_time(config: dict) -> float:
    data = pl.read_csv(path)
    print(data)
    pipeline = MaskDataFramePipeline(config, workers=min(4, len(config)))

    start_time = time.time()
    data = pipeline(data)

    path_to_save = path.resolve().parent / "build"
    if path_to_save.exists():
        shutil.rmtree(path_to_save)
    path_to_save.mkdir(parents=True, exist_ok=True)

    data.write_csv(path_to_save / "MaskedData.csv")

    # Print Masked Data und Concordance Tables
    for col_name, concordance_table in pipeline.concordance_tables.items():
        df_concordance_table = pl.DataFrame({
            "clear_values": list(concordance_table.keys()),
            "masked_values": list(concordance_table.values()),
        })

        df_concordance_table.write_csv(
            path_to_save / f"{col_name}_concordance_table.csv"
        )

    return time.time() - start_time


config = {
    "Name":
    # [
    {
        "masking_operation": HashOperation(
            col_name="Name",
            secret="my_secret",
            concordance_table=pd.DataFrame({
                "clear_values": ["Spiess"],
                "masked_values": ["SP"],
            }),
        )
    }
    #     {
    #         "masking_operation": HashOperation(
    #             col_name="Name",
    #             secret="my_secret",
    #             concordance_table=pd.DataFrame({
    #                 "clear_values": ["SP"],
    #                 "masked_values": ["123"],
    #             }),
    #         )
    #     },
    # ],
    # "Vorname": {
    #     "masking_operation": HashOperation(col_name="Vorname", secret="my_secret"),
    #     "concordance_table": pd.DataFrame({
    #         "clear_values": ["Darius"],
    #         "masked_values": ["DA"],
    #     }),
    # },
}

times = [measure_execution_time(config) for _ in range(1)]
print("Execution time: %s seconds" % (sum(times) / len(times)))
