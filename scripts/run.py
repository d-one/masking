import argparse
import shutil
import time
from pathlib import Path

from masking.mask.operations.operation_fake_date import FakeDate
from masking.mask.operations.operation_fake_name import FakeNameOperation
from masking.mask.operations.operation_fake_plz import FakePLZ
from masking.mask.operations.operation_hash import HashOperation
from masking.mask.operations.operation_med_stats import MedStatsOperation
from masking.mask.operations.operation_presidio import MaskPresidio
from masking.mask.operations.operation_presidio_dict import MaskDictOperation
from masking.mask.operations.operation_string_match import StringMatchOperation
from masking.mask.operations.operation_string_match_dict import StringMatchDictOperation
from masking.mask.operations.operation_yyyy_hash import YYYYHashOperation
from masking.mask.pipeline import MaskDataFramePipeline
from masking.utils.entity_detection import PresidioMultilingualAnalyzer
from pandas import DataFrame, read_csv

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
    pipeline = MaskDataFramePipeline(config, workers=min(4, len(config)))
    data = read_csv(path)

    print(data)

    start_time = time.time()
    data = pipeline(data)

    path_to_save = path.resolve().parent / "build"
    if path_to_save.exists():
        shutil.rmtree(path_to_save)
    path_to_save.mkdir(parents=True, exist_ok=True)

    data.to_csv(path_to_save / "MaskedData.csv", index=False)

    # Print Masked Data und Concordance Tables
    for col_name, concordance_table in pipeline.concordance_tables.items():
        df_concordance_table = DataFrame(
            {
                "clear_values": list(concordance_table.keys()),
                "masked_values": list(concordance_table.values()),
            },
            index=None,
        )

        df_concordance_table.to_csv(
            path_to_save / f"{col_name}_concordance_table.csv", index=False
        )

    return time.time() - start_time


analyzer = PresidioMultilingualAnalyzer(
    models={"de": "de_core_news_lg", "en": "en_core_web_trf"}
).analyzer


config = {
    "PLZ": [
        {
            "masking_operation": FakePLZ(
                col_name="PLZ", preserve=("district", "area", "route")
            )
        },
        {"masking_operation": MedStatsOperation(col_name="PLZ")},
    ],
    "Name": [
        {
            "masking_operation": FakeNameOperation(
                col_name="Name",
                concordance_table=DataFrame({
                    "clear_values": ["Spiess"],
                    "masked_values": ["SP"],
                }),
                name_type="last",
            )
        },
        {
            "masking_operation": HashOperation(
                col_name="Name",
                secret="my_secret",
                concordance_table=DataFrame({
                    "clear_values": ["SP"],
                    "masked_values": ["123"],
                }),
            )
        },
    ],
    "Vorname": {
        "masking_operation": HashOperation(col_name="Vorname", secret="my_secret"),
        "concordance_table": DataFrame({
            "clear_values": ["Darius"],
            "masked_values": ["DA"],
        }),
    },
    "Beschrieb": [
        {
            "masking_operation": StringMatchOperation(
                col_name="Beschrieb",
                masking_function=lambda x: "<MASKED>",
                pii_cols=["Vorname", "Name"],
            )
        },
        {
            "masking_operation": MaskPresidio(
                col_name="Beschrieb",
                masking_function=lambda x: "<MASKED>",
                analyzer=analyzer,
                allow_list=["Darius"],
                pii_entities=["PERSON"],
            )
        },
    ],
    "Report": [
        {
            "masking_operation": StringMatchDictOperation(
                col_name="Report",
                masking_function=lambda x: "<MASKED>",
                pii_cols=["Vorname", "Name"],
                # path_separator=".",
                # deny_keys=["*.patient"],
            )
        },
        {
            "masking_operation": MaskDictOperation(
                col_name="Report",
                masking_function=lambda x: "<MASKED>",
                analyzer=analyzer,
                pii_entities=["PERSON"],
                # path_separator=".",
                # deny_keys=["*.patient"],
            )
        },
    ],
    "Geburtsdatum": [
        {
            "masking_operation": FakeDate(
                col_name="Geburtsdatum",
                preserve=("year", "month"),
                concordance_table=DataFrame({
                    "clear_values": ["1979-04-24"],
                    "masked_values": ["2050-01-01"],
                }),
            )
        },
        {
            "masking_operation": YYYYHashOperation(
                col_name="Geburtsdatum",
                secret="my_secret",
                concordance_table=DataFrame({
                    "clear_values": ["2050-01-01"],
                    "masked_values": ["<MASKED>"],
                }),
            )
        },
    ],
}

times = [measure_execution_time(config) for _ in range(1)]
print("Execution time: %s seconds" % (sum(times) / len(times)))
