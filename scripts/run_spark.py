import argparse
import shutil
import time
from pathlib import Path

import pandas as pd
from masking.mask_spark.operations.operation_fake_date import FakeDate
from masking.mask_spark.operations.operation_fake_name import FakeNameOperation
from masking.mask_spark.operations.operation_fake_plz import FakePLZ
from masking.mask_spark.operations.operation_hash import HashOperation
from masking.mask_spark.operations.operation_med_stats import MedStatsOperation
from masking.mask_spark.operations.operation_presidio import MaskPresidio
from masking.mask_spark.operations.operation_presidio_dict import MaskDictOperation
from masking.mask_spark.operations.operation_string_match import StringMatchOperation
from masking.mask_spark.operations.operation_string_match_dict import (
    StringMatchDictOperation,
)
from masking.mask_spark.operations.operation_yyyy_hash import YYYYHashOperation
from masking.mask_spark.pipeline import MaskDataFramePipeline
from masking.utils.entity_detection import PresidioMultilingualAnalyzer
from pyspark.sql import SparkSession

# Parse command-line arguments
parser = argparse.ArgumentParser(description="Mask data in a CSV file.")
parser.add_argument("-d", "--data", type=str, help="Path to the CSV file.")
args = parser.parse_args()

# Load the data
path = Path(args.data).resolve()
if not path.exists():
    msg = f"File not found: {path}"
    raise FileNotFoundError(msg)

spark = (
    SparkSession.builder.appName("masking")
    .config("spark.executor.memory", "8g")
    .config("spark.driver.memory", "8g")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.executor.extraJavaOptions", "-XX:ReservedCodeCacheSize=256m")
    .config("spark.driver.extraJavaOptions", "-XX:ReservedCodeCacheSize=256m")
    .getOrCreate()
)


def measure_execution_time(config: dict) -> float:
    data = (
        spark.read.option("escape", '"')
        .option("multiline", "true")
        .csv(str(path.absolute()), header=True)
    )

    data.show(10)

    # Cast the colum Geburtsdatum to Timestamp
    # if "Geburtsdatum" in data.columns:
    #     data = data.withColumn("Geburtsdatum", data["Geburtsdatum"].cast("timestamp"))

    # Repartition the DataFrame to a single partition
    data = data.repartition(1)

    # Print the number of partitions
    print("Number of partitions: ", data.rdd.getNumPartitions())

    # Print the number of workers
    print("Number of workers: ", spark.sparkContext.defaultParallelism)

    start_time = time.time()

    pipeline = MaskDataFramePipeline(config, workers=min(4, len(config)))
    data = pipeline(data)

    path_to_save = path.parent / "build"
    if path_to_save.exists():
        shutil.rmtree(path_to_save)

    path_to_save /= "MaskedData.csv"
    path_to_save.parent.mkdir(parents=True, exist_ok=True)

    data.coalesce(1).write.option("escape", '"').option("multiline", "true").csv(
        str(path_to_save.absolute()), mode="overwrite", header=True
    )

    # Print Masked Data und Concordance Tables
    for col_name, concordance_table in pipeline.concordance_tables.items():
        ct = pd.DataFrame(
            zip(concordance_table.keys(), concordance_table.values(), strict=False),
            columns=["clear_values", "masked_values"],
        )
        ct.to_csv(path_to_save.parent / f"concordance_table_{col_name}.csv")

    return time.time() - start_time


analyzer = PresidioMultilingualAnalyzer(
    models={"de": "de_core_news_lg", "en": "en_core_web_trf"}
).analyzer


config = {
    "PLZ": [
        {"masking_operation": FakePLZ(col_name="PLZ", preserve=("district"))},
        {"masking_operation": MedStatsOperation(col_name="PLZ")},
    ],
    "Name": [
        {
            "masking_operation": FakeNameOperation(
                col_name="Name",
                concordance_table=pd.DataFrame({
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
                concordance_table=pd.DataFrame({
                    "clear_values": ["SP"],
                    "masked_values": ["123"],
                }),
            )
        },
    ],
    "Vorname": {
        "masking_operation": HashOperation(col_name="Vorname", secret="my_secret"),
        "concordance_table": pd.DataFrame({
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
                concordance_table=pd.DataFrame({
                    "clear_values": ["1979-04-24"],
                    "masked_values": ["2050-01-01"],
                }),
            )
        },
        {
            "masking_operation": YYYYHashOperation(
                col_name="Geburtsdatum",
                secret="my_secret",
                concordance_table=pd.DataFrame({
                    "clear_values": ["2050-01-01"],
                    "masked_values": ["<MASKED>"],
                }),
            )
        },
    ],
}

times = [measure_execution_time(config) for _ in range(1)]
print("Execution time: %s seconds" % (sum(times) / len(times)))
