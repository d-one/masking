import argparse
import shutil
import time
from pathlib import Path

import pandas as pd
from masking.mask_spark.operations.operation_hash import HashOperation
from masking.mask_spark.pipeline import MaskDataFramePipeline
from masking.utils.presidio_handler import PresidioMultilingualAnalyzer
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
    if "Geburtsdatum" in data.columns:
        data = data.withColumn("Geburtsdatum", data["Geburtsdatum"].cast("timestamp"))
        # data = data.withColumn("Extra", data["Extra"].cast("string"))
    data = data.repartition(1)

    # Print the number of partitions
    print("Number of partitions: ", data.rdd.getNumPartitions())

    # Print the number of workers
    print("Number of workers: ", spark.sparkContext.defaultParallelism)

    # if any(
    #     isinstance(
    #         config[col_name]["masking_operation"], HashDictOperation | HashPresidio
    #     )
    #     for col_name in config
    # ):
    #     # Update the analyzer in the config
    #     for col_name in config:
    #         if isinstance(
    #             config[col_name]["masking_operation"], HashDictOperation | HashPresidio
    #         ):
    #             analyzer = config[col_name]["masking_operation"].analyzer
    #             broadcased_analyzer = spark.sparkContext.broadcast(analyzer)

    #             config[col_name]["masking_operation"].update_analyzer(
    #                 broadcased_analyzer.value
    #             )

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
    "Name": [
        {
            "masking_operation": HashOperation(
                col_name="Name", secret="my_secret", concordance_table={"Spiess": "SP"}
            )
        }
    ],
    "Vorname": {
        "masking_operation": HashOperation(col_name="Vorname", secret="my_secret"),
        "concordance_table": {"Darius": "DA"},
    },
    # "Beschrieb": {
    #     "masking_operation": HashPresidio(
    #         col_name="Beschrieb",
    #         masking_function=lambda x: "<MASKED>",
    #         analyzer=analyzer,
    #         allow_list=["Darius"],
    #         pii_entities=["PERSON"],
    #     )
    # },
    # "Geburtsdatum": {
    #     "masking_operation": YYYYHashOperation(
    #         col_name="Geburtsdatum", secret="my_secret"
    #     )
    # },
    # "Extra": {
    #     "masking_operation": StringMatchOperation(
    #         col_name="Extra",
    #         pii_cols=["Name", "Vorname", "Geburtsdatum"],
    #         allow_list=["Darius"],
    #         deny_keys=["Doctor"],
    #         masking_function=lambda x: "<MASKED>",
    #     )
    # },
}

times = [measure_execution_time(config) for _ in range(1)]
print("Execution time: %s seconds" % (sum(times) / len(times)))
