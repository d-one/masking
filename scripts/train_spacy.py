import argparse
import random
import shutil
import time
from pathlib import Path

import spacy
from masking.mask.operations.operation_presidio import MaskPresidio
from masking.mask.pipeline import MaskDataFramePipeline
from masking.utils.entity_detection import PresidioMultilingualAnalyzer
from pandas import DataFrame, read_csv
from presidio_analyzer import Pattern, PatternRecognizer
from spacy.training.example import Example

MY_TRAIN_ANNOTATIONS = [
    (
        "The medical report indicates a diagnosis of both diabetes and hypertension.",
        {"entities": [("diabetes", "DISEASE"), ("hypertension", "DISEASE")]},
    ),
    (
        "During the assessment, signs of asthma were clearly noted.",
        {"entities": [("asthma", "DISEASE")]},
    ),
    (
        "He has been suffering from chronic bronchitis and emphysema for several years.",
        {"entities": [("bronchitis", "DISEASE"), ("emphysema", "DISEASE")]},
    ),
    (
        "Pneumonia was confirmed after analyzing the chest X-ray.",
        {"entities": [("pneumonia", "DISEASE")]},
    ),
    (
        "Current treatment includes medication for tuberculosis and HIV.",
        {"entities": [("tuberculosis", "DISEASE"), ("HIV", "DISEASE")]},
    ),
    (
        "Lupus was detected following a series of autoimmune tests.",
        {"entities": [("Lupus", "DISEASE")]},
    ),
    (
        "Early signs of Alzheimer's disease were detected in the cognitive screening.",
        {"entities": [("Alzheimer's disease", "DISEASE")]},
    ),
    (
        "She frequently experiences intense migraines and vertigo.",
        {"entities": [("migraines", "DISEASE"), ("vertigo", "DISEASE")]},
    ),
    (
        "Chickenpox was diagnosed in the child after a rash appeared.",
        {"entities": [("Chickenpox", "DISEASE")]},
    ),
    (
        "He is currently recovering from a recent influenza infection.",
        {"entities": [("influenza", "DISEASE")]},
    ),
    (
        "The patient is dealing with both rheumatoid arthritis and osteoporosis.",
        {
            "entities": [
                ("rheumatoid arthritis", "DISEASE"),
                ("osteoporosis", "DISEASE"),
            ]
        },
    ),
]


def prepare_annotations_for_training(annotations: list) -> list:
    """Prepare the annotations for spacy training by converting entity texts to start and end indices.

    Args:
    ----
        annotations (list): A list of tuples where each tuple contains a text and a dictionary with entities.
                            Expected format: [(text, {"entities": [(entity, label), ...]}), ...]

    Returns:
    -------
        list: A list of tuples where each tuple contains a text and a dictionary with unique entities.
                            Format: [(text, {"entities": [(start, end, label), ...]}), ...]

    """
    prepared_annotations = []
    for text, entities in annotations:
        entity_set = set()
        for entity, label in entities["entities"]:
            start = text.find(entity)
            if start != -1:
                end = start + len(entity)
                entity_set.add((start, end, label))
        if entity_set:
            prepared_annotations.append((text, {"entities": list(entity_set)}))
    return prepared_annotations


def check_annotations(annotations: list) -> None:
    """Check the annotations by printing the text with entities in brackets."""
    for text, entities in annotations:
        print(f"Text: {text}")
        if not entities["entities"]:
            print("    No entities found.")
            continue
        print("    Entities:")
        for start, end, label in entities["entities"]:
            print(f"        [{text[start:end]}] {label}")


def train_ner(
    nlp: spacy.language.Language, train_data: list, n_iter: int
) -> spacy.language.Language:
    """Train a Named Entity Recognition (NER) model using spaCy.

    Args:
    ----
        nlp (spacy.language.Language): The spaCy language model to train.
        train_data (list): A list of training data in the format [(text, {"entities": [(start, end, label), ...]}), ...].
        n_iter (int): The number of iterations to train the model.

    Returns:
    -------
        spacy.language.Language: The trained spaCy language model with the NER component updated.

    """
    # Add the NER component if not already in the pipeline
    if "ner" not in nlp.pipe_names:
        print("Adding NER component to the pipeline.")
        nlp.add_pipe("ner", last=True)

    ner = nlp.get_pipe("ner")

    # Add the labels to the NER component
    for _, annotations in train_data:
        for ent in annotations.get("entities"):
            ner.add_label(ent[2])

    # Disable other components in the pipeline
    other_pipes = [pipe for pipe in nlp.pipe_names if pipe != "ner"]

    # Train the NER model
    with nlp.disable_pipes(*other_pipes):
        optimizer = nlp.begin_training()
        for itn in range(n_iter):
            random.shuffle(train_data)
            losses = {}

            for text, annotations in train_data:
                doc = nlp.make_doc(text)
                example = Example.from_dict(doc, annotations)
                nlp.update([example], drop=0.5, sgd=optimizer, losses=losses)

            print(f"Iteration {itn} Loss: {losses}")

    return nlp


# TRAIN_DATA = prepare_annotations_for_training(MY_TRAIN_ANNOTATIONS)
# #check_annotations(TRAIN_DATA)

# # Define a spaCy model to train
# nlp = spacy.load("en_core_web_lg")

# # Test text to see the model's performance before training
# test_text = "Müller Darius could also have suffered from another illness that led to heart failure, for example myocarditis or cardiomyopathy. (Attending Doctor: Dr. med. Detlef Schaller)"

# print("Training the model...")
# nlp = train_ner(nlp, TRAIN_DATA, 100)

# print("After training:")
# doc = nlp(test_text)
# for ent in doc.ents:
#     print(ent.text, ent.label_)

# exit(1)

# ---- Try the model on our dataframe

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
    models={"en": "en_core_web_trf"}
    # ner_model_mapping={"DISEASE": "DISEASE"}
).analyzer

# Add custom recognizer for diseases
analyzer.registry.add_recognizer(
    PatternRecognizer(
        supported_entity="DISEASE",
        # deny_list=[
        #     "diabetes",
        #     "hypertension",
        #     "asthma",
        #     "bronchitis",
        #     "emphysema",
        #     "cardiomyopathy",
        #     "tuberculosis",
        #     "HIV",
        #     "Lupus",
        #     "Alzheimer's disease",
        #     "migraines",
        #     "vertigo",
        #     "Chickenpox",
        #     "influenza",
        #     "rheumatoid arthritis",
        #     "osteoporosis",
        # ],
        patterns=[
            Pattern(
                name="regex_disease",
                regex=r"(?i)\b(?:diabetes|hypertension|asthma|"
                r"bronchitis|emphysema|cardiomyopathy|"
                r"tuberculosis|hiv|lupus|alzheimer's disease|"
                r"migraines|vertigo|chickenpox|influenza|"
                r"rheumatoid arthritis|osteoporosis|myocarditis)\b",
                score=0.85,
            )
        ],
    )
)

config = {
    "Description": [
        {
            "masking_operation": MaskPresidio(
                col_name="Description",
                masking_function=lambda x: "<MASKED>",
                analyzer=analyzer,
                allow_list=["Darius"],
                pii_entities=["DISEASE"],
            )
        }
    ]
}

times = [measure_execution_time(config) for _ in range(1)]
print("Execution time: %s seconds" % (sum(times) / len(times)))
