import time
from concurrent.futures import ThreadPoolExecutor

import spacy
from presidio_analyzer import AnalyzerEngine
from presidio_analyzer.nlp_engine import SpacyNlpEngine


# Create a class inheriting from SpacyNlpEngine
class LoadedSpacyNlpEngine(SpacyNlpEngine):
    def __init__(
        self,
        german_model: str = "de_core_news_lg",
        english_model: str = "en_core_web_trf",
    ):
        super().__init__()
        self.nlp = {"de": spacy.load(german_model), "en": spacy.load(english_model)}


# Pass the loaded model to the new LoadedSpacyNlpEngine

loaded_nlp_engine = LoadedSpacyNlpEngine()

# Pass the engine to the analyzer
analyzer = AnalyzerEngine(
    nlp_engine=loaded_nlp_engine, supported_languages=["de", "en"]
)

# Analyze text
t = [
    "Künzler Yvette könnte auch an einer anderen Erkrankung gelitten haben, die zum Herzversagen führte, z. B. an einer Myokarditis oder an einer Kardiomyopathie.",
    "Hediger Olivia könnte auch an einer anderen Erkrankung gelitten haben, die zum Herzversagen führte, z. B. an einer Myokarditis oder an einer Kardiomyopathie.",
]

print("Text : ", t)
print("-------------------")

print("German : ")
print()


def analyze_text(text: str):
    print("Text")
    s = analyzer.analyze(
        text=text, language="de", return_decision_process=True, allow_list=["z. B."]
    )

    time.sleep(1)

    print("Done")

    return s


t *= 10

with ThreadPoolExecutor() as executor:
    results = executor.map(analyze_text, t)


for r in results:
    for res in r:
        print("Entity : ", res)
        print(t[res.start : res.end])
        print("t : <", res.entity_type, ">", sep="")
        print(res.analysis_explanation)
