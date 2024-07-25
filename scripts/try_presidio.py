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

# registry = RecognizerRegistry()
# # registry.load_predefined_recognizers("de")

# for recognizer in Recognizers.values():
#     for rec in recognizer:
#         registry.add_recognizer(rec)

# analyzer.registry = registry

# Analyze text
t = "Künzler Yvette könnte auch an einer anderen Erkrankung gelitten haben, die zum Herzversagen führte, z. B. an einer Myokarditis oder an einer Kardiomyopathie."

print("Text : ", t)
print("-------------------")

print("German : ")
print()

r = analyzer.analyze(
    text=t, language="de", return_decision_process=True, allow_list=["z. B."]
)

for res in r:
    print("Entity : ", res)
    print(t[res.start : res.end])
    print("t : <", res.entity_type, ">", sep="")
    print(res.analysis_explanation)


print("-------------------")

print("English : ")
print()

r = analyzer.analyze(text=t, language="en")

for res in r:
    print("Entity : ", res)
    print(t[res.start : res.end])
