from presidio_analyzer import PatternRecognizer

# ---------------- GERMAN ----------------

titles_de = ["Dr.", "Prof."]
title_recognizer_de = PatternRecognizer(
    supported_entity="TITLE", deny_list=titles_de, supported_language="de"
)

abbreviation_de = ["z. B.", "zB", "z.B."]
abbreviation_recognizer_de = PatternRecognizer(
    supported_entity="ABKUERZUNG", deny_list=abbreviation_de, supported_language="de"
)

recognizer_de = [title_recognizer_de, abbreviation_recognizer_de]

# ---------------- ENGLISH ----------------

titles_en = ["Dr.", "Prof."]
title_recognizer_en = PatternRecognizer(
    supported_entity="TITLE", deny_list=titles_en, supported_language="en"
)

recognizer_en = [title_recognizer_en]

# ---------------- RECOGNIZERS ----------------

Recognizers = {"de": recognizer_de, "en": recognizer_en}

allow_list = set(titles_de + abbreviation_de + titles_en + abbreviation_de)
