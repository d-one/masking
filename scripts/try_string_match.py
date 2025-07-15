import re

from presidio_analyzer import Pattern, PatternRecognizer
from presidio_anonymizer import AnonymizerEngine, OperatorConfig

test_strings = [
    ("123 45", "ID: 123 45."),
    ("123 45", "ID:123 45"),
    ("123 45", "ID-123 45"),
    ("123 45", "\t123 45\n"),
    ("123 45", "value=123 45, next one"),
    ("123 45", "abc123 45"),  # shouldn't match
    ("123 45", "123 456"),  # shouldn't match
    ("123 45", "<MASKED> Vaska, 04.07.1999 <MASKED>, CH-123 45 Neuenhof, PID: 2127468"),
    ("Marco", "<MASKED> Marco, ."),
]
template = r"""(?ix)
            (
                \b{value}\b
                |
                (?<=\n|\t){value}(?=\n|\t)
                |
                (?<=\W){value}(?=\W)
            )
        """

for v, test_string in test_strings:
    value = re.escape(v)
    presidio_pattern = Pattern(
        {"TestPattern"}, regex=template.format(value=value), score=0.85
    )
    recognizer = PatternRecognizer(
        supported_entity="TestEntity", patterns=[presidio_pattern]
    )
    print(f"Testing: '{test_string}'")
    anonym = AnonymizerEngine()
    anonymized_text = anonym.anonymize(
        text=test_string,
        analyzer_results=recognizer.analyze(text=test_string, entities=["TestEntity"]),
        operators={"TestEntity": OperatorConfig("replace", {"new_value": "<MASKED>"})},
    ).text
    print(f"Anonymized: '{anonymized_text}'\n")
