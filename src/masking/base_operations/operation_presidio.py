from collections.abc import Callable
from functools import reduce
from typing import ClassVar

from presidio_anonymizer import AnonymizerEngine, OperatorConfig

from masking.base_operations.entity_detection import (
    AnalyzerEngine,
    PresidioMultilingualAnalyzer,
)
from masking.base_operations.operation import Operation


class HashPresidioBase(Operation):
    """Hashes a text using hashlib algorithm and presidio to detect entities."""

    col_name: str  # column name to be hashed

    # Hashing function
    masking_function: Callable[[str], str]  # function to hash the input string

    # Spacy model to detect entities
    analyzer: AnalyzerEngine  # presidio analyzer engine

    # Entities to be detected as PII
    _PII_ENTITIES: ClassVar[set[str]] = {"EMAIL_ADDRESS", "PERSON", "PHONE_NUMBER"}

    def __init__(  # noqa: PLR0913
        self,
        col_name: str,
        masking_function: Callable[[str], str],
        allow_list: list[str] | None = None,
        analyzer: AnalyzerEngine | None = None,
        operators: dict[str, OperatorConfig] | None = None,
    ) -> None:
        """Initialize the HashTextSHA256 class.

        Args:
        ----
            col_name (str): column name to be hashed
            masking_function (Callable[[str], str]): function to hash the input string
            allow_list (list[str]): list of strings to be ignored
            analyzer (AnalyzerEngine): presidio analyzer engine
            operators (dict[str, OperatorConfig]): operators to use for masking entities

        """
        self.col_name = col_name
        self.masking_function = masking_function

        self.allow_list = allow_list if allow_list is not None else []

        self.concordance_table = {}

        self.analyzer = analyzer or PresidioMultilingualAnalyzer().analyzer

        self.operators = operators or {
            entity: OperatorConfig("custom", {"lambda": self.masking_function})
            for entity in self._PII_ENTITIES
        }

        self.anonymizer = AnonymizerEngine()

    def update_analyzer(self, analyzer: AnalyzerEngine) -> None:
        """Update the analyzer engine.

        Args:
        ----
            analyzer (AnalyzerEngine): analyzer engine

        """
        self.analyzer = analyzer

    def update_anonymizer(self, anonymizer: AnonymizerEngine) -> None:
        """Update the anonymizer engine.

        Args:
        ----
            anonymizer (AnonymizerEngine): anonymizer engine

        """
        self.anonymizer = anonymizer

    def _get_language_entities(
        self, line: str, language: str, nlp_artifacts: list | None = None
    ) -> set[str]:
        """Get entities in a text.

        Args:
        ----
            line (str): input text
            language (str): language of the text
            nlp_artifacts (list): nlp artifacts

        Returns:
        -------
            dict[str, set[str]]: dictionary with entities detected in the text

        """
        params = {
            "text": line,
            "language": language,
            "entities": self._PII_ENTITIES,
            "allow_list": self.allow_list,
        }

        if nlp_artifacts:
            params["nlp_artifacts"] = nlp_artifacts

        return self.analyzer.analyze(**params)

    def _get_entities(self, line: str) -> list[str]:
        """Get entities in text for each language.

        Args:
        ----
            line (str): input text

        Returns:
        -------
            dict[str, set[str]]: dictionary with entities detected in the text

        """
        return reduce(
            lambda x, y: x.union(y),
            (
                self._get_language_entities(line, lang)
                for lang in self.analyzer.supported_languages
            ),
        )

    def _mask_line(self, line: str, entities: list[str] | None = None) -> str:
        """Mask a single line.

        Args:
        ----
            line (str): input line
            entities (list): list of entities to mask

        Returns:
        -------
            str: masked line

        """
        # Detect entities in the line
        if entities is None:
            entities = self._get_entities(line)

        # Substitute entities with masked values
        return self.anonymizer.anonymize(
            line, list(entities), operators=self.operators
        ).text
