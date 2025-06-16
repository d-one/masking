import operator
from collections.abc import Callable
from functools import reduce
from typing import ClassVar

from presidio_anonymizer import AnonymizerEngine, OperatorConfig

from masking.utils.entity_detection import AnalyzerEngine


class PresidioHandler:
    analyzer: AnalyzerEngine = None
    anonymizer: AnonymizerEngine = None

    operators: dict[str, OperatorConfig] | None = None

    allow_list: list[str] | None = (None,)
    deny_list: list[str] | None = (None,)

    # Entities to be detected as PII
    _PII_ENTITIES: ClassVar[set[str]] = {
        "EMAIL_ADDRESS",
        "PERSON",
        "PHONE_NUMBER",
        "DATE_TIME",
        "LOCATION",
    }

    def __init__(  # noqa: PLR0913
        self,
        analyzer: AnalyzerEngine = None,
        anonymizer: AnonymizerEngine = None,
        operators: dict[str, OperatorConfig] | None = None,
        masking_function: Callable[[str], str] | None = None,
        allow_list: list[str] | None = None,
        pii_entities: list[str] | None = None,
        **kwargs: dict,
    ) -> None:
        """Initialize the Presidio Handler.

        Args:
        ----
            analyzer (AnalyzerEngine): presidio analyzer engine
            anonymizer (AnonymizerEngine): presidio anonymizer engine
            operators (dict[str, OperatorConfig]): operators for masking
            masking_function (Callable[[str], str]): function to mask the text
            allow_list (list[str]): list of allowed entities
            pii_entities (list[str]): list of entities to detect
            **kwargs: keyword arguments

        """
        super().__init__(**kwargs)

        self.allow_list = allow_list or []
        self.allow_list = [a_clean for a in self.allow_list if (a_clean := a.strip())]

        if pii_entities:
            self._PII_ENTITIES = pii_entities

        self.analyzer = analyzer  # or PresidioMultilingualAnalyzer().analyzer
        self.anonymizer = anonymizer or AnonymizerEngine()
        self.operators = operators or {
            entity: OperatorConfig("replace", {"new_value": f"<{entity}>"})
            for entity in self._PII_ENTITIES
        }

        # The masking function can be used to mask path on dictionaries of parts of dictionaries.
        # This can be different from the operator used in the anonymizer.
        if masking_function:
            self.operators = {
                entity: OperatorConfig("custom", {"lambda": masking_function})
                for entity in self._PII_ENTITIES
            }
            self.masking_function = masking_function

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
    ) -> list:
        """Get entities in a text.

        Args:
        ----
            line (str): input text
            language (str): language of the text
            nlp_artifacts (list): nlp artifacts

        Returns:
        -------
            list: dictionary with entities detected in the text

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
            operator.add,
            (
                self._get_language_entities(line, lang)
                for lang in self.analyzer.supported_languages
            ),
        )
