import hashlib
import json
from collections import defaultdict
from collections.abc import Callable, Generator
from typing import ClassVar

from presidio_anonymizer import AnonymizerEngine, OperatorConfig

from masking.base_operations.entity_detection import (
    AnalyzerEngine,
    PresidioMultilingualAnalyzer,
)
from masking.base_operations.operation import Operation
from masking.utils.multi_nested_dict import MultiNestedDictHandler


class HashDictOperationBase(Operation, MultiNestedDictHandler):
    """Hashes a column using SHA256 algorithm."""

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
        col_name: str,
        masking_function: Callable = hashlib.sha256,
        allow_list: list[str] | None = None,
        analyzer: AnalyzerEngine | None = None,
        operators: dict[str, OperatorConfig] | None = None,
        **kwargs: dict,
    ) -> None:
        """Initialize the HashOperation class.

        Args:
        ----
            col_name (str): column name to be hashed
            secret (str): secret key to hash the line string
            masking_function (Callable): function to hash the line string
            allow_list (list[str]): list of strings to be allowed in the masking process.
            analyzer (AnalyzerEngine): analyzer engine to use
            operators (dict[str, OperatorConfig]): operators to be used in the masking process
            **kwargs (dict): keyword arguments

        """
        super().__init__(**kwargs)

        self.col_name = col_name
        self.masking_function = masking_function

        self.allow_list = allow_list if allow_list is not None else []

        self.analyzer = analyzer or PresidioMultilingualAnalyzer().analyzer

        self.concordance_table = {}

        self.operators = operators or {
            entity: OperatorConfig("custom", {"lambda": self.masking_function})
            for entity in self._PII_ENTITIES
        }

        self.anonymizer = AnonymizerEngine()

    def update_analyzer(self, analyzer: AnalyzerEngine) -> None:
        """Update the analyzer engine.

        Args:
        ----
            analyzer (AnalyzerEngine): analyzer engine to use

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

    def _get_leafs(self, line: dict | str) -> tuple[Generator[str, None, None]]:
        """Get the leafs of a nested dictionary.

        Args:
        ----
            line (dict): input dictionary

        Returns:
        -------
            tuple[Generator[str, None, None]]: generator of leafs

        """
        leafs_to_mask, leafs_to_deny = [], []
        for leaf in self._find_leaf_path(line):
            if self._is_denied_path(leaf):
                leafs_to_deny.append(leaf)
                continue

            leafs_to_mask.append(leaf)

        return leafs_to_mask, leafs_to_deny

    def _mask_line(
        self,
        line: dict | str,
        leaf_to_mask: tuple | None = None,
        leaf_to_deny: tuple | None = None,
        entities: dict | None = None,
    ) -> str:
        """Mask a single line represented by a dictionary.

        Args:
        ----
            line (dict): line line
            leaf_to_mask (tuple): leafs to mask
            leaf_to_deny (tuple): leafs to deny
            entities (dict): entities detected in the line

        Returns:
        -------
            str: masked line

        """
        if isinstance(line, str):
            try:
                line = json.loads(line)
            except json.JSONDecodeError:
                msg = "Failed to parse line, treat it as a string."
                print(msg)  # noqa: T201

        if leaf_to_deny is None and leaf_to_mask is None:
            leaf_to_mask, leaf_to_deny = self._get_leafs(line)

        for leaf in leaf_to_deny:
            value = self._get_leaf(line, self._undeny_path(leaf))
            masked = self.masking_function(
                value
                if isinstance(value, str)
                else json.dumps(value, ensure_ascii=False)
            )
            line = self._set_leaf(line, self._undeny_path(leaf), masked)

        # Process the leaf values
        if entities is None:
            entities = defaultdict(set)
            for lang in self.analyzer.supported_languages:
                for leaf, doc in self.analyzer.nlp_engine.process_batch(
                    [
                        self._get_leaf(line, leaf.split(self.path_separator))
                        for leaf in leaf_to_mask
                    ],
                    language=lang,
                ):
                    entities[leaf].update(self._get_language_entities(leaf, lang, doc))

        for leaf in leaf_to_mask:
            value = self._get_leaf(line, leaf)
            masked = self.anonymizer.anonymize(
                value, analyzer_results=list(entities[value]), operators=self.operators
            ).text
            line = self._set_leaf(line, leaf, masked)

        if isinstance(line, dict):
            return json.dumps(line, ensure_ascii=False)

        return line
