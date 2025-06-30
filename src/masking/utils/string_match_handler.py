import datetime
import re
from typing import ClassVar

from presidio_analyzer import Pattern, PatternRecognizer


class StringMatchHandler:
    # Entities to be detected as PII
    _PII_ENTITIES: ClassVar[set[str]] = {"PATIENT_DATA"}

    def __init__(self, pii_cols: list[str] | None = None, **kwargs: dict) -> None:
        """Initialize the Presidio Handler.

        Args:
        ----
            pii_cols (list[str]): list of PII columns
            **kwargs: The keyword arguments

        """
        super().__init__(**kwargs)

        self.pii_cols = pii_cols or []

    def _get_pii_values(self, line: dict) -> list[str]:
        return [
            # pii_value
            pii_value
            for col in self.pii_cols
            if (pii_value := str(line.get(col))) not in self.allow_list
        ]

    def _get_pattern_recognizer(self, pii_values: list) -> PatternRecognizer | None:
        """Get the pattern recognizer.

        Args:
        ----
            pii_values (list): The PII values

        Returns:
        -------
            PatternRecognizer: The pattern recognizer

        """
        patterns = []
        for v in pii_values:
            if not v:
                continue

            if isinstance(v, str):
                v = v.strip()  # noqa: PLW2901
                if not v:
                    continue

                patterns.append(
                    # Create a pattern for the PII entity
                    Pattern(
                        self._PII_ENTITIES, regex=rf"(?i)\b{re.escape(v)}\b", score=0.8
                    )
                )
                continue

            if isinstance(v, datetime.datetime):
                patterns.extend([
                    Pattern(
                        self._PII_ENTITIES,
                        regex=rf"(?i){re.escape(v.strftime('%Y-%m-%d'))}",
                        score=0.8,
                    ),
                    Pattern(
                        self._PII_ENTITIES,
                        regex=rf"(?i){re.escape(v.strftime('%Y.%m.%d'))}",
                        score=0.8,
                    ),
                    Pattern(
                        self._PII_ENTITIES,
                        regex=rf"(?i){re.escape(v.strftime('%Y/%m/%d'))}",
                        score=0.8,
                    ),
                ])
                continue

        if not patterns:
            return None

        return PatternRecognizer(
            supported_entity=next(iter(self._PII_ENTITIES)), patterns=patterns
        )
