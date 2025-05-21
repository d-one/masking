import datetime
import re
from collections.abc import Callable
from hashlib import sha256

from presidio_analyzer import Pattern, PatternRecognizer
from presidio_anonymizer import OperatorConfig

from masking.base_operations.operation import Operation
from masking.utils.presidio_handler import PresidioHandler


class StringMatchOperationBase(Operation, PresidioHandler):
    _PII_ENTITY = "PATIENT_DATA"

    def __init__(
        self,
        col_name: str,
        pii_cols: list[str] | None = None,
        masking_function: Callable = sha256,
        operators: dict[str, OperatorConfig] | None = None,
        **kwargs: dict,
    ) -> None:
        """Initialize the StringMatchingOperation.

        Args:
        ----
            col_name (str): The column name
            pii_cols (list): The PII columns
            masking_function (Callable): The masking function
            operators (dict): The operators
            **kwargs (dict): The keyword arguments

        """
        super().__init__(col_name=col_name, **kwargs)

        self.operators = operators or {
            self._PII_ENTITY: OperatorConfig("replace", {"new_value": "<MASKED>"})
        }

        self.pii_cols = pii_cols or []
        self.masking_function = masking_function

    @property
    def serving_columns(self) -> list[str]:
        return [self.col_name, *self.pii_cols]

    def _get_pii_values(self, line: dict) -> list[str]:
        return [
            pii_value
            for col in self.pii_cols
            if (pii_value := line.get(col)) not in self.allow_list
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
                    Pattern(
                        self._PII_ENTITY, regex=rf"(?i)\b{re.escape(v)}\b", score=0.8
                    )
                )
                continue

            if isinstance(v, datetime.datetime):
                patterns.extend([
                    Pattern(
                        self._PII_ENTITY,
                        regex=rf"(?i){re.escape(v.strftime('%Y-%m-%d'))}",
                        score=0.8,
                    ),
                    Pattern(
                        self._PII_ENTITY,
                        regex=rf"(?i){re.escape(v.strftime('%Y.%m.%d'))}",
                        score=0.8,
                    ),
                    Pattern(
                        self._PII_ENTITY,
                        regex=rf"(?i){re.escape(v.strftime('%Y/%m/%d'))}",
                        score=0.8,
                    ),
                ])
                continue

        if not patterns:
            return None

        return PatternRecognizer(supported_entity=self._PII_ENTITY, patterns=patterns)

    def _mask_line(
        self,
        line: str,
        additional_values: dict | None = None,
        **kwargs: dict | None,  # noqa: ARG002
    ) -> str:
        """Mask a single line.

        Args:
        ----
            line (str): input line
            additional_values (dict): additional values to mask
            kwargs (dict): keyword arguments

        Returns:
        -------
            str: masked line

        """
        # Filter all the NaN or NaT values
        recognizer = self._get_pattern_recognizer(
            self._get_pii_values(additional_values)
        )

        if recognizer and (
            res := recognizer.analyze(line, entities=[self._PII_ENTITY])
        ):
            return self.anonymizer.anonymize(line, res, operators=self.operators).text

        return line
