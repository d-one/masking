from masking.base_operations.operation import Operation
from masking.utils.presidio_handler import PresidioHandler
from masking.utils.string_match_handler import StringMatchHandler


class StringMatchOperationBase(Operation, StringMatchHandler, PresidioHandler):
    """String Match Operation Base Class."""

    @property
    def serving_columns(self) -> list[str]:
        return [self.col_name, *self.pii_cols]

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
            res := recognizer.analyze(line, entities=list(self._PII_ENTITIES))
        ):
            return self.anonymizer.anonymize(line, res, operators=self.operators).text

        return line
