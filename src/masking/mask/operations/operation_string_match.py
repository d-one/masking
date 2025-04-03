import json
from collections.abc import Generator
from contextlib import suppress

from pandas import DataFrame, Series

from masking.base_operations.operation_string_match import StringMatchOperationBase
from masking.mask.operations.operation import PandasOperation


class StringMatchOperation(PandasOperation, StringMatchOperationBase):
    """String matching operation."""

    def _find_leafs_to_mask(self, line: Series) -> Generator[tuple, None, None]:
        """Find the leafs to mask.

        Args:
        ----
            line (Series): input line

        Yields:
        ------
            dict: leafs to mask

        """
        # Filter all the NaN or NaT values
        recognizer = self._get_pattern_recognizer(self._get_pii_values(line.dropna()))

        for path, leaf in self._find_leaf_path_and_value(line[self.col_name]):
            if self._is_denied_path(path):
                path = self._undeny_path(path)  # noqa: PLW2901
                dumped_leaf = (
                    json.dumps(leaf, ensure_ascii=False)
                    if isinstance(leaf, dict)
                    else leaf
                )
                yield (path, self.masking_function(dumped_leaf))
                continue

            if recognizer and (
                res := recognizer.analyze(leaf, entities=[self._PII_ENTITY])
            ):
                yield (
                    path,
                    self.anonymizer.anonymize(leaf, res, operators=self.operators).text,
                )

    def _substitute_leafs(
        self, line: dict | list | str, leafs: Generator[tuple, None, None]
    ) -> str:
        """Substitute the leafs.

        Args:
        ----
            line (dict): input line
            leafs (dict): leafs to substitute

        Returns:
        -------
            dict: substituted line

        """
        for path, leaf in leafs:
            if leaf:
                line = self._set_leaf(line, path, leaf)

        if isinstance(line, dict | list):
            return json.dumps(line, ensure_ascii=False)

        return line

    def _find_and_substitute(self, line: Series) -> str:
        """Find and substitute the values.

        Args:
        ----
            line (Series): input series

        Returns:
        -------
            str: masked string

        """
        with suppress(Exception):
            line[self.col_name] = json.loads(line[self.col_name], strict=False)

        leafs = self._find_leafs_to_mask(line)
        if not leafs:
            if isinstance(line[self.col_name], dict | list):
                return json.dumps(line[self.col_name], ensure_ascii=False)

            return line[self.col_name]

        return self._substitute_leafs(line[self.col_name], leafs)

    def _mask_data(self, data: DataFrame) -> Series:
        """Mask the data.

        Args:
        ----
            data (DataFrame): input dataframe

        Returns:
        -------
            Series: series with masked column

        """
        return data[self.serving_columns].apply(self._find_and_substitute, axis=1)
