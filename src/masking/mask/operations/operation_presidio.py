import warnings
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import ClassVar

import pandas as pd
import spacy
from masking.presidio_recognizers import Recognizers, allow_list
from presidio_analyzer import AnalyzerEngine
from presidio_analyzer.nlp_engine import SpacyNlpEngine

from .operation import Operation
from masking.delimiters import DELIMITERS

# Ignore the specific FutureWarnings from torch.load
warnings.filterwarnings(
    "ignore", message=".*weights_only=False.*", category=FutureWarning
)
# Ignore the specific FutureWarnings from torch.cuda.amp.autocast
warnings.filterwarnings(
    "ignore", message=".*torch.cuda.amp.autocast.*", category=FutureWarning
)


class LoadedSpacyNlpEngine(SpacyNlpEngine):
    def __init__(self, models: dict[str, str]) -> None:
        """Initialize the LoadedSpacyNlpEngine class.

        Args:
        ----
            models (dict[str, str]): dictionary with language and model name.
                    Model should be a list of spaCy models, with corresponding language abbreviation:
                    for example, {"de": "de_core_news_lg", "en": "en_core_web_trf"}.

        """
        super().__init__()
        self.nlp = {
            lang: spacy.load(
                model,
                disable=[
                    "tok2vec",
                    "tagger",
                    "parser",
                    "attribute_ruler",
                    "lemmatizer",
                ],
            )
            for lang, model in models.items()
        }


class HashPresidio(Operation):
    """Hashes a text using hashlib algorithm and presidio to detect entities."""

    col_name: str  # column name to be hashed

    # Hashing function
    masking_function: Callable[[str], str]  # function to hash the input string

    # Spacy model to detect entities
    nlp_engine: LoadedSpacyNlpEngine  # spaCy model to detect entities
    analyzer: AnalyzerEngine  # presidio analyzer engine

    # Entities to be detected as PII
    _ENTITIES: ClassVar[set[str]] = {"EMAIL_ADDRESS", "PERSON", "PHONE_NUMBER"}

    _ENTITIES_TO_BE_FILTERED: ClassVar[list[str]] = {
        recon.supported_entities[0]
        for lang in Recognizers
        for recon in Recognizers[lang]
    }

    _PII_ENTITIES: ClassVar[list[str]] = list(_ENTITIES.union(_ENTITIES_TO_BE_FILTERED))

    def __init__(
        self,
        col_name: str,
        masking_function: Callable[[str], str],
        model: dict[str, str] | None = None,
        delimiter: str = "<<>>",
    ) -> None:
        """Initialize the HashTextSHA256 class.

        Args:
        ----
            col_name (str): column name to be hashed
            model (str): spaCy model to detect entities
            masking_function (Collable[[str],str], optional): function to hash the input string.

        """
        self.col_name = col_name
        self.masking_function = masking_function
        self.delimiter = DELIMITERS.get(delimiter,{'start':'<<','end':'>>'})

        if model is None:
            model = {"de": "de_core_news_lg", "en": "en_core_web_trf"}

        try:
            self.nlp_engine = LoadedSpacyNlpEngine(models=model)
            self.analyzer = AnalyzerEngine(
                nlp_engine=self.nlp_engine, supported_languages=list(model.keys())
            )

        except Exception as e:
            msg = f"Error loading spaCy model for Presidio: {e}"
            raise ValueError(msg) from e

        # Define the executor for multiple languages
        self.executor = ThreadPoolExecutor(max_workers=min(len(model), 2))

    def _get_language_entities(self, line: str, language: str) -> set[str]:
        """Get entities in a text.

        Args:
        ----
            line (str): input text
            language (str): language of the text

        Returns:
        -------
            dict[str, set[str]]: dictionary with entities detected in the text

        """
        results = self.analyzer.analyze(
            text=line,
            language=language,
            entities=self._PII_ENTITIES,
            allow_list=list(allow_list),
        )

        return {line[detected.start : detected.end].strip() for detected in results}

    def _get_entities(self, line: str) -> list[str]:
        """Get entities in text for each language.

        Args:
        ----
            line (str): input text

        Returns:
        -------
            dict[str, set[str]]: dictionary with entities detected in the text

        """
        languages = self.nlp_engine.nlp.keys()

        # Parallelize the process
        entities = list(
            self.executor.map(
                lambda lang: self._get_language_entities(line, lang), languages
            )
        )

        # Merge results: merge the sets of entities, and convert into a list
        results = list(set().union(*entities))
        return sorted(results, key=len, reverse=True)

    def _mask_entities_line(self, line: str) -> str:
        """Mask entities in a text.

        Args:
        ----
            line (str): input text

        Returns:
        -------
            str: text with entities masked

        """
        # Detect entities in the line
        results = self._get_entities(line)

        # Create concordance table for the line
        line_concordance_table = {}

        for entity in results:
            if entity not in self.concordance_table:
                masked = self.masking_function(entity)

                while masked in line_concordance_table.values():
                    print(  # noqa: T201
                        f"Collision: Masked entity {masked} already exists in the concordance table. Regenerating..."
                    )
                    masked = self.masking_function(entity)

                line_concordance_table[entity] = masked

        # Mask entities
        for entity in results:
            masked_entity = line_concordance_table.get(
                entity, self.concordance_table.get(entity, entity)
            )
            delimited_masked_entity = self.delimiter['start'] + masked_entity + self.delimiter['end']
            line = line.replace(entity, delimited_masked_entity)

        return line, line_concordance_table

    def _mask_entities(self, line: str) -> str:
        """Mask entities in a text.

        Args:
        ----
            line (str): input text

        Returns:
        -------
            str: text with entities masked

        """
        # Detect entities in the line
        line, line_concordance_table = self._mask_entities_line(line)

        # Add to the concordance table the newly found entities
        self.concordance_table.update(line_concordance_table)

        return line

    def _mask_data(self, data: pd.DataFrame | pd.Series) -> pd.DataFrame | pd.Series:
        """Mask the data.

        Args:
        ----
            data (pd.DataFrame or pd.Series): input dataframe or series

        Returns:
        -------
            pd.DataFrame or pd.Series: dataframe or series with masked column

        """
        # Apply masking function to the data
        if isinstance(data, pd.Series):
            # Extract indexes of not-na values
            return data.apply(lambda x: self._mask_entities(x) if pd.notna(x) else x)

        data[self.col_name] = data[self.col_name].apply(
            lambda x: self._mask_entities(x) if pd.notna(x) else x
        )
        return data
