from collections.abc import Callable

from masking.base_operations.operation import Operation


class NullOperationBase(Operation):
    """Mask a column by replacing values with None values."""

    secret: str  # secret key to hash the input string

    def __init__(
        self,
        col_name: str,
        condition: Callable[[str], bool] | None = None,
        **kwargs: dict,
    ) -> None:
        """Initialize the HashOperation class.

        Args:
        ----
            col_name (str): column name to be hashed
            condition (Callable[[str], bool], optional): condition to apply before masking
            **kwargs (dict): keyword arguments

        """
        super().__init__(col_name=col_name, **kwargs)

        if not any([isinstance(condition, Callable), condition is None]):
            msg = f"Invalid condition, expected a Callable with input 'str' and returning 'bool', got {type(condition)}"
            raise TypeError(msg)

        self.condition = condition or (
            lambda x: True  # noqa: ARG005
        )  # Default to a condition that always returns True

    def _mask_line(self, line: str | int, **kwargs: dict) -> str:  # noqa: ARG002
        """Mask a single line.

        Args:
        ----
            line (str): input line
            **kwargs (dict): keyword arguments

        Returns:
        -------
            str: masked line

        """
        if self.condition(str(line)):
            return None

        return str(line)  # Return the original line if condition is not met
