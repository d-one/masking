from masking.base_operations.operation_med_stats import MedStatsOperationBase
from masking.mask.operations.operation import PandasOperation


class MedStatsOperation(MedStatsOperationBase, PandasOperation):
    """Anonymize a column using MedStats regions."""
