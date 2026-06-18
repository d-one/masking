from masking.base_operations.operation_med_stats import MedStatsOperationBase
from masking.mask_polars.operations.operation import PolarsOperation


class MedStatsOperation(MedStatsOperationBase, PolarsOperation):
    """Anonymize a column using MedStats regions."""
