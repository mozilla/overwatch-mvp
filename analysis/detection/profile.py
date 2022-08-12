class Detection:
    def __init__(
        self,
        metric_name,
        date_of_interest,
        threshold_percent,
        baseline_value,
        current_value,
    ):
        self.metric_name = metric_name
        self.date_of_interest = date_of_interest
        self.threshold_percent = threshold_percent
        self.baseline_value = baseline_value
        self.current_value = current_value
        self._creation_time = None


# The original intent was for the AP to be generic and re-usable, unlikely to be that simple.
class AnalysisProfile:
    # The defaults were added here since there are 2 ways to use the AnalysisProfile
    def __init__(
        self,
        metric_name: str,
        table_name: str,
        app_name: str = None,
        threshold_percent: float = 0.0,
        historical_days_for_compare: int = 0,
        index_fields: list = None,
        dimensions: list = None,
        sort_by: list = None,
    ):
        self.metric_name = metric_name
        self.app_name = app_name
        self.table_name = table_name
        self.threshold_percent = threshold_percent
        self.historical_days_for_compare = historical_days_for_compare
        self.index_fields = index_fields
        self.dimensions = dimensions
        self.sort_by = sort_by
