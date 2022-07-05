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
    def __init__(
        self,
        metric_name: str,
        threshold_percent: float,
        historical_days_for_compare: int,
    ):
        self.metric_name = metric_name
        self.threshold_percent = threshold_percent
        self.historical_days_for_compare = historical_days_for_compare
