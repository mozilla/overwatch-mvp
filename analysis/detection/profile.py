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
