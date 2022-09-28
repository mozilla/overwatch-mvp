from datetime import datetime, timedelta

import numpy as np
from pandas import DataFrame

from analysis.detection.profile import AnalysisProfile, Detection


class Detector:
    def apply_profile(
        self, profile: AnalysisProfile, df: DataFrame, date_of_interest: datetime
    ) -> Detection:
        filtered = df.set_index(profile.index_fields)
        print(filtered)

        pct_change = filtered.pct_change()
        print(pct_change)

        # The column may not always be the metric_name
        # Need to check that the sign is the same
        triggered = pct_change[
            (abs(pct_change[profile.metric_name]) >= abs(profile.threshold_percent))
            & (np.sign(pct_change[profile.metric_name]) == np.sign(profile.threshold_percent))
        ]
        print(triggered)

        current_value = df[df["submission_date"] == date_of_interest][profile.metric_name].values[0]

        start_date = date_of_interest - timedelta(profile.historical_days_for_compare)
        baseline_value = df[df["submission_date"] == start_date][profile.metric_name].values[0]
        detection = Detection(
            profile.metric_name,
            date_of_interest,
            profile.threshold_percent,
            baseline_value,
            current_value,
        )
        return detection
