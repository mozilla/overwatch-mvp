from datetime import datetime

from analysis.data.metric import MetricLookupManager
from analysis.detection.profile import AnalysisProfile


class Geolocation:
    dimensions_to_process = ["region_name", "subregion_name", "country"]

    def __init__(self, profile: AnalysisProfile, date_of_interest: datetime):
        self.profile = profile
        self.date_of_interest = date_of_interest

    def _get_metric_values_by_column(self, dimension_to_process):
        # This DF is sorted by country the date so the pct function should still work
        df = MetricLookupManager().get_data_for_metric_with_column(
            metric_name=self.profile.metric_name,
            column_name=dimension_to_process,
            date_of_interest=self.date_of_interest,
            historical_days_for_compare=self.profile.historical_days_for_compare,
        )
        return df

    def run_evaluation(self) -> dict:
        evaluations = {}
        for processing_dim in self.dimensions_to_process:
            df = self._get_metric_values_by_column(processing_dim)
            print(f"Processing dimension: {processing_dim}")
            pct_change = (
                df.set_index(["submission_date", processing_dim])
                .new_profiles.unstack([processing_dim])
                .pct_change()
                .dropna()
            )

            transposed = pct_change.T
            cleaned = transposed[
                transposed[self.date_of_interest].abs() < 1
            ].sort_values(by=self.date_of_interest, key=abs, ascending=False)

            print(cleaned.head(10))
            result = list(cleaned.head(10).itertuples(index=True, name=None))
            evaluations[processing_dim] = result
            print(result)
        return evaluations
