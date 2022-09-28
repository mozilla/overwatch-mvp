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
        df = MetricLookupManager().get_data_for_metric_by_geolocation(
            metric_name=self.profile.metric_name,
            column_name=dimension_to_process,
            date_of_interest=self.date_of_interest,
            historical_days_for_compare=self.profile.historical_days_for_compare,
        )
        return df

    def run_evaluation(self) -> dict:
        evaluations = {}
        for processing_dim in self.dimensions_to_process:
            df = (
                self._get_metric_values_by_column(processing_dim).groupby(processing_dim)
                # 2 is used since only current and 1 day in history is used
                .filter(lambda x: len(x) == 2)
            )
            print(f"Processing dimension: {processing_dim}")
            # Up to here we also have a calc average which we are not using.
            # [self.profile.metric_name] after the set_index restricts the unstack to only the
            # column we're interested in.
            pct_change = (
                df.set_index(["submission_date", processing_dim])[self.profile.metric_name]
                .unstack([processing_dim])
                .pct_change()
                .dropna()
            )

            transposed = pct_change.T
            cleaned = transposed[transposed[self.date_of_interest].abs() < 1].sort_values(
                by=self.date_of_interest, key=abs, ascending=False
            )

            print(cleaned.head(10))
            result = list(cleaned.head(10).itertuples(index=True, name=None))
            evaluations[processing_dim] = result
            print(result)
        return evaluations
