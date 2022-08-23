import pandas as pd
from pandas import DataFrame

from analysis.data.metric import MetricLookupManager
from analysis.detection.profile import AnalysisProfile


class TopLevelEvaluator:
    def __init__(self, profile: AnalysisProfile, date_ranges: dict):
        self.profile = profile
        self.date_ranges = date_ranges

    def _get_current_and_baseline_values(self) -> DataFrame:
        current_df = MetricLookupManager().get_data_for_metric_with_date_range(
            metric_name=self.profile.metric_name,
            table_name=self.profile.table_name,
            app_name=self.profile.app_name,
            date_range=self.date_ranges.get("recent_period"),
        )
        current_df["timeframe"] = "current"

        baseline_df = MetricLookupManager().get_data_for_metric_with_date_range(
            metric_name=self.profile.metric_name,
            table_name=self.profile.table_name,
            app_name=self.profile.app_name,
            date_range=self.date_ranges.get("previous_period"),
        )
        baseline_df["timeframe"] = "baseline"

        return pd.concat([current_df, baseline_df])

    @staticmethod
    def _calculate_percent_change(df: DataFrame) -> float:
        """
        :param df:
            Expected columns are ["metric_value", "timeframe"]
                'timeframe' column values are either "current" or "baseline".
                'metric_value' column contains the measure.


        :return:
            float: percent change from the baseline value to to current value.
        """
        # TODO GLE sorting df to have baseline before current, hacky.
        df = df.set_index("timeframe").sort_index()
        percent_change = round(
            df.pct_change().dropna()["metric_value"].values[0] * 100, 4
        )
        return percent_change

    def evaluate(self) -> dict:
        values = self._get_current_and_baseline_values()
        percent_change = self._calculate_percent_change(df=values)
        return {"top_level_percent_change": percent_change, "top_level_values": values}
