import pandas as pd
from pandas import DataFrame

from analysis.data.metric import MetricLookupManager
from analysis.configuration.configs import AnalysisProfile
from analysis.configuration.processing_dates import ProcessingDateRange


class TopLevelEvaluator:
    def __init__(
        self,
        profile: AnalysisProfile,
        baseline_period: ProcessingDateRange,
        current_period: ProcessingDateRange,
    ):
        self.profile = profile
        self.baseline_period = baseline_period
        self.current_period = current_period

    def _get_current_and_baseline_values(self) -> DataFrame:
        current_df = MetricLookupManager().get_metric_with_date_range(
            metric_name=self.profile.dataset.metric_name,
            table_name=self.profile.dataset.table_name,
            app_name=self.profile.dataset.app_name,
            date_range=self.current_period,
        )
        current_df["timeframe"] = "current"

        baseline_df = MetricLookupManager().get_metric_with_date_range(
            metric_name=self.profile.dataset.metric_name,
            table_name=self.profile.dataset.table_name,
            app_name=self.profile.dataset.app_name,
            date_range=self.baseline_period,
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
        percent_change = round(df.pct_change().dropna()["metric_value"].values[0] * 100, 4)
        return percent_change

    def evaluate(self) -> dict:
        values = self._get_current_and_baseline_values().round(
            self.profile.percent_change.report_rounding
        )
        percent_change = self._calculate_percent_change(df=values).round(
            self.profile.percent_change.report_rounding
        )
        return {"top_level_percent_change": percent_change, "top_level_values": values}
