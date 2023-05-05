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

    def _get_current_and_baseline_values_excluded_dim_values_only(self) -> DataFrame:
        current_df = MetricLookupManager().get_metric_with_date_range(
            metric_name=self.profile.dataset.metric_name,
            table_name=self.profile.dataset.table_name,
            app_name=self.profile.dataset.app_name,
            date_range=self.current_period,
            included_dimensions_only=self.profile.percent_change.exclude_dimension_values,
        )
        current_df["timeframe"] = "current"

        baseline_df = MetricLookupManager().get_metric_with_date_range(
            metric_name=self.profile.dataset.metric_name,
            table_name=self.profile.dataset.table_name,
            app_name=self.profile.dataset.app_name,
            date_range=self.baseline_period,
            included_dimensions_only=self.profile.percent_change.exclude_dimension_values,
        )
        baseline_df["timeframe"] = "baseline"
        return pd.concat([current_df, baseline_df])

    def _get_current_and_baseline_values_dim_values_excluded(self) -> DataFrame:
        current_df = MetricLookupManager().get_metric_with_date_range(
            metric_name=self.profile.dataset.metric_name,
            table_name=self.profile.dataset.table_name,
            app_name=self.profile.dataset.app_name,
            date_range=self.current_period,
            excluded_dimensions=self.profile.percent_change.exclude_dimension_values,
        )
        current_df["timeframe"] = "current"
        baseline_df = MetricLookupManager().get_metric_with_date_range(
            metric_name=self.profile.dataset.metric_name,
            table_name=self.profile.dataset.table_name,
            app_name=self.profile.dataset.app_name,
            date_range=self.baseline_period,
            excluded_dimensions=self.profile.percent_change.exclude_dimension_values,
        )
        baseline_df["timeframe"] = "baseline"
        return pd.concat([current_df, baseline_df])

    @staticmethod
    def _calculate_diff(df: DataFrame) -> float:
        """
        :param df:
            Expected columns are ["metric_value", "timeframe"]
                'timeframe' column values are either "current" or "baseline".
                'metric_value' column contains the measure.


        :return:
            float: difference from baseline to current value
        """
        # TODO GLE sorting df to have baseline before current, hacky.
        df = df.set_index("timeframe").sort_index()
        diff = df.diff().dropna()["metric_value"].values[0]
        return diff

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

    def _evaluate(self, get_values, key_suffix="") -> dict:
        values = get_values().round(self.profile.percent_change.results_rounding)
        percent_change = self._calculate_percent_change(df=values).round(
            self.profile.percent_change.results_rounding
        )
        diff = self._calculate_diff(df=values)
        return {
            f"top_level_percent_change{key_suffix}": percent_change,
            f"top_level_values{key_suffix}": values,
            f"top_level_diff{key_suffix}": diff,
        }

    def evaluate(self) -> dict:
        return self._evaluate(get_values=self._get_current_and_baseline_values)

    def evaluate_excluded_dimension_values_only(self) -> dict:
        if len(self.profile.percent_change.exclude_dimension_values) > 0:
            return self._evaluate(
                get_values=self._get_current_and_baseline_values_excluded_dim_values_only,
                key_suffix="_dimension_values_only",
            )
        return {}

    def evaluate_dimension_values_excluded(self) -> dict:
        if len(self.profile.percent_change.exclude_dimension_values) > 0:
            return self._evaluate(
                get_values=self._get_current_and_baseline_values_dim_values_excluded,
                key_suffix="_dimension_values_excluded",
            )
        return {}
