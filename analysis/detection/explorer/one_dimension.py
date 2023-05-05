from functools import reduce

import pandas as pd
from pandas import DataFrame

from analysis.data.metric import MetricLookupManager
from analysis.detection.explorer.dimension_evaluator import DimensionEvaluator
from analysis.configuration.configs import AnalysisProfile
from analysis.configuration.processing_dates import ProcessingDateRange


class OneDimensionEvaluator(DimensionEvaluator):
    def __init__(
        self,
        profile: AnalysisProfile,
        baseline_period: ProcessingDateRange,
        current_period: ProcessingDateRange,
        parent_df: DataFrame,
    ):
        super().__init__(parent_df)
        # Currently the profile only references percent_change.
        self.profile = profile
        self.baseline_period = baseline_period
        self.current_period = current_period

    def _get_current_and_baseline_values(self, dimension: str) -> DataFrame:
        """
        Retrieves the current and baseline values for the metric specified in the AnalysisProfile.
        Although all dimensions are included in the AnalysisProfile, only the dimension specified as
         the parameter is calculated.
        :param dimensions:  Dimension to calculate data on.  Must have len = 1.
        :return: Dataframe containing the current and baseline values.  Dataframe columns are
            'dimension_value' column contains the dimension values (e.g. 'ca').
            'metric_value' column contains the measure.
            'dimension' column contains one value, the name of the dimension (e.g. 'country').
            'timeframe' column values are either "current" or "baseline".
        """

        # For the one dimension evaluator if we are given a list we process each one separately.
        current_by_dimension = MetricLookupManager().get_metric_by_dimensions_with_date_range(
            metric_name=self.profile.dataset.metric_name,
            table_name=self.profile.dataset.table_name,
            app_name=self.profile.dataset.app_name,
            date_range=self.current_period,
            dimensions=[dimension],
            excluded_dimensions=self.profile.percent_change.exclude_dimension_values,
        )
        current_by_dimension["timeframe"] = "current"
        baseline_by_dimension = MetricLookupManager().get_metric_by_dimensions_with_date_range(
            metric_name=self.profile.dataset.metric_name,
            table_name=self.profile.dataset.table_name,
            app_name=self.profile.dataset.app_name,
            date_range=self.baseline_period,
            dimensions=[dimension],
            excluded_dimensions=self.profile.percent_change.exclude_dimension_values,
        )
        baseline_by_dimension["timeframe"] = "baseline"

        # the BigQuery package uses type 'Int64' as the type.  For dropna() to work the type needs
        # to be 'int64' (lowercase).  'Int64' handles missing values implicitly so dropna() has no
        # effect
        df = pd.concat([current_by_dimension, baseline_by_dimension]).astype(
            {"metric_value": "int64"}
        )
        return df

    def evaluate(self) -> dict:
        """
        Runs an evaluation of the specified dimensions individually.
        :return: a dict containing one dataframe for all evaluated metrics.  The data frame is
         sorted by percent change results, not dimension resulting in mixed order of dimensions
          (if more than 1 dimension has been calculated).
        """
        contrib_to_overall_change_threshold = (
            self.profile.percent_change.contrib_to_overall_change_threshold_percent
        )
        large_contrib_to_change = {}

        for dimension in self.profile.percent_change.dimensions:
            values = self._get_current_and_baseline_values(dimension=dimension)
            percent_change_df = self._calculate_percent_change(df=values)
            diff_df = self._calculate_diff(df=values)
            contrib_to_overall_change_df = self._calculate_contribution_to_overall_change(
                current_df=values
            )
            change_in_proportion_df = self._calculate_change_in_proportion(current_df=values)
            change_distance_df = self._calculate_change_distance(
                contrib_to_overall_change_df=contrib_to_overall_change_df,
                change_in_proportion_df=change_in_proportion_df,
            )

            dimension_value_cols = DimensionEvaluator.dimension_value_cols(df=values)
            dimension_cols = DimensionEvaluator.dimension_cols(df=values)
            raw_values = values.astype({"metric_value": "int64"}).pivot_table(
                "timeframe", dimension_cols + dimension_value_cols, "timeframe"
            )

            data_frames = [
                raw_values,
                percent_change_df,
                diff_df,
                contrib_to_overall_change_df,
                change_in_proportion_df,
                change_distance_df,
            ]

            merge_cols = DimensionEvaluator.dimension_cols(
                percent_change_df
            ) + DimensionEvaluator.dimension_value_cols(percent_change_df)
            result = reduce(
                lambda left, right: pd.merge(left, right, on=merge_cols),
                data_frames,
            ).reset_index()

            large_contrib_to_change[dimension] = (
                result[
                    abs(result["contrib_to_overall_change"]) > contrib_to_overall_change_threshold
                ]
                .sort_values(
                    by=self.profile.percent_change.sort_by,
                    key=abs,
                    ascending=False,
                    ignore_index=True,
                )
                .round(self.profile.percent_change.results_rounding)
                .head(self.profile.percent_change.limit_results)
            )

        return {"dimension_calc": large_contrib_to_change}
