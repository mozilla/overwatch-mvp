import itertools
from functools import reduce

import pandas as pd
from pandas import DataFrame

from analysis.data.metric import MetricLookupManager
from analysis.detection.explorer.dimension_evaluator import DimensionEvaluator
from analysis.detection.explorer.top_level import TopLevelEvaluator
from analysis.configuration.configs import AnalysisProfile
from analysis.configuration.processing_dates import ProcessingDateRange

# TODO GLE only doing additive algm not the ratio algm.


class MultiDimensionEvaluator(DimensionEvaluator):
    def __init__(
        self,
        profile: AnalysisProfile,
        baseline_period: ProcessingDateRange,
        current_period: ProcessingDateRange,
    ):
        # TODO GLE currently the profile only references percent_change.
        self.profile = profile
        self.baseline_period = baseline_period
        self.current_period = current_period

    def _get_current_and_baseline_values(self, dimensions: list) -> DataFrame:
        """
        :param dimensions: list of pairs of dimensions
        :return: Dataframe containing the current and baseline values.  Dataframe columns are
            'dimension_value' column contains the dimension values (e.g. 'ca').
            'dimension_value_1'  etc
            'metric_value' column contains the measure.
            'dimension' column contains one value, the name of the dimension (e.g. 'country').
            'timeframe' column values are either "current" or "baseline".
        """
        current = MetricLookupManager().get_metric_by_dimensions_with_date_range(
            metric_name=self.profile.dataset.metric_name,
            table_name=self.profile.dataset.table_name,
            app_name=self.profile.dataset.app_name,
            date_range=self.current_period,
            dimensions=dimensions,
        )
        current["timeframe"] = "current"
        baseline = MetricLookupManager().get_metric_by_dimensions_with_date_range(
            metric_name=self.profile.dataset.metric_name,
            table_name=self.profile.dataset.table_name,
            app_name=self.profile.dataset.app_name,
            date_range=self.baseline_period,
            dimensions=dimensions,
        )
        baseline["timeframe"] = "baseline"

        # the BigQuery package uses type 'Int64' as the type.  For dropna() to work the type needs
        # to be 'int64' (lowercase).  'Int64' handles missing values implicitly so dropna() has no
        # effect
        df = pd.concat([current, baseline]).astype({"metric_value": "float64"})
        return df

    # TODO GLE Alot of this code can be combined with one_dimension.py
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

        # Skip the evaluation if permutation processing is not enabled,
        if not self.profile.percent_change.include_dimension_permutations:
            return {"multi_dimension_calc": large_contrib_to_change}

        # TODO GLE THIS IS VERY BAD NEED TO USE A CACHED VALUE
        # TODO the top level is a OneDimensionEvaluator?  For now use overall but may need to use a
        #  different evaluator and order the dimensions
        top_level_df = TopLevelEvaluator(
            profile=self.profile,
            baseline_period=self.baseline_period,
            current_period=self.current_period,
        )._get_current_and_baseline_values()

        # Get all permutations and filter duplicates (a, b) = (b, a)
        dim_permutations = itertools.permutations(self.profile.percent_change.dimensions, 2)
        dim_permutations = list(set(tuple(sorted(perm)) for perm in dim_permutations))

        for pair in dim_permutations:
            values = self._get_current_and_baseline_values(dimensions=list(pair))
            percent_change_df = self._calculate_percent_change(df=values)
            contrib_to_overall_change_df = self._calculate_contribution_to_overall_change(
                parent_df=top_level_df, current_df=values
            )
            change_in_proportion_df = self._calculate_change_in_proportion(
                parent_df=top_level_df, current_df=values
            )

            change_distance_df = self._calculate_change_distance(
                contrib_to_overall_change_df=contrib_to_overall_change_df,
                change_in_proportion_df=change_in_proportion_df,
            )

            data_frames = [
                change_distance_df,
                percent_change_df,
                contrib_to_overall_change_df,
                change_in_proportion_df,
            ]
            merge_cols = DimensionEvaluator.dimension_cols(
                percent_change_df
            ) + DimensionEvaluator.dimension_value_cols(percent_change_df)

            result = reduce(
                lambda left, right: pd.merge(left, right, on=merge_cols),
                data_frames,
            ).reset_index()

            # TODO GLE due to more combinations the threshold here might need to be lower.
            large_contrib_to_change[pair] = result[
                abs(result["contrib_to_overall_change"]) > contrib_to_overall_change_threshold
            ].sort_values(
                by=self.profile.percent_change.sort_by,
                key=abs,
                ascending=False,
                ignore_index=True,
            )

        return {"multi_dimension_calc": large_contrib_to_change}
