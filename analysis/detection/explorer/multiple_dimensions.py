import itertools
from functools import reduce

import pandas as pd
from pandas import DataFrame

from analysis.data.metric import MetricLookupManager
from analysis.detection.explorer.dimension_evaluator import DimensionEvaluator
from analysis.detection.explorer.top_level import TopLevelEvaluator
from analysis.detection.profile import AnalysisProfile

# TODO GLE only doing additive algm not the ratio algm.


class MultiDimensionEvaluator(DimensionEvaluator):
    def __init__(self, profile: AnalysisProfile, date_ranges: dict):
        self.profile = profile
        self.date_ranges = date_ranges

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
        if len(dimensions) != 1:
            raise ValueError("Can only specify 1 set of dimensions")

        current = MetricLookupManager().get_metric_by_multi_dimensions_with_date_range(
            metric_name=self.profile.metric_name,
            table_name=self.profile.table_name,
            app_name=self.profile.app_name,
            date_range=self.date_ranges.get("recent_period"),
            dimensions=dimensions[0],
        )
        current["timeframe"] = "current"
        baseline = MetricLookupManager().get_metric_by_multi_dimensions_with_date_range(
            metric_name=self.profile.metric_name,
            table_name=self.profile.table_name,
            app_name=self.profile.app_name,
            date_range=self.date_ranges.get("previous_period"),
            dimensions=dimensions[0],
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
        contrib_to_overall_change_threshold = self.profile.threshold_percent
        large_contrib_to_change = {}

        # TODO GLE THIS IS VERY BAD NEED TO USE A CACHED VALUE
        # TODO the top level is a OneDimensionEvaluator?  For now use overall but may need to use a
        #  different evaluator and order the dimensions
        top_level_df = TopLevelEvaluator(
            profile=self.profile, date_ranges=self.date_ranges
        )._get_current_and_baseline_values()

        # Get all permutations and filter duplicates (a, b) = (b, a)
        dim_permutations = itertools.permutations(self.profile.dimensions, 2)
        dim_permutations = list(set(tuple(sorted(perm)) for perm in dim_permutations))

        for pair in dim_permutations:
            values = self._get_current_and_baseline_values(dimensions=[pair])
            percent_change_df = self._calculate_percent_change(df=values)
            contrib_to_overall_change_df = self._calculate_contribution_to_overall_change(
                parent_df=top_level_df, current_df=values
            )
            change_to_contrib = self._calculate_change_to_contribution(
                parent_df=top_level_df, current_df=values
            )

            significance = self._calculate_significance(parent_df=top_level_df, current_df=values)
            data_frames = [
                percent_change_df,
                contrib_to_overall_change_df,
                change_to_contrib,
                significance,
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
                by=self.profile.sort_by,
                key=abs,
                ascending=False,
                ignore_index=True,
            )

        return {"multi_dimension_calc": large_contrib_to_change}
