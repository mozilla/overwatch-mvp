from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from pandas import DataFrame

from analysis.data.metric import MetricLookupManager
from analysis.detection.profile import AnalysisProfile


class OneDimensionEvaluator:
    # TODO GLE may be able to determine the dim dynamically.
    def __init__(self, profile: AnalysisProfile, date_of_interest: datetime):
        self.profile = profile
        self.date_of_interest = date_of_interest

    def _get_current_and_baseline_values_for_dimensions(self):
        # For the one dimension evaluator if we are given a list we process each one separately.
        # TODO GLE will need to handle more than 1 dimension
        for dimension in self.profile.dimensions:
            current_by_dimension = (
                MetricLookupManager().get_data_for_metric_by_dimensions_with_date(
                    metric_name=self.profile.metric_name,
                    date_of_interest=self.date_of_interest,
                    dimensions=[dimension],
                )
            )
            current_by_dimension["timeframe"] = "current"
            baseline_by_dimension = (
                MetricLookupManager().get_data_for_metric_by_dimensions_with_date(
                    metric_name=self.profile.metric_name,
                    date_of_interest=self.date_of_interest
                    - timedelta(self.profile.historical_days_for_compare),
                    dimensions=[dimension],
                )
            )
            baseline_by_dimension["timeframe"] = "baseline"
        # the BigQuery package uses type 'Int64' as the type.  For dropna() to work the type needs
        # to be 'int64' (lowercase)
        df = pd.concat([current_by_dimension, baseline_by_dimension]).astype(
            {"metric_value": "int64"}
        )
        return df

    @staticmethod
    def _calculate_percent_change(df: DataFrame) -> DataFrame:
        """
        :param df:
            Expected columns are ['dimension_value', 'metric_value', 'dimension', 'timeframe']
            'dimension_value' column contains the dimension values (e.g. 'ca').
            'metric_value' column contains the measure.
            'dimension' column contains one value, the name of the dimension (e.g. 'country').
            'timeframe' column values are either "current" or "baseline".

        :return: df
            Columns are ['dimension_value', 'percent_change']
            'dimension_value' column contains the dimension values (e.g. 'ca') provided in input.
            'percent_change' contains the percent change from the baseline value to to current value
             for each dimension_value
        """
        # TODO GLE sorting df to have baseline before current, hacky.
        prepared_df = (
            df.set_index(["timeframe", "dimension_value"])["metric_value"]
            .unstack("dimension_value")
            .sort_index()
        )
        # calc percent change and drop unneeded index, replace and drop np.inf values.
        pct_change_df = (
            prepared_df.pct_change()
            .dropna(how="all")
            .reset_index()
            .drop(columns="timeframe")
            .replace([np.inf, -np.inf], np.nan)
            .dropna(axis="columns")
        )
        output = pct_change_df.T
        output.columns = ["percent_change"]
        # pull dimension value out of index
        output = output.reset_index()

        # Convert to percent from decimal
        output["percent_change"] = output["percent_change"].multiply(100)
        output = output.sort_values(
            by="percent_change", key=abs, ascending=False, ignore_index=True
        ).round(4)
        return output

    def evaluate(self) -> dict:
        values = self._get_current_and_baseline_values_for_dimensions()
        percent_change = self._calculate_percent_change(df=values)

        return {"dimension_percent_change": percent_change}
