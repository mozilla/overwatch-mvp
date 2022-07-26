from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from pandas import DataFrame

from analysis.data.metric import MetricLookupManager
from analysis.detection.explorer.top_level import TopLevelEvaluator
from analysis.detection.profile import AnalysisProfile


class OneDimensionEvaluator:
    # TODO GLE may be able to determine the dim dynamically.
    def __init__(self, profile: AnalysisProfile, date_of_interest: datetime):
        self.profile = profile
        self.date_of_interest = date_of_interest

    def _get_current_and_baseline_values_for_dimension(
        self, dimension: str
    ) -> DataFrame:
        """
        Retrieves the current and baseline values for the metric specified in the AnalysisProfile.
        Although all dimensions are included in the AnalysisProfile, only the dimension specified as
         the parameter is calculated.
        :param dimension:  Dimension to calculate data on.
        :return: Dataframe containing the current and baseline values.  Dataframe columns are
            'dimension_value' column contains the dimension values (e.g. 'ca').
            'metric_value' column contains the measure.
            'dimension' column contains one value, the name of the dimension (e.g. 'country').
            'timeframe' column values are either "current" or "baseline".
        """
        # For the one dimension evaluator if we are given a list we process each one separately.
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
            Columns are ['dimension_value', 'percent_change', 'dimension']
            'dimension_value' column contains the dimension values (e.g. 'ca') provided in input.
            'percent_change' contains the percent change from the baseline value to to current value
             for each dimension_value
            'dimension' columns contains the dimension name (e.g. country)
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

        # Carry the dimension label through
        output["dimension"] = df["dimension"].values[0]
        return output

    @staticmethod
    def _contribution_to_overall_change(row) -> float:
        parent_current_value = row["parent_current"]
        parent_baseline_value = row["parent_baseline"]
        current_value = row["current"]
        baseline_value = row["baseline"]
        contribution = (
            (current_value - baseline_value)
            / (parent_current_value - parent_baseline_value)
            * 100
        )
        return contribution

    def _calculate_contribution_to_overall_change(
        self, current_df: DataFrame, parent_df
    ) -> DataFrame:
        """
        :param current_df:
            current_df structure ['dimension_value', 'metric_value', 'dimension', 'timeframe']
        :param parent_df:
        :return: df
            Columns are ['dimension_value', 'contrib_to_overall', 'dimension']
            'dimension_value' column contains the dimension values (e.g. 'ca') provided in input.
            'contrib_to_overall' contains the percent any change in the metric for the
             dimension_value contributed to the overall amount of change in the metric value.
            'dimension' columns contains the dimension name (e.g. country)
        """

        parent_df_as_cols = parent_df.astype({"metric_value": "int64"}).pivot_table(
            columns=["timeframe"], values="metric_value"
        )
        current_df_as_cols = current_df.set_index(
            (["dimension", "dimension_value", "timeframe"])
        )["metric_value"].unstack("timeframe")

        # Add the parent values to each row of the current_df
        current_df_as_cols["parent_baseline"] = parent_df_as_cols["baseline"][0]
        current_df_as_cols["parent_current"] = parent_df_as_cols["current"][0]

        # Calculate the contribution to overall change
        contrib_to_overall_change = current_df_as_cols.apply(
            self._contribution_to_overall_change, axis=1
        ).rename("contrib_to_overall_change")

        # Add the calculation to the current_df and pull dimension value out of index
        result = pd.merge(
            current_df_as_cols, contrib_to_overall_change, on="dimension_value"
        ).reset_index()
        # Carry the dimension label through
        result["dimension"] = current_df["dimension"].values[0]

        result = (
            result[["dimension_value", "contrib_to_overall_change", "dimension"]]
            .sort_values(
                by="contrib_to_overall_change",
                key=abs,
                ascending=False,
                ignore_index=True,
            )
            .round(4)
        )

        print(
            f"sum of contrib_to_overall_change: {result['contrib_to_overall_change'].sum()}"
        )
        return result

    def evaluate(self) -> dict:
        """
        Runs an evaluation of the specified dimensions individually.
        :return: a dict containing one dataframe for all evaluated metrics.  The data frame is
         sorted by percent change results, not dimension resulting in mixed order of dimensions
          (if more than 1 dimension has been calculated).
        """
        changes = []
        # TODO GLE THIS IS VERY BAD NEED TO USE A CACHED VALUE
        top_level_df = TopLevelEvaluator(
            profile=self.profile, date_of_interest=self.date_of_interest
        )._get_current_and_baseline_values()
        for dimension in self.profile.dimensions:
            values = self._get_current_and_baseline_values_for_dimension(
                dimension=dimension
            )
            percent_change_df = self._calculate_percent_change(df=values)
            contr_to_overall_change_df = self._calculate_contribution_to_overall_change(
                parent_df=top_level_df, current_df=values
            )
            result = pd.merge(
                percent_change_df,
                contr_to_overall_change_df,
                on=["dimension", "dimension_value"],
            ).reset_index()
            changes.append(result)

        all_dims_calcs = pd.concat(changes).sort_values(
            by=["contrib_to_overall_change", "percent_change"],
            key=abs,
            ascending=False,
            ignore_index=True,
        )

        return {"dimension_calc": all_dims_calcs}
