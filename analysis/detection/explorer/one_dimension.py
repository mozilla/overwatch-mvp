import math
from datetime import datetime, timedelta
from functools import reduce

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
            f" (should = 100)"
        )
        return result

    @staticmethod
    def _change_to_contribution(row) -> float:
        # Sum of all should = 0
        parent_current_value = row["parent_current"]
        parent_baseline_value = row["parent_baseline"]
        current_value = row["current"]
        baseline_value = row["baseline"]

        change_to_contrib = (
            (current_value / parent_current_value)
            - (baseline_value / parent_baseline_value)
        ) * 100
        return change_to_contrib

    def _calculate_change_to_contribution(
        self, current_df: DataFrame, parent_df
    ) -> DataFrame:
        """
        The change to contribution tracks how much the dimension value changed wrt the overall.
        If the value is negative then the dimension value is contributing less than it was.
        If it is positive then the dimension value is contributing more than it was.
        If the the overall percent change is negative, and the change in contribution value is
        positive that means other dimensions dropped more substantially, such that the positive
        dimension now makes up a higher percentage of teh overall value (the opposite is also true).

        calculation =100 * (
            (current_value/parent_current_value) - (baseline_value/parent_baseline_value)
            )

        :param current_df:
            current_df structure ['dimension_value', 'metric_value', 'dimension', 'timeframe']
        :param parent_df:
        :return: df
            Columns are ['dimension_value', 'change_to_contrib', 'dimension']
            'dimension_value' column contains the dimension values (e.g. 'ca') provided in input.
            'change_to_contrib' contains the change in contribution of that dimension value to the
                overall total
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
        change_to_contrib = current_df_as_cols.apply(
            self._change_to_contribution, axis=1
        ).rename("change_to_contrib")

        # Add the calculation to the current_df and pull dimension value out of index
        result = pd.merge(
            current_df_as_cols, change_to_contrib, on="dimension_value"
        ).reset_index()
        # Carry the dimension label through
        result["dimension"] = current_df["dimension"].values[0]

        result = (
            result[["dimension_value", "change_to_contrib", "dimension"]]
            .sort_values(
                by="change_to_contrib",
                key=abs,
                ascending=False,
                ignore_index=True,
            )
            .round(4)
        )

        print(
            f"sum of change_to_contrib: {result['change_to_contrib'].sum()} (should = 0)"
        )
        return result

    @staticmethod
    def _significance(row) -> float:
        # baseline  current  parent_baseline  parent_current
        # return row["baseline"] + row["current"] + row["parent_baseline"] + row["parent_current"]
        parent_current_value = row["parent_current"]
        parent_baseline_value = row["parent_baseline"]
        current_value = row["current"]
        baseline_value = row["baseline"]

        # TODO GLE size == value but need to verify if this is always the case.
        # using the blog post as a guide, this calculation represents
        # contribution_c/contribution_all from the significance equation
        contribution = (baseline_value + current_value) / (
            parent_baseline_value + parent_current_value
        )
        print(f"contribution: {contribution}")
        # represents r from the significance equation.
        # parent_ratio is the change ratio between the baseline and the current of the parent node.
        # The expectation is that the changes by each dimension should match the change ratio of
        # the parent. If it does not, it is likely that the dimension value is anomalous.
        parent_ratio = parent_current_value / parent_baseline_value
        # parent_ratio = 1.04651162
        print(f"parent_ratio: {parent_ratio}")
        # represents r * v_b
        # Using the baseline value, multiply it by the expected ratio of the parent change. This is
        # the expected current value.
        expected_baseline_value = parent_ratio * baseline_value
        print(f"expected_baseline_value: {expected_baseline_value}")

        # represents v_c/(r * v_b) from significance equation
        expected_ratio = current_value / expected_baseline_value
        print(f"expected_ratio: {expected_ratio}")

        # represents
        # (v_c/(r * v_b) - 1) * (contribution_c/contribution_all) + 1
        # from significance equation
        weighted_expected_ratio = (expected_ratio - 1) * contribution + 1
        print(f"weighted_expected_ratio: {weighted_expected_ratio}")

        # represents
        # log((v_c/(r * v_b) - 1) * (contribution_c/contribution_all) + 1)
        # from significance equation
        log_exp_ratio = math.log(weighted_expected_ratio)
        print(f"log_exp_ratio: {log_exp_ratio}")

        # represents
        # (v_c - r * v_b) * log((v_c/(r * v_b) - 1) * (contribution_c/contribution_all) + 1)
        # from significance equation
        significance = (current_value - expected_baseline_value) * log_exp_ratio
        print(f"significance: {significance}")
        print("*******************")
        return significance

    def _calculate_significance(self, current_df: DataFrame, parent_df) -> DataFrame:

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
        dimension_value_significance = current_df_as_cols.apply(
            self._significance, axis=1
        ).rename("significance")

        # Add the calculation to the current_df and pull dimension value out of index
        result = pd.merge(
            current_df_as_cols, dimension_value_significance, on="dimension_value"
        ).reset_index()
        result = result.replace([np.inf, -np.inf], np.nan)
        result["significance"] = result["significance"].fillna(0)
        # Carry the dimension label through
        result["dimension"] = current_df["dimension"].values[0]
        total_significance = result["significance"].sum()
        result["percent_significance"] = (
            100 * result["significance"] / total_significance
        )
        result = (
            result[
                [
                    "dimension_value",
                    "percent_significance",
                    "dimension",
                ]
            ]
            .sort_values(
                by="percent_significance",
                key=abs,
                ascending=False,
                ignore_index=True,
            )
            .round(4)
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
            contrib_to_overall_change_df = (
                self._calculate_contribution_to_overall_change(
                    parent_df=top_level_df, current_df=values
                )
            )
            change_to_contrib = self._calculate_change_to_contribution(
                parent_df=top_level_df, current_df=values
            )

            significance = self._calculate_significance(
                parent_df=top_level_df, current_df=values
            )
            data_frames = [
                percent_change_df,
                contrib_to_overall_change_df,
                change_to_contrib,
                significance,
            ]
            result = reduce(
                lambda left, right: pd.merge(
                    left, right, on=["dimension", "dimension_value"]
                ),
                data_frames,
            ).reset_index()
            changes.append(result)

        all_dims_calcs = pd.concat(changes).sort_values(
            by=[
                "percent_significance",
                "contrib_to_overall_change",
                "change_to_contrib",
                "percent_change",
            ],
            key=abs,
            ascending=False,
            ignore_index=True,
        )

        return {"dimension_calc": all_dims_calcs}
