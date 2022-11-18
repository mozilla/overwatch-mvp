import math
from abc import ABC, abstractmethod

import numpy as np
import pandas as pd
from pandas import DataFrame

from analysis.logging import logger


class DimensionEvaluator(ABC):
    @abstractmethod
    def evaluate(self) -> dict:
        pass

    @staticmethod
    def dimension_value_cols(df: DataFrame) -> list:
        """
        :param df:
        :return: list of 'dimension_value_n' columns in the provided dataframe
        """
        return [col for col in df.columns if "dimension_value_" in col]

    @staticmethod
    def dimension_cols(df: DataFrame) -> list:
        """
        :param df:
        :return: list of 'dimension_n' columns in the provided dataframe
        """
        return [col for col in df.columns if "dimension_" in col and "dimension_value_" not in col]

    @staticmethod
    def _calculate_percent_change(df: DataFrame) -> DataFrame:
        """
        :param df:
            Expected columns are ['dimension_value_n', 'metric_value', 'dimension_n', 'timeframe']
            - multiple 'dimension_value_n' columns contain the dimension values (e.g. 'ca').  The
             'n' is an integer index to account for multi dimensional processing.
            - 'metric_value' column contains the measure.
            - multiple 'dimension_n' column contains one value, the name of the dimension (e.g.
             'country'). The 'n' is an integer index to account for multi dimensional processing.
            - 'timeframe' column values are either "current" or "baseline".
        :return: df
            Columns are ['dimension_value_n', 'percent_change', 'dimension_n']
            - multiple 'dimension_value_n' columns contain the dimension values (e.g. 'ca') provided
             in input. Multiple dimension_value columns with numeric indicators may be present if
              the calculation is completed for multiple dimensions.
            - 'percent_change' contains the percent change from the baseline value to to current
             value for each dimension_value
            - 'dimension_n' columns contain the dimension name (e.g. country). Multiple dimension
             columns with numeric indicators may be present if the calculation is completed for
              multiple dimensions.
        """
        # Find the columns that contain dimension values so they can be set as the index.
        dimension_value_cols = DimensionEvaluator.dimension_value_cols(df)

        # TODO GLE sorting df to have baseline before current, hacky.
        prepared_df = (
            df.set_index(["timeframe"] + dimension_value_cols)["metric_value"]
            .unstack(dimension_value_cols)
            .sort_index()
        )
        # calc percent change and drop unneeded index, replace and drop np.inf values.
        # TODO GLE PerformanceWarning: dropping on a non-lexsorted multi-index without a level
        #  parameter may impact performance.
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

        # Carry the dimension label through by getting the columns that contain the dimension names.
        dimension_cols = DimensionEvaluator.dimension_cols(df)
        for col in dimension_cols:
            output[col] = df[col].values[0]
        return output

    @staticmethod
    def _contribution_to_overall_change(row) -> float:
        parent_current_value = row["parent_current"]
        parent_baseline_value = row["parent_baseline"]
        current_value = row["current"]
        baseline_value = row["baseline"]
        contribution = (
            (current_value - baseline_value) / abs(parent_baseline_value - parent_current_value)
        ) * 100
        return contribution

    def _calculate_contribution_to_overall_change(
        self, current_df: DataFrame, parent_df
    ) -> DataFrame:
        """
        :param current_df:
            Expected columns are ['dimension_value_n', 'metric_value', 'dimension_n', 'timeframe']
            - multiple 'dimension_value_n' columns contain the dimension values (e.g. 'ca').  The
             'n' is an integer index to account for multi dimensional processing.
            - 'metric_value' column contains the measure.
            - multiple 'dimension_n' column contains one value, the name of the dimension (e.g.
             'country'). The 'n' is an integer index to account for multi dimensional processing.
            - 'timeframe' column values are either "current" or "baseline".
        :param parent_df:
            Expected columns are ['metric_value', 'timeframe']
            'metric_value' column contains the measure.
             The 'n' is an integer index to account for multi dimensional processing.
            'timeframe' column values are either "current" or "baseline".
        :return: df
            Columns are ['dimension_value', 'contrib_to_overall', 'dimension']
            - multiple 'dimension_value_n' columns contain the dimension values (e.g. 'ca') provided
             in input. Multiple dimension_value columns with numeric indicators may be present if
             the calculation is completed for multiple dimensions.
            - 'contrib_to_overall' contains the percent any change in the metric for the
             dimension_value contributed to the overall amount of change in the metric value.
            - 'dimension_n' columns contain the dimension name (e.g. country). Multiple dimension
             columns with numeric indicators may be present if the calculation is completed for
              multiple dimensions.
        """

        parent_df_as_cols = parent_df.astype({"metric_value": "int64"}).pivot_table(
            columns=["timeframe"], values="metric_value"
        )
        dimension_value_cols = DimensionEvaluator.dimension_value_cols(current_df)
        dimension_cols = DimensionEvaluator.dimension_cols(current_df)

        current_df_as_cols = current_df.set_index(
            dimension_value_cols + dimension_cols + ["timeframe"]
        )["metric_value"].unstack("timeframe")

        # If the dimension value does not exist for both the current and baseline timelines then
        # Nan is assigned as the missing value during the unstack.  Setting these Nan to 0 since if
        # the dimension value is not available for that date then 0 represents the metric value
        # accurately.  app_version is an example of a dimension that may not hae the same value set
        # for both current and baseline.
        current_df_as_cols = current_df_as_cols.fillna(0)
        # Add the parent values to each row of the current_df
        current_df_as_cols["parent_baseline"] = parent_df_as_cols["baseline"][0]
        current_df_as_cols["parent_current"] = parent_df_as_cols["current"][0]

        # Calculate the contribution to overall change
        contrib_to_overall_change = current_df_as_cols.apply(
            self._contribution_to_overall_change, axis=1
        ).rename("contrib_to_overall_change")

        # Add the calculation to the current_df and pull dimension value out of index
        result = pd.merge(
            current_df_as_cols, contrib_to_overall_change, on=dimension_value_cols
        ).reset_index()
        # Carry the dimension label through
        for col in dimension_cols:
            result[col] = current_df[col].values[0]

        result = (
            result[dimension_value_cols + ["contrib_to_overall_change"] + dimension_cols]
            .sort_values(
                by="contrib_to_overall_change",
                key=abs,
                ascending=False,
                ignore_index=True,
            )
            .round(4)
        )
        sum = result["contrib_to_overall_change"].sum()
        logger.info(f"sum of contrib_to_overall_change: {sum} (should = 100)")
        assert abs(round(sum, 1)) == 100.0
        return result

    @staticmethod
    def _change_to_contribution(row) -> float:
        # Sum of all should = 0
        parent_current_value = row["parent_current"]
        parent_baseline_value = row["parent_baseline"]
        current_value = row["current"]
        baseline_value = row["baseline"]

        change_to_contrib = (
            (current_value / parent_current_value) - (baseline_value / parent_baseline_value)
        ) * 100
        return change_to_contrib

    def _calculate_change_to_contribution(self, current_df: DataFrame, parent_df) -> DataFrame:
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
            Expected columns are ['dimension_value_n', 'metric_value', 'dimension_n', 'timeframe']
            - multiple 'dimension_value_n' columns contain the dimension values (e.g. 'ca').  The
             'n' is an integer index to account for multi dimensional processing.
            - 'metric_value' column contains the measure.
            - multiple 'dimension_n' column contains one value, the name of the dimension (e.g.
             'country'). The 'n' is an integer index to account for multi dimensional processing.
            - 'timeframe' column values are either "current" or "baseline".
        :param parent_df:
            Expected columns are ['metric_value', 'timeframe']
            'metric_value' column contains the measure.
             The 'n' is an integer index to account for multi dimensional processing.
            'timeframe' column values are either "current" or "baseline".
        :return: df
            Columns are ['dimension_value', 'change_to_contrib', 'dimension']
            - multiple 'dimension_value_n' columns contain the dimension values (e.g. 'ca') provided
             in input. Multiple dimension_value columns with numeric indicators may be present if
              the calculation is completed for multiple dimensions.
            - 'change_to_contrib' contains the change in contribution of that dimension value to the
                overall total
            - 'dimension_n' columns contain the dimension name (e.g. country). Multiple dimension
             columns with numeric indicators may be present if the calculation is completed for
              multiple dimensions.
        """
        parent_df_as_cols = parent_df.astype({"metric_value": "int64"}).pivot_table(
            columns=["timeframe"], values="metric_value"
        )
        dimension_value_cols = DimensionEvaluator.dimension_value_cols(current_df)
        dimension_cols = DimensionEvaluator.dimension_cols(current_df)

        current_df_as_cols = current_df.set_index(
            dimension_value_cols + dimension_cols + ["timeframe"]
        )["metric_value"].unstack("timeframe")
        # If the dimension value does not exist for both the current and baseline timelines then
        # Nan is assigned as the missing value during the unstack.  Setting these Nan to 0 since if
        # the dimension value is not available for that date then 0 represents the metric value
        # accurately.  app_version is an example of a dimension that may not hae the same value set
        # for both current and baseline.
        current_df_as_cols = current_df_as_cols.fillna(0)

        # Add the parent values to each row of the current_df
        current_df_as_cols["parent_baseline"] = parent_df_as_cols["baseline"][0]
        current_df_as_cols["parent_current"] = parent_df_as_cols["current"][0]

        # Calculate the contribution to overall change
        change_to_contrib = current_df_as_cols.apply(self._change_to_contribution, axis=1).rename(
            "change_to_contrib"
        )

        # Add the calculation to the current_df and pull dimension value out of index
        result = pd.merge(
            current_df_as_cols, change_to_contrib, on=dimension_value_cols
        ).reset_index()
        # Carry the dimension label through
        for col in dimension_cols:
            result[col] = current_df[col].values[0]

        result = (
            result[dimension_value_cols + ["change_to_contrib"] + dimension_cols]
            .sort_values(
                by="change_to_contrib",
                key=abs,
                ascending=False,
                ignore_index=True,
            )
            .round(8)
            #  Changed rounding from 4 to 8 due to change_to_contribution for
            #  country/app_verision summing to 0.2681999999999969  It is a relatively high
            #  cardinality data set resulting in a cumulative rounding error.
            #  This will have to be considered in final implementation.
        )

        sum = result["change_to_contrib"].sum()
        logger.info(f"sum of change_to_contrib: {sum} (should = 0)")
        assert round(sum, 1) == 0.0
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
        # represents r from the significance equation.
        # parent_ratio is the change ratio between the baseline and the current of the parent node.
        # The expectation is that the changes by each dimension should match the change ratio of
        # the parent. If it does not, it is likely that the dimension value is anomalous.
        parent_ratio = parent_current_value / parent_baseline_value
        # represents r * v_b
        # Using the baseline value, multiply it by the expected ratio of the parent change. This is
        # the expected current value.
        expected_baseline_value = parent_ratio * baseline_value

        # represents v_c/(r * v_b) from significance equation
        expected_ratio = current_value / expected_baseline_value

        # represents
        # (v_c/(r * v_b) - 1) * (contribution_c/contribution_all) + 1
        # from significance equation
        weighted_expected_ratio = (expected_ratio - 1) * contribution + 1

        # represents
        # log((v_c/(r * v_b) - 1) * (contribution_c/contribution_all) + 1)
        # from significance equation
        log_exp_ratio = math.log(weighted_expected_ratio)

        # represents
        # (v_c - r * v_b) * log((v_c/(r * v_b) - 1) * (contribution_c/contribution_all) + 1)
        # from significance equation
        significance = (current_value - expected_baseline_value) * log_exp_ratio
        return significance

    def _calculate_significance(self, current_df: DataFrame, parent_df) -> DataFrame:
        """

         :param current_df:
            Expected columns are ['dimension_value_n', 'metric_value', 'dimension_n', 'timeframe']
            - multiple 'dimension_value_n' columns contain the dimension values (e.g. 'ca').  The
             'n' is an integer index to account for multi dimensional processing.
            - 'metric_value' column contains the measure.
            - multiple 'dimension_n' column contains one value, the name of the dimension (e.g.
             'country'). The 'n' is an integer index to account for multi dimensional processing.
            - 'timeframe' column values are either "current" or "baseline".
        :param parent_df:
            Expected columns are ['metric_value', 'timeframe']
            'metric_value' column contains the measure.
             The 'n' is an integer index to account for multi dimensional processing.
            'timeframe' column values are either "current" or "baseline".
        :return:
        Columns are ['dimension_value_n', 'percent_significance', 'dimension_n']
        - multiple 'dimension_value_n' columns contain the dimension values (e.g. 'ca') provided
             in input. Multiple dimension_value columns with numeric indicators may be present if
              the calculation is completed for multiple dimensions.
        - 'percent_significance' column contains the significance of the change in dimension value.
        - 'dimension_n' columns contain the dimension name (e.g. country). Multiple dimension
             columns with numeric indicators may be present if the calculation is completed for
              multiple dimensions.
        """

        parent_df_as_cols = parent_df.astype({"metric_value": "int64"}).pivot_table(
            columns=["timeframe"], values="metric_value"
        )

        dimension_value_cols = DimensionEvaluator.dimension_value_cols(current_df)
        dimension_cols = DimensionEvaluator.dimension_cols(current_df)

        current_df_as_cols = current_df.set_index(
            dimension_value_cols + dimension_cols + ["timeframe"]
        )["metric_value"].unstack("timeframe")

        # Add the parent values to each row of the current_df
        current_df_as_cols["parent_baseline"] = parent_df_as_cols["baseline"][0]
        current_df_as_cols["parent_current"] = parent_df_as_cols["current"][0]

        # Calculate the contribution to overall change
        dimension_value_significance = current_df_as_cols.apply(self._significance, axis=1).rename(
            "significance"
        )

        # Add the calculation to the current_df and pull dimension value out of index
        result = pd.merge(
            current_df_as_cols, dimension_value_significance, on=dimension_value_cols
        ).reset_index()
        result = result.replace([np.inf, -np.inf], np.nan)
        result["significance"] = result["significance"].fillna(0)
        # Carry the dimension label through
        for col in dimension_cols:
            result[col] = current_df[col].values[0]

        total_significance = result["significance"].sum()
        result["percent_significance"] = 100 * result["significance"] / total_significance

        result = (
            result[dimension_value_cols + ["percent_significance"] + dimension_cols]
            .sort_values(
                by="percent_significance",
                key=abs,
                ascending=False,
                ignore_index=True,
            )
            .round(4)
        )
        return result
