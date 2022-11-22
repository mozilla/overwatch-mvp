import pandas as pd
from analysis.detection.explorer.dimension_evaluator import DimensionEvaluator


class AllDimensionEvaluator(DimensionEvaluator):
    def __init__(
        self,
        one_dim_evaluation: dict,
        multi_dim_evaluation: dict,
    ):
        self.one_dim_evaluation = (one_dim_evaluation,)
        self.multi_dim_evaluation = (multi_dim_evaluation,)

    def evaluate(self) -> dict:

        all_dim_df = pd.DataFrame()

        for dimension, df in self.one_dim_evaluation[0]["dimension_calc"].items():

            df["dimension"] = df["dimension_0"]
            df["dimension_value"] = df["dimension_value_0"]
            all_dim_df = pd.concat([all_dim_df, df])
            all_dim_df.drop(columns=["dimension_0", "dimension_value_0"], inplace=True)

        for dimension, df in self.multi_dim_evaluation[0]["multi_dimension_calc"].items():

            dimension_cols = DimensionEvaluator.dimension_cols(df)
            df["dimension"] = df[dimension_cols].apply(
                lambda row: " | ".join(row.values.astype(str)), axis=1
            )

            dimension_value_cols = DimensionEvaluator.dimension_value_cols(df)
            df["dimension_value"] = df[dimension_value_cols].apply(
                lambda row: " | ".join(row.values.astype(str)), axis=1
            )

            all_dim_df = pd.concat(
                [all_dim_df, df.drop(columns=dimension_cols + dimension_value_cols)]
            )

        all_dim_df.sort_values(by="change_distance", ascending=False, inplace=True)

        # TODO: returning only top 5 results for now.
        #  Perhaps this could be configurable in the future.
        return {"overall_change_calc": all_dim_df.head(5)}
