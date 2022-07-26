from datetime import datetime

from pandas import DataFrame
from pandas.testing import assert_frame_equal

from analysis.detection.explorer.one_dimension import OneDimensionEvaluator
from analysis.detection.profile import AnalysisProfile


def test_percent_change(dimension_df):
    date_of_interest = datetime.strptime("2022-04-09", "%Y-%m-%d")
    new_profiles_ap = AnalysisProfile(
        metric_name="new_profiles",
        dimensions=[
            "country",
        ],
    )

    rows = [
        ["mx", 26.6667, "country"],
        ["ca", 14.2857, "country"],
        ["us", 1.25, "country"],
    ]
    cols = ["dimension_value", "percent_change", "dimension"]
    expected_df = DataFrame(rows, columns=cols)

    percent_change = OneDimensionEvaluator(
        profile=new_profiles_ap, date_of_interest=date_of_interest
    )._calculate_percent_change(df=dimension_df)
    assert_frame_equal(expected_df, percent_change)


def test_calculate_contribution_to_overall_change(dimension_df, parent_df):
    date_of_interest = datetime.strptime("2022-04-09", "%Y-%m-%d")
    new_profiles_ap = AnalysisProfile(
        metric_name="new_profiles",
        dimensions=[
            "country",
        ],
    )

    # calculation was: (current_value - baseline_value) / (baseline_value - current_value)
    # however updated to 100 * (current_value - baseline_value) / (current_value - baseline_value)
    rows = [
        ["mx", 50.00, "country"],
        ["ca", 37.50, "country"],
        ["us", 12.50, "country"],
    ]
    cols = ["dimension_value", "contrib_to_overall_change", "dimension"]
    expected_df = DataFrame(rows, columns=cols)

    contr_to_change = OneDimensionEvaluator(
        profile=new_profiles_ap, date_of_interest=date_of_interest
    )._calculate_contribution_to_overall_change(
        current_df=dimension_df, parent_df=parent_df
    )

    assert_frame_equal(expected_df, contr_to_change)
