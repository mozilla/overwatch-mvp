from pandas import DataFrame
from pandas.testing import assert_frame_equal

from analysis.detection.explorer.one_dimension import OneDimensionEvaluator


def test_percent_change(
    mock_parent_df, mock_baseline_period, mock_current_period, dimension_df, mock_analysis_profile
):
    rows = [
        ["mx", 26.6667, "country"],
        ["ca", 14.2857, "country"],
        ["us", 1.25, "country"],
    ]
    cols = ["dimension_value_0", "percent_change", "dimension_0"]
    expected_df = DataFrame(rows, columns=cols)

    percent_change = OneDimensionEvaluator(
        profile=mock_analysis_profile,
        baseline_period=mock_baseline_period,
        current_period=mock_current_period,
        parent_df=mock_parent_df,
    )._calculate_percent_change(df=dimension_df)
    assert_frame_equal(expected_df, percent_change)


def test_calculate_contribution_to_overall_change(
    mock_baseline_period,
    mock_current_period,
    dimension_df,
    mock_parent_df,
    mock_analysis_profile,
):
    # calculation =
    # 100 * (current_value - baseline_value) / abs((parent_baseline_value - parent_current_value))
    rows = [
        ["mx", 50.00, "country"],
        ["ca", 37.50, "country"],
        ["us", 12.50, "country"],
    ]
    cols = ["dimension_value_0", "contrib_to_overall_change", "dimension_0"]
    expected_df = DataFrame(rows, columns=cols)

    contr_to_change = OneDimensionEvaluator(
        profile=mock_analysis_profile,
        baseline_period=mock_baseline_period,
        current_period=mock_current_period,
        parent_df=mock_parent_df,
    )._calculate_contribution_to_overall_change(current_df=dimension_df)

    assert_frame_equal(expected_df, contr_to_change)


def test_change_in_proportion(
    mock_baseline_period,
    mock_current_period,
    dimension_df,
    mock_parent_df,
    mock_analysis_profile,
):
    # calculation =
    # 100 * ((current_value/parent_current_value) - (baseline_value/parent_baseline_value))
    # sorted by abs value.
    rows = [
        ["us", -3.6429366, "country"],
        ["mx", 2.39154616, "country"],
        ["ca", 1.25139043, "country"],
    ]

    cols = ["dimension_value_0", "change_in_proportion", "dimension_0"]
    expected_df = DataFrame(rows, columns=cols)

    change_in_proportion = OneDimensionEvaluator(
        profile=mock_analysis_profile,
        baseline_period=mock_baseline_period,
        current_period=mock_current_period,
        parent_df=mock_parent_df,
    )._calculate_change_in_proportion(current_df=dimension_df)

    assert_frame_equal(expected_df, change_in_proportion)


def test_diff(
    mock_baseline_period,
    mock_current_period,
    dimension_df,
    mock_parent_df,
    mock_analysis_profile,
):
    rows = [
        ["mx", 4.0, "country"],
        ["ca", 3.0, "country"],
        ["us", 1.0, "country"],
    ]

    cols = ["dimension_value_0", "diff", "dimension_0"]
    expected_df = DataFrame(rows, columns=cols)

    diff = OneDimensionEvaluator(
        profile=mock_analysis_profile,
        baseline_period=mock_baseline_period,
        current_period=mock_current_period,
        parent_df=mock_parent_df,
    )._calculate_diff(df=dimension_df)

    assert_frame_equal(expected_df, diff)
