from pandas import DataFrame
from pandas._testing import assert_frame_equal

from analysis.detection.explorer.multiple_dimensions import MultiDimensionEvaluator


def test_percent_change(
    mock_previous_date_range, mock_current_date_range, multi_dimension_df, mock_analysis_profile
):
    rows = [
        ["mx", "nightly", 100.0, "country", "channel"],
        ["us", "nightly", -80.0, "country", "channel"],
        ["mx", "beta", 66.6667, "country", "channel"],
        ["ca", "release", 63.6364, "country", "channel"],
        ["ca", "nightly", -50.0, "country", "channel"],
        ["ca", "beta", -33.3333, "country", "channel"],
        ["us", "beta", -16.6667, "country", "channel"],
        ["us", "release", 11.1111, "country", "channel"],
        ["mx", "release", 0.0, "country", "channel"],
    ]
    cols = [
        "dimension_value_0",
        "dimension_value_1",
        "percent_change",
        "dimension_0",
        "dimension_1",
    ]
    expected_df = DataFrame(rows, columns=cols)

    percent_change = MultiDimensionEvaluator(
        profile=mock_analysis_profile,
        previous_date_range=mock_previous_date_range,
        current_date_range=mock_current_date_range,
    )._calculate_percent_change(df=multi_dimension_df)
    assert_frame_equal(expected_df, percent_change)


def test_calculate_contribution_to_overall_change(
    mock_previous_date_range,
    mock_current_date_range,
    multi_dimension_df,
    parent_df,
    mock_analysis_profile,
):
    # calculation =
    # 100 * (current_value - baseline_value) / abs(parent_baseline_value - parent_current_value)
    rows = [
        ["ca", "release", 87.5, "country", "channel"],
        ["us", "release", 87.5, "country", "channel"],
        ["us", "nightly", -50.0, "country", "channel"],
        ["ca", "beta", -25.0, "country", "channel"],
        ["ca", "nightly", -25, "country", "channel"],
        ["mx", "beta", 25.0, "country", "channel"],
        ["mx", "nightly", 25.0, "country", "channel"],
        ["us", "beta", -25.0, "country", "channel"],
        ["mx", "release", 0.0, "country", "channel"],
    ]
    cols = [
        "dimension_value_0",
        "dimension_value_1",
        "contrib_to_overall_change",
        "dimension_0",
        "dimension_1",
    ]
    expected_df = DataFrame(rows, columns=cols)

    contr_to_change = MultiDimensionEvaluator(
        profile=mock_analysis_profile,
        previous_date_range=mock_previous_date_range,
        current_date_range=mock_current_date_range,
    )._calculate_contribution_to_overall_change(current_df=multi_dimension_df, parent_df=parent_df)

    assert_frame_equal(expected_df, contr_to_change)


def test_change_to_contribution(
    mock_previous_date_range,
    mock_current_date_range,
    multi_dimension_df,
    parent_df,
    mock_analysis_profile,
):
    # calculation =
    # 100 * ((current_value/parent_current_value) - (baseline_value/parent_baseline_value))
    # sorted by abs value.
    rows = [
        ["ca", "release", 5.03337041, "country", "channel"],
        ["us", "nightly", -3.50389321, "country", "channel"],
        ["us", "beta", -2.28031146, "country", "channel"],
        ["us", "release", 2.14126808, "country", "channel"],
        ["ca", "beta", -1.94660734, "country", "channel"],
        ["ca", "nightly", -1.83537264, "country", "channel"],
        ["mx", "nightly", 1.50166852, "country", "channel"],
        ["mx", "beta", 1.44605117, "country", "channel"],
        ["mx", "release", -0.55617353, "country", "channel"],
    ]

    cols = [
        "dimension_value_0",
        "dimension_value_1",
        "change_to_contrib",
        "dimension_0",
        "dimension_1",
    ]
    expected_df = DataFrame(rows, columns=cols)

    change_to_contrib = MultiDimensionEvaluator(
        profile=mock_analysis_profile,
        previous_date_range=mock_previous_date_range,
        current_date_range=mock_current_date_range,
    )._calculate_change_to_contribution(current_df=multi_dimension_df, parent_df=parent_df)

    assert_frame_equal(expected_df, change_to_contrib)


def test_calculate_significance(
    mock_previous_date_range,
    mock_current_date_range,
    multi_dimension_df,
    parent_df,
    mock_analysis_profile,
):
    rows = [
        ["ca", "release", 52.5817, "country", "channel"],
        ["us", "nightly", 12.0897, "country", "channel"],
        ["us", "beta", 7.8223, "country", "channel"],
        ["us", "release", 7.7773, "country", "channel"],
        ["mx", "nightly", 5.4358, "country", "channel"],
        ["ca", "beta", 5.1703, "country", "channel"],
        ["mx", "beta", 4.4875, "country", "channel"],
        ["ca", "nightly", 4.1317, "country", "channel"],
        ["mx", "release", 0.5038, "country", "channel"],
    ]
    cols = [
        "dimension_value_0",
        "dimension_value_1",
        "percent_significance",
        "dimension_0",
        "dimension_1",
    ]

    expected_df = DataFrame(rows, columns=cols)

    significance = MultiDimensionEvaluator(
        profile=mock_analysis_profile,
        previous_date_range=mock_previous_date_range,
        current_date_range=mock_current_date_range,
    )._calculate_significance(current_df=multi_dimension_df, parent_df=parent_df)

    assert_frame_equal(expected_df, significance)
