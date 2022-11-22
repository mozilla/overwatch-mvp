from pandas import DataFrame
from pandas.testing import assert_frame_equal

from analysis.detection.explorer.all_dimensions import AllDimensionEvaluator
from analysis.detection.explorer.one_dimension import OneDimensionEvaluator
from analysis.detection.explorer.multiple_dimensions import MultiDimensionEvaluator


def test_change_distance_one_dim(
    mock_baseline_period,
    mock_current_period,
    dimension_df,
    parent_df,
    mock_analysis_profile,
):
    # calculation =
    # sqrt((change_in_proportion^2) + (contrib_to_overall_change^2))
    # sorted by value descending.
    rows = [
        ["mx", "country", 50.0572],
        ["ca", "country", 37.5209],
        ["us", "country", 13.0200],
    ]

    cols = ["dimension_value_0", "dimension_0", "change_distance"]
    expected_df = DataFrame(rows, columns=cols)

    one_dim_evaluation = OneDimensionEvaluator(
        profile=mock_analysis_profile,
        baseline_period=mock_baseline_period,
        current_period=mock_current_period,
    )

    contr_to_change = one_dim_evaluation._calculate_contribution_to_overall_change(
        current_df=dimension_df, parent_df=parent_df
    )

    change_in_proportion = one_dim_evaluation._calculate_change_in_proportion(
        current_df=dimension_df, parent_df=parent_df
    )

    multi_dim_evaluation = MultiDimensionEvaluator(
        profile=mock_analysis_profile,
        baseline_period=mock_baseline_period,
        current_period=mock_current_period,
    )

    change_distance_df = AllDimensionEvaluator(
        one_dim_evaluation=one_dim_evaluation,
        multi_dim_evaluation=multi_dim_evaluation,
    )._calculate_change_distance(
        contrib_to_overall_change_df=contr_to_change, change_in_proportion_df=change_in_proportion
    )

    assert_frame_equal(expected_df, change_distance_df)


def test_change_distance_multi_dim(
    mock_baseline_period,
    mock_current_period,
    multi_dimension_df,
    parent_df,
    mock_analysis_profile,
):
    # calculation =
    # sqrt((change_in_proportion^2) + (contrib_to_overall_change^2))
    # sorted by value descending.
    rows = [
        ["ca", "release", "country", "channel", 87.6447],
        ["us", "release", "country", "channel", 87.5262],
        ["us", "nightly", "country", "channel", 50.1226],
        ["ca", "beta", "country", "channel", 25.0757],
        ["ca", "nightly", "country", "channel", 25.0673],
        ["mx", "beta", "country", "channel", 25.0418],
        ["mx", "nightly", "country", "channel", 25.0451],
        ["us", "beta", "country", "channel", 25.1038],
        ["mx", "release", "country", "channel", 0.5562],
    ]
    cols = [
        "dimension_value_0",
        "dimension_value_1",
        "dimension_0",
        "dimension_1",
        "change_distance",
    ]
    expected_df = DataFrame(rows, columns=cols)

    one_dim_evaluation = OneDimensionEvaluator(
        profile=mock_analysis_profile,
        baseline_period=mock_baseline_period,
        current_period=mock_current_period,
    )

    multi_dim_evaluation = MultiDimensionEvaluator(
        profile=mock_analysis_profile,
        baseline_period=mock_baseline_period,
        current_period=mock_current_period,
    )

    contr_to_change = multi_dim_evaluation._calculate_contribution_to_overall_change(
        current_df=multi_dimension_df,
        parent_df=parent_df,
    )

    change_in_proportion = multi_dim_evaluation._calculate_change_in_proportion(
        current_df=multi_dimension_df, parent_df=parent_df
    )

    change_distance_df = AllDimensionEvaluator(
        one_dim_evaluation=one_dim_evaluation,
        multi_dim_evaluation=multi_dim_evaluation,
    )._calculate_change_distance(
        contrib_to_overall_change_df=contr_to_change, change_in_proportion_df=change_in_proportion
    )

    assert_frame_equal(expected_df, change_distance_df)
