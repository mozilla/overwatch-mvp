from pandas import DataFrame
from pandas._testing import assert_frame_equal

from analysis.detection.explorer.multiple_dimensions import MultiDimensionEvaluator
from analysis.detection.profile import AnalysisProfile


def test_percent_change(date_ranges_of_interest, multi_dimension_df):
    new_profiles_ap = AnalysisProfile(
        metric_name="new_profiles",
        table_name="test",
        app_name="Fenix",
        dimensions=["country", "channel"],
    )

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
        profile=new_profiles_ap, date_ranges=date_ranges_of_interest
    )._calculate_percent_change(df=multi_dimension_df)
    assert_frame_equal(expected_df, percent_change)


def test_calculate_contribution_to_overall_change(
    date_ranges_of_interest, multi_dimension_df, parent_df
):
    new_profiles_ap = AnalysisProfile(
        metric_name="new_profiles",
        table_name="test",
        app_name="Fenix",
        dimensions=["country", "channel"],
    )

    # calculation =
    # 100 * (current_value - baseline_value) / (parent_baseline_value - parent_current_value)
    rows = [
        ["ca", "release", -87.5, "country", "channel"],
        ["us", "release", -87.5, "country", "channel"],
        ["us", "nightly", 50.0, "country", "channel"],
        ["ca", "beta", 25.0, "country", "channel"],
        ["ca", "nightly", 25, "country", "channel"],
        ["mx", "beta", -25.0, "country", "channel"],
        ["mx", "nightly", -25.0, "country", "channel"],
        ["us", "beta", 25.0, "country", "channel"],
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
        profile=new_profiles_ap, date_ranges=date_ranges_of_interest
    )._calculate_contribution_to_overall_change(
        current_df=multi_dimension_df, parent_df=parent_df
    )

    assert_frame_equal(expected_df, contr_to_change)


def test_change_to_contribution(date_ranges_of_interest, multi_dimension_df, parent_df):
    new_profiles_ap = AnalysisProfile(
        metric_name="new_profiles",
        table_name="test",
        app_name="Fenix",
        dimensions=["country", "channel"],
    )
    # calculation =
    # 100 * ((current_value/parent_current_value) - (baseline_value/parent_baseline_value))
    # sorted by abs value.
    rows = [
        ["ca", "release", 5.0334, "country", "channel"],
        ["us", "nightly", -3.5039, "country", "channel"],
        ["us", "beta", -2.2803, "country", "channel"],
        ["us", "release", 2.1413, "country", "channel"],
        ["ca", "beta", -1.9466, "country", "channel"],
        ["ca", "nightly", -1.8354, "country", "channel"],
        ["mx", "nightly", 1.5017, "country", "channel"],
        ["mx", "beta", 1.4461, "country", "channel"],
        ["mx", "release", -0.5562, "country", "channel"],
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
        profile=new_profiles_ap, date_ranges=date_ranges_of_interest
    )._calculate_change_to_contribution(
        current_df=multi_dimension_df, parent_df=parent_df
    )

    assert_frame_equal(expected_df, change_to_contrib)


def test_calculate_significance(date_ranges_of_interest, multi_dimension_df, parent_df):
    new_profiles_ap = AnalysisProfile(
        metric_name="new_profiles",
        app_name="Fenix",
        table_name="test",
        dimensions=[
            "country",
        ],
    )

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
        profile=new_profiles_ap, date_ranges=date_ranges_of_interest
    )._calculate_significance(current_df=multi_dimension_df, parent_df=parent_df)

    assert_frame_equal(expected_df, significance)
