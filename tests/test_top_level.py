from datetime import datetime

from analysis.detection.explorer.top_level import TopLevelEvaluator


def test_percent_change(parent_df, mock_analysis_profile):
    date_ranges_of_interest = {
        # 7 day configuration, dates are inclusive.  Current max window average is 7 days.
        # For single day configuration set start_date = end_date.
        "previous_period": {
            "start_date": datetime.strptime("2022-04-02", "%Y-%m-%d"),
            "end_date": datetime.strptime("2022-04-02", "%Y-%m-%d"),
        },
        "recent_period": {
            "start_date": datetime.strptime("2022-04-09", "%Y-%m-%d"),
            "end_date": datetime.strptime("2022-04-09", "%Y-%m-%d"),
        },
    }

    expected_percent = round(100 * (124 - 116) / 116, 4)

    percent_change = TopLevelEvaluator(
        profile=mock_analysis_profile, date_ranges=date_ranges_of_interest
    )._calculate_percent_change(df=parent_df)
    assert percent_change == expected_percent
