from datetime import datetime

from analysis.detection.explorer.top_level import TopLevelEvaluator
from analysis.configuration.processing_dates import ProcessingDateRange


def test_percent_change(parent_df, mock_analysis_profile):
    previous_date_range = ProcessingDateRange(
        start_date=datetime.strptime("2022-04-02", "%Y-%m-%d"),
        end_date=datetime.strptime("2022-04-02", "%Y-%m-%d"),
    )
    recent_date_range = ProcessingDateRange(
        start_date=datetime.strptime("2022-04-09", "%Y-%m-%d"),
        end_date=datetime.strptime("2022-04-09", "%Y-%m-%d"),
    )
    expected_percent = round(100 * (124 - 116) / 116, 4)

    percent_change = TopLevelEvaluator(
        profile=mock_analysis_profile,
        previous_date_range=previous_date_range,
        current_date_range=recent_date_range,
    )._calculate_percent_change(df=parent_df)
    assert percent_change == expected_percent
