from datetime import datetime
import pytz
from analysis.configuration.processing_dates import calculate_date_ranges


def test_calculate_date_ranges(mock_analysis_profile):
    airflow_date = datetime.strptime("2022-04-17", "%Y-%m-%d").replace(tzinfo=pytz.utc)
    mock_analysis_profile.dataset.date_range_offset = 7
    mock_analysis_profile.dataset.previous_date_range = 7
    mock_analysis_profile.dataset.current_date_range = 7

    previous_date_range, recent_date_range = calculate_date_ranges(
        mock_analysis_profile.dataset, airflow_date
    )

    assert previous_date_range.start_date.strftime("%Y-%m-%d") == "2022-04-03"
    assert previous_date_range.end_date.strftime("%Y-%m-%d") == "2022-04-10"

    assert recent_date_range.start_date.strftime("%Y-%m-%d") == "2022-04-10"
    assert recent_date_range.end_date.strftime("%Y-%m-%d") == "2022-04-17"


def test_calculate_date_ranges_overlapping(mock_analysis_profile):
    airflow_date = datetime.strptime("2022-04-17", "%Y-%m-%d").replace(tzinfo=pytz.utc)
    mock_analysis_profile.dataset.date_range_offset = 6
    mock_analysis_profile.dataset.previous_date_range = 7
    mock_analysis_profile.dataset.current_date_range = 7

    previous_date_range, recent_date_range = calculate_date_ranges(
        mock_analysis_profile.dataset, airflow_date
    )

    assert previous_date_range.start_date.strftime("%Y-%m-%d") == "2022-04-04"
    assert previous_date_range.end_date.strftime("%Y-%m-%d") == "2022-04-11"

    assert recent_date_range.start_date.strftime("%Y-%m-%d") == "2022-04-10"
    assert recent_date_range.end_date.strftime("%Y-%m-%d") == "2022-04-17"


def test_calculate_date_ranges_with_gap(mock_analysis_profile):
    airflow_date = datetime.strptime("2022-04-30", "%Y-%m-%d").replace(tzinfo=pytz.utc)
    mock_analysis_profile.dataset.date_range_offset = 14
    mock_analysis_profile.dataset.previous_date_range = 7
    mock_analysis_profile.dataset.current_date_range = 7

    previous_date_range, recent_date_range = calculate_date_ranges(
        mock_analysis_profile.dataset, airflow_date
    )

    assert previous_date_range.start_date.strftime("%Y-%m-%d") == "2022-04-09"
    assert previous_date_range.end_date.strftime("%Y-%m-%d") == "2022-04-16"

    assert recent_date_range.start_date.strftime("%Y-%m-%d") == "2022-04-23"
    assert recent_date_range.end_date.strftime("%Y-%m-%d") == "2022-04-30"


def test_calculate_date_ranges_single_day(mock_analysis_profile):
    airflow_date = datetime.strptime("2022-04-17", "%Y-%m-%d").replace(tzinfo=pytz.utc)
    mock_analysis_profile.dataset.date_range_offset = 14
    mock_analysis_profile.dataset.previous_date_range = 1
    mock_analysis_profile.dataset.current_date_range = 1

    previous_date_range, recent_date_range = calculate_date_ranges(
        mock_analysis_profile.dataset, airflow_date
    )

    assert previous_date_range.start_date.strftime("%Y-%m-%d") == "2022-04-02"
    assert previous_date_range.end_date.strftime("%Y-%m-%d") == "2022-04-03"

    assert recent_date_range.start_date.strftime("%Y-%m-%d") == "2022-04-16"
    assert recent_date_range.end_date.strftime("%Y-%m-%d") == "2022-04-17"
