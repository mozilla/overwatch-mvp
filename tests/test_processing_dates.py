from datetime import datetime
import pytz
from analysis.configuration.processing_dates import calculate_date_ranges


def test_calculate_date_ranges(mock_analysis_profile):
    airflow_date = datetime.strptime("2022-04-17", "%Y-%m-%d").replace(tzinfo=pytz.utc)
    # resetting to 0 to verify no offset works.
    mock_analysis_profile.dataset.processing_period_offset = 0
    mock_analysis_profile.dataset.period_offset = 7
    mock_analysis_profile.dataset.baseline_period = 7
    mock_analysis_profile.dataset.current_period = 7

    baseline_period, current_period = calculate_date_ranges(
        mock_analysis_profile.dataset, airflow_date
    )

    assert baseline_period.start_date.strftime("%Y-%m-%d") == "2022-04-03"
    assert baseline_period.end_date.strftime("%Y-%m-%d") == "2022-04-10"

    assert current_period.start_date.strftime("%Y-%m-%d") == "2022-04-10"
    assert current_period.end_date.strftime("%Y-%m-%d") == "2022-04-17"


def test_calculate_date_ranges_overlapping(mock_analysis_profile):
    airflow_date = datetime.strptime("2022-04-17", "%Y-%m-%d").replace(tzinfo=pytz.utc)
    mock_analysis_profile.dataset.period_offset = 6
    mock_analysis_profile.dataset.baseline_period = 7
    mock_analysis_profile.dataset.current_period = 7

    baseline_period, current_period = calculate_date_ranges(
        mock_analysis_profile.dataset, airflow_date
    )

    assert baseline_period.start_date.strftime("%Y-%m-%d") == "2022-04-03"
    assert baseline_period.end_date.strftime("%Y-%m-%d") == "2022-04-10"

    assert current_period.start_date.strftime("%Y-%m-%d") == "2022-04-09"
    assert current_period.end_date.strftime("%Y-%m-%d") == "2022-04-16"


def test_calculate_date_ranges_with_gap(mock_analysis_profile):
    airflow_date = datetime.strptime("2022-04-30", "%Y-%m-%d").replace(tzinfo=pytz.utc)
    mock_analysis_profile.dataset.period_offset = 14
    mock_analysis_profile.dataset.baseline_period = 7
    mock_analysis_profile.dataset.current_period = 7

    baseline_period, current_period = calculate_date_ranges(
        mock_analysis_profile.dataset, airflow_date
    )

    assert baseline_period.start_date.strftime("%Y-%m-%d") == "2022-04-08"
    assert baseline_period.end_date.strftime("%Y-%m-%d") == "2022-04-15"

    assert current_period.start_date.strftime("%Y-%m-%d") == "2022-04-22"
    assert current_period.end_date.strftime("%Y-%m-%d") == "2022-04-29"


def test_calculate_date_ranges_single_day(mock_analysis_profile):
    airflow_date = datetime.strptime("2022-04-17", "%Y-%m-%d").replace(tzinfo=pytz.utc)
    mock_analysis_profile.dataset.period_offset = 14
    mock_analysis_profile.dataset.baseline_period = 1
    mock_analysis_profile.dataset.current_period = 1

    baseline_period, current_period = calculate_date_ranges(
        mock_analysis_profile.dataset, airflow_date
    )

    assert baseline_period.start_date.strftime("%Y-%m-%d") == "2022-04-01"
    assert baseline_period.end_date.strftime("%Y-%m-%d") == "2022-04-02"

    assert current_period.start_date.strftime("%Y-%m-%d") == "2022-04-15"
    assert current_period.end_date.strftime("%Y-%m-%d") == "2022-04-16"


def test_processing_period_offset(mock_analysis_profile):
    airflow_date = datetime.strptime("2022-04-17", "%Y-%m-%d").replace(tzinfo=pytz.utc)
    # resetting to 0 to verify 0 offset works.
    mock_analysis_profile.dataset.processing_period_offset = 0
    mock_analysis_profile.dataset.period_offset = 7
    mock_analysis_profile.dataset.baseline_period = 7
    mock_analysis_profile.dataset.current_period = 7

    baseline_period, current_period = calculate_date_ranges(
        mock_analysis_profile.dataset, airflow_date
    )
    assert baseline_period.start_date.strftime("%Y-%m-%d") == "2022-04-03"
    assert baseline_period.end_date.strftime("%Y-%m-%d") == "2022-04-10"
    assert current_period.start_date.strftime("%Y-%m-%d") == "2022-04-10"
    assert current_period.end_date.strftime("%Y-%m-%d") == "2022-04-17"

    # Set offset period to 3 days, all dates shift back by 3 days.
    mock_analysis_profile.dataset.processing_period_offset = 3
    baseline_period, current_period = calculate_date_ranges(
        mock_analysis_profile.dataset, airflow_date
    )
    assert baseline_period.start_date.strftime("%Y-%m-%d") == "2022-03-31"
    assert baseline_period.end_date.strftime("%Y-%m-%d") == "2022-04-07"
    assert current_period.start_date.strftime("%Y-%m-%d") == "2022-04-07"
    assert current_period.end_date.strftime("%Y-%m-%d") == "2022-04-14"
