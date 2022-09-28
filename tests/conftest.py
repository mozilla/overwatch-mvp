from datetime import datetime

import pytest
from pandas import DataFrame, concat


@pytest.fixture
def mock_current_parent_values():
    rows = [
        [124, "current"],
    ]
    cols = ["metric_value", "timeframe"]
    return DataFrame(rows, columns=cols)


@pytest.fixture
def mock_baseline_parent_values():
    rows = [
        [116, "baseline"],
    ]
    cols = ["metric_value", "timeframe"]
    return DataFrame(rows, columns=cols)


@pytest.fixture
def mock_current_dimension_values():
    rows = [
        ["mx", 19, "country", "current"],
        ["ca", 24, "country", "current"],
        ["us", 81, "country", "current"],
    ]
    cols = ["dimension_value_0", "metric_value", "dimension_0", "timeframe"]
    return DataFrame(rows, columns=cols)


@pytest.fixture
def mock_current_multi_dimension_values():
    rows = [
        ["mx", 10, "country", "release", "channel", "current"],
        ["mx", 5, "country", "beta", "channel", "current"],
        ["mx", 4, "country", "nightly", "channel", "current"],
        ["ca", 18, "country", "release", "channel", "current"],
        ["ca", 4, "country", "beta", "channel", "current"],
        ["ca", 2, "country", "nightly", "channel", "current"],
        ["us", 70, "country", "release", "channel", "current"],
        ["us", 10, "country", "beta", "channel", "current"],
        ["us", 1, "country", "nightly", "channel", "current"],
    ]
    cols = [
        "dimension_value_0",
        "metric_value",
        "dimension_0",
        "dimension_value_1",
        "dimension_1",
        "timeframe",
    ]
    return DataFrame(rows, columns=cols)


@pytest.fixture
def mock_baseline_dimension_values():
    rows = [
        ["mx", 15, "country", "baseline"],
        ["ca", 21, "country", "baseline"],
        ["us", 80, "country", "baseline"],
    ]
    cols = ["dimension_value_0", "metric_value", "dimension_0", "timeframe"]
    return DataFrame(rows, columns=cols)


@pytest.fixture
def mock_baseline_multi_dimension_values():
    rows = [
        ["mx", 10, "country", "release", "channel", "baseline"],
        ["mx", 3, "country", "beta", "channel", "baseline"],
        ["mx", 2, "country", "nightly", "channel", "baseline"],
        ["ca", 11, "country", "release", "channel", "baseline"],
        ["ca", 6, "country", "beta", "channel", "baseline"],
        ["ca", 4, "country", "nightly", "channel", "baseline"],
        ["us", 63, "country", "release", "channel", "baseline"],
        ["us", 12, "country", "beta", "channel", "baseline"],
        ["us", 5, "country", "nightly", "channel", "baseline"],
    ]
    cols = [
        "dimension_value_0",
        "metric_value",
        "dimension_0",
        "dimension_value_1",
        "dimension_1",
        "timeframe",
    ]
    return DataFrame(rows, columns=cols)


@pytest.fixture
def parent_df(mock_current_parent_values, mock_baseline_parent_values):
    return concat([mock_current_parent_values, mock_baseline_parent_values])


@pytest.fixture
def dimension_df(mock_current_dimension_values, mock_baseline_dimension_values):
    return concat([mock_current_dimension_values, mock_baseline_dimension_values])


@pytest.fixture
def multi_dimension_df(mock_current_multi_dimension_values, mock_baseline_multi_dimension_values):
    return concat([mock_current_multi_dimension_values, mock_baseline_multi_dimension_values])


@pytest.fixture
def date_ranges_of_interest():
    return {
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
