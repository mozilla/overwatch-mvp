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
    cols = ["dimension_value", "metric_value", "dimension", "timeframe"]
    return DataFrame(rows, columns=cols)


@pytest.fixture
def mock_baseline_dimension_values():
    rows = [
        ["mx", 15, "country", "baseline"],
        ["ca", 21, "country", "baseline"],
        ["us", 80, "country", "baseline"],
    ]
    cols = ["dimension_value", "metric_value", "dimension", "timeframe"]
    return DataFrame(rows, columns=cols)


@pytest.fixture
def parent_df(mock_current_parent_values, mock_baseline_parent_values):
    return concat([mock_current_parent_values, mock_baseline_parent_values])


@pytest.fixture
def dimension_df(mock_current_dimension_values, mock_baseline_dimension_values):
    return concat([mock_current_dimension_values, mock_baseline_dimension_values])
