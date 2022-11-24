from analysis.cli import exceeds_top_level_percent_change
from analysis.configuration.configs import Config
from analysis.configuration.processing_dates import ProcessingDateRange


def test_exceeds_top_level_percent_change(
    mock_config: Config,
    mock_current_period: ProcessingDateRange,
    mock_baseline_period: ProcessingDateRange,
    get_mock_get_current_and_baseline_values_func,
):
    assert exceeds_top_level_percent_change(
        mock_config.analysis_profile, {"top_level_percent_change": 0.8}
    )
    assert not exceeds_top_level_percent_change(
        mock_config.analysis_profile, {"top_level_percent_change": 0.1}
    )
