from analysis.configuration.configs import Config


def test_load_config(mock_config: Config):
    assert mock_config.analysis_profile
    assert mock_config.analysis_profile.name == "Config with all fields"

    assert mock_config.analysis_profile.percent_change
    assert (
        mock_config.analysis_profile.percent_change.contrib_to_overall_change_threshold_percent == 1
    )
    assert mock_config.analysis_profile.percent_change.include_dimension_permutations is True
    assert mock_config.analysis_profile.percent_change.dimensions == [
        "country",
        "channel",
    ]
    assert mock_config.analysis_profile.percent_change.sort_by == [
        "change_distance",
        "contrib_to_overall_change",
        "percent_change",
        "change_in_proportion",
    ]
    assert mock_config.analysis_profile.percent_change.results_rounding == 2
    assert mock_config.analysis_profile.percent_change.overall_threshold_percent == 0.5

    assert mock_config.analysis_profile.dataset
    assert mock_config.analysis_profile.dataset.metric_name == "unit_test_metric"
    assert mock_config.analysis_profile.dataset.table_name == "test"
    assert mock_config.analysis_profile.dataset.app_name == "unit test app_name"
    assert mock_config.analysis_profile.dataset.period_offset == 14
    assert mock_config.analysis_profile.dataset.current_period == 1
    assert mock_config.analysis_profile.dataset.baseline_period == 7

    assert mock_config.notification
    assert mock_config.notification.report
    assert mock_config.notification.report.template == "report_version2.html.j2"

    assert mock_config.notification.slack
    assert mock_config.notification.slack.channel == "#unit-test-channel"
    assert mock_config.notification.slack.message == "Unit test message"
