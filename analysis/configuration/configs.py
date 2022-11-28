import attr


@attr.s(auto_attribs=True)
class PercentChange:
    contrib_to_overall_change_threshold_percent: int = attr.ib()
    dimensions: list = attr.ib()
    sort_by: list = attr.ib()
    overall_threshold_percent: float = attr.ib(0)
    include_dimension_permutations: bool = attr.ib(True)
    results_rounding: int = attr.ib(2)
    limit_results: int = attr.ib(10)


@attr.s(auto_attribs=True)
class Dataset:
    metric_name: str = attr.ib()
    table_name: str = attr.ib()
    period_offset: int = attr.ib()
    current_period: int = attr.ib()
    baseline_period: int = attr.ib()
    app_name: str = attr.ib(None)
    processing_period_offset: int = attr.ib(0)


@attr.s(auto_attribs=True)
class AnalysisProfile:
    name: str = attr.ib()
    # In the future there may be other calculations types supported.
    percent_change: PercentChange = attr.ib()
    dataset: Dataset = attr.ib()


@attr.s(auto_attribs=True)
class Slack:
    channel: str = attr.ib()
    message: str = attr.ib()


@attr.s(auto_attribs=True)
class Report:
    template: str = attr.ib()


# In the future Notification will need to support multiple notification types (e.g. slack, jira).
@attr.s(auto_attribs=True)
class Notification:
    report: Report = attr.ib()
    slack: Slack = attr.ib()


@attr.s(auto_attribs=True)
class Config:
    analysis_profile: AnalysisProfile = attr.ib()
    notification: Notification = attr.ib()
