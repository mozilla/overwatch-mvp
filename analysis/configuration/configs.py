import attr


@attr.s(auto_attribs=True)
class PercentChange:
    contrib_to_overall_change_threshold_percent: int = attr.ib()
    dimensions: list = attr.ib()
    sort_by: list = attr.ib()


@attr.s(auto_attribs=True)
class Dataset:
    metric_name: str = attr.ib()
    table_name: str = attr.ib()
    date_range_offset: int = attr.ib()
    current_date_range: int = attr.ib()
    previous_date_range: int = attr.ib()
    app_name: str = attr.ib(None)


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