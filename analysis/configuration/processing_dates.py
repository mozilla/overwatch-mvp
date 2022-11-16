import attr

from datetime import datetime, timedelta

from analysis.configuration.configs import Dataset


@attr.s(auto_attribs=True)
class ProcessingDateRange:
    start_date: datetime = attr.ib()
    end_date: datetime = attr.ib()


def calculate_date_ranges(
    dataset_config: Dataset, exclusive_end_date: datetime
) -> (ProcessingDateRange, ProcessingDateRange):
    """
    Determine the start and end dates of the baseline and current dataset periods exclusive of the
    provided date.
    baseline_start_date = date - (current_period + dataset_gap + baseline_period)
    baseline_end_date = date - (current_period + dataset_gap)

    current_start_date = date - current_period
    current_end_date = date

    date_range_offset is the number of days between the start of the baseline date range and the
    current date range.
    @param dataset_config: config loaded from the analysis profile configuration files.
    @param exclusive_end_date: date provided via Airflow.  The calculated date ranges will end on
    the prior day.
    @return: a tuple of ProcessingDateRanges (baseline, current)
    """
    baseline_period = dataset_config.baseline_period
    current_period = dataset_config.current_period
    period_offset = dataset_config.period_offset

    # the number of days between the end of the baseline date range and the start of the current
    # date range, may be negative if they overlap
    dataset_gap = period_offset - baseline_period

    start_date = exclusive_end_date - timedelta(
        days=(current_period + dataset_gap + baseline_period)
    )
    end_date = exclusive_end_date - timedelta(days=(current_period + dataset_gap))
    baseline_period = ProcessingDateRange(start_date=start_date, end_date=end_date)

    start_date = exclusive_end_date - timedelta(days=current_period)
    current_period = ProcessingDateRange(start_date=start_date, end_date=exclusive_end_date)

    return baseline_period, current_period
