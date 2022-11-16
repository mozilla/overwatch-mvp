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
    Determine the start and end dates of the previous and current dataset ranges exclusive of the
    provided date.
    previous_start_date = date - (current_date_range + dataset_gap + previous_date_range)
    previous_end_date = date - (current_date_range + dataset_gap)

    current_start_date = date - current_date_range
    current_end_date = date

    date_range_offset is the number of days between the start of the previous date range and the
    current date range.
    @param dataset_config: config loaded from the analysis profile configuration files.
    @param exclusive_end_date: date provided via Airflow.  The calculated date ranges will end on
    the prior day.
    @return: a tuple of ProcessingDateRanges (previous, current)
    """
    previous_date_range = dataset_config.previous_date_range
    current_date_range = dataset_config.current_date_range
    date_range_offset = dataset_config.date_range_offset

    # the number of days between the end of the previous date range and the start of the current
    # date range, may be negative if they overlap
    dataset_gap = date_range_offset - previous_date_range

    previous_start_date = exclusive_end_date - timedelta(
        days=(current_date_range + dataset_gap + previous_date_range)
    )
    previous_end_date = exclusive_end_date - timedelta(days=(current_date_range + dataset_gap))
    previous_date_range = ProcessingDateRange(
        start_date=previous_start_date, end_date=previous_end_date
    )

    current_start_date = exclusive_end_date - timedelta(days=current_date_range)
    current_date_range = ProcessingDateRange(
        start_date=current_start_date, end_date=exclusive_end_date
    )

    return previous_date_range, current_date_range
