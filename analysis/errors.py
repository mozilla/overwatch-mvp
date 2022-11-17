from analysis.configuration.processing_dates import ProcessingDateRange


class NoDataFoundForDateRange(Exception):
    def __init__(self, metric: str, query: str, date_range: ProcessingDateRange):
        super().__init__(
            f"No data found for metric: {metric} date_range: {date_range} query: \n{query}"
        )


class BigQueryPermissionsError(Exception):
    def __init__(self, metric: str, query: str, date_range: ProcessingDateRange, msg: str):
        super().__init__(
            f"Unable to access data for metric: {metric} date_range: {date_range} {msg }query: "
            f"\n{query} "
        )


class SqlNotDefined(Exception):
    def __init__(self, metric: str, table_name: str, filename: str):
        super().__init__(f"Sql not defined for table_name: {table_name}, expected file: {filename}")
