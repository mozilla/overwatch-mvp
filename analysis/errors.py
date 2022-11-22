from analysis.configuration.processing_dates import ProcessingDateRange


class NoDataFoundForDateRangeError(Exception):
    def __init__(self, metric: str, query: str, date_range: ProcessingDateRange):
        super().__init__(
            f"No data found for metric: {metric} date_range: {date_range} query: \n{query}"
        )


class BigQueryPermissionsError(Exception):
    def __init__(self, metric: str, query: str, msg: str):
        super().__init__(f"Unable to access data for metric: {metric} {msg } query: " f"\n{query} ")


class SqlNotDefinedError(Exception):
    def __init__(self, filename: str):
        super().__init__(f"Sql missing, expected file: {filename}")
