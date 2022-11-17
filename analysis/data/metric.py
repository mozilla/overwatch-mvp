import os
from pathlib import Path

import pandas as pd
from google.cloud import bigquery
from google.api_core.exceptions import Forbidden
from pandas import DataFrame

from analysis.logging import logger
from analysis.configuration.processing_dates import ProcessingDateRange
from analysis.errors import NoDataFoundForDateRange, BigQueryPermissionsError, SqlNotDefined

PATH = Path(os.path.dirname(__file__))
TEMPLATE_FOLDER = PATH / "templates"


class MetricLookupManager:
    SUBMISSION_DATE_FORMAT = "%Y-%m-%d"

    def run_query(
        self,
        query: str,
        metric: str,
        app_name: str,
        date_range: ProcessingDateRange,
        dimension: str = None,
        full_dim_spec: str = None,
        full_dim_value_spec: str = None,
    ) -> DataFrame:
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "start_date",
                    "STRING",
                    date_range.start_date.strftime(self.SUBMISSION_DATE_FORMAT),
                ),
                bigquery.ScalarQueryParameter(
                    "end_date",
                    "STRING",
                    date_range.end_date.strftime(self.SUBMISSION_DATE_FORMAT),
                ),
            ]
        )

        query = query.replace(
            "@window_end_date", date_range.end_date.strftime(self.SUBMISSION_DATE_FORMAT)
        )

        if dimension:
            query = query.replace("@dimension", dimension)

        if full_dim_spec and full_dim_value_spec:
            query = query.replace("@full_dim_value_spec", full_dim_value_spec)
            query = query.replace("@full_dim_spec", full_dim_spec)

        query = query.replace("@metric", metric)

        if app_name:
            query = query.replace("@app_name", app_name)

        bq_client = bigquery.Client()
        # TODO GLE wait for complete
        query_job = bq_client.query(query, job_config=job_config)

        # The submission_date is dbdate, convert to datetime
        try:
            df = query_job.to_dataframe()
        except Forbidden as e:
            raise BigQueryPermissionsError(
                metric=metric, query=query, date_range=date_range, msg=e.message
            )
        # run_version_1_poc includes the submission date column, run_version_2_poc does not.
        if "submission_date" in df.columns:
            df["submission_date"] = pd.to_datetime(df["submission_date"])
        df = df.rename(columns={"dimension_value": "dimension_value_0"})

        if df.empty:
            raise NoDataFoundForDateRange(metric=metric, query=query, date_range=date_range)
        return df

    def get_metric_with_date_range(
        self,
        metric_name: str,
        table_name: str,
        app_name: str,
        date_range: ProcessingDateRange,
    ) -> DataFrame:
        file = TEMPLATE_FOLDER / (table_name + "_no_dim.sql")
        if not file.is_file():
            raise SqlNotDefined(metric=metric_name, table_name=table_name, filename=file)

        with open(file) as f:
            query = f.read()
        return self.run_query(
            query=query,
            metric=metric_name,
            app_name=app_name,
            date_range=date_range,
        )

    def get_metric_by_dimensions_with_date_range(
        self,
        metric_name: str,
        table_name: str,
        app_name: str,
        date_range: ProcessingDateRange,
        dimensions: list,  # indicates the permutation of the dimensions to evaluate.
    ) -> DataFrame:
        file = TEMPLATE_FOLDER / (table_name + "_by_dims.sql")
        if not file.is_file():
            raise SqlNotDefined(metric=metric_name, table_name=table_name, filename=file)
        with open(file) as f:
            query = f.read()

        result_df = DataFrame()
        dim_value_spec = "@dimension as dimension_value"
        dim_spec = "@dimension"
        full_dim_value_spec = ""
        full_dim_spec = ""

        # Build up the list of dimensions=
        for dim in dimensions:
            if len(full_dim_value_spec) > 0:
                full_dim_value_spec += ","
                full_dim_spec += ","
            full_dim_value_spec += dim_value_spec.replace("@dimension", dim, 1)
            full_dim_spec += dim_spec.replace("@dimension", dim, 1)

        logger.info(f"processing dimensions: {full_dim_spec}")
        df = self.run_query(
            query=query,
            metric=metric_name,
            app_name=app_name,
            date_range=date_range,
            dimension=None,
            full_dim_spec=full_dim_spec,
            full_dim_value_spec=full_dim_value_spec,
        )
        # Need to add indexing to the column indicating the dimension.
        i = 0
        for dim in dimensions:
            df["dimension_" + str(i)] = dim
            i += 1

        result_df = pd.concat([result_df, df])

        result_df = result_df.dropna(axis="rows")
        return result_df
