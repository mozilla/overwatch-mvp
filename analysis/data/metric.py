import os
from pathlib import Path
from typing import Any, Dict

import pandas as pd
from google.cloud import bigquery
from google.api_core.exceptions import Forbidden
from pandas import DataFrame

from jinja2 import Environment, FileSystemLoader, TemplateNotFound

from analysis.logging import logger
from analysis.configuration.processing_dates import ProcessingDateRange
from analysis.errors import (
    NoDataFoundForDateRangeError,
    BigQueryPermissionsError,
    SqlNotDefinedError,
)

PATH = Path(os.path.dirname(__file__))
TEMPLATE_FOLDER = PATH / "templates"


class MetricLookupManager:
    SUBMISSION_DATE_FORMAT = "%Y-%m-%d"

    def _render_sql(self, template_file: str, render_kwargs: Dict[str, Any]):
        """Render and return the SQL from a template."""
        file_loader = FileSystemLoader(TEMPLATE_FOLDER)
        env = Environment(loader=file_loader)
        try:
            template = env.get_template(template_file)
        except TemplateNotFound:
            raise SqlNotDefinedError(filename=template_file)

        sql = template.render(**render_kwargs)
        return sql

    def run_query(
        self,
        query: str,
        metric: str,
        date_range: ProcessingDateRange,
    ) -> DataFrame:
        bq_client = bigquery.Client()
        # TODO GLE wait for complete
        query_job = bq_client.query(query)
        try:
            df = query_job.to_dataframe()
        except Forbidden as e:
            raise BigQueryPermissionsError(metric=metric, query=query, msg=e.message)

        if "submission_date" in df.columns:
            df["submission_date"] = pd.to_datetime(df["submission_date"])
        df = df.rename(columns={"dimension_value": "dimension_value_0"})

        if df.empty:
            raise NoDataFoundForDateRangeError(metric=metric, query=query, date_range=date_range)
        return df

    def get_metric_with_date_range(
        self,
        metric_name: str,
        table_name: str,
        app_name: str,
        date_range: ProcessingDateRange,
        excluded_dimensions: list = None,
        included_dimensions_only: list = None,
    ) -> DataFrame:

        if excluded_dimensions is not None and included_dimensions_only is not None:
            raise ValueError("Cannot include both excluded_dimensions and included_dimensions_only")

        file = table_name + "_no_dim.sql"

        render_kwargs = {
            "metric": metric_name,
            "start_date": date_range.start_date.strftime(self.SUBMISSION_DATE_FORMAT),
            "end_date": date_range.end_date.strftime(self.SUBMISSION_DATE_FORMAT),
            "app_name": app_name,
            "exclude_dimension_values": excluded_dimensions,
            "included_dimensions_only": included_dimensions_only,
        }
        query = self._render_sql(template_file=file, render_kwargs=render_kwargs)
        # print(f"{query}")
        return self.run_query(
            query=query,
            metric=metric_name,
            date_range=date_range,
        )

    def get_metric_by_dimensions_with_date_range(
        self,
        metric_name: str,
        table_name: str,
        app_name: str,
        date_range: ProcessingDateRange,
        dimensions: list,  # indicates the permutation of the dimensions to evaluate.
        excluded_dimensions: list = None,
    ) -> DataFrame:
        file = table_name + "_by_dims.sql"

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

        render_kwargs = {
            "metric": metric_name,
            "start_date": date_range.start_date.strftime(self.SUBMISSION_DATE_FORMAT),
            "end_date": date_range.end_date.strftime(self.SUBMISSION_DATE_FORMAT),
            "app_name": app_name,
            "full_dim_value_spec": full_dim_value_spec,
            "full_dim_spec": full_dim_spec,
            "exclude_dimension_values": excluded_dimensions,
        }
        query = self._render_sql(template_file=file, render_kwargs=render_kwargs)

        df = self.run_query(
            query=query,
            metric=metric_name,
            date_range=date_range,
        )
        # Need to add indexing to the column indicating the dimension.
        i = 0
        result_df = DataFrame()

        for dim in dimensions:
            df["dimension_" + str(i)] = dim
            # Sometimes the dimension value was None and filtering was dropping it.  This resulted
            # assert checks failing (the contribution to change sum != 100, replacing with the text
            # "None" here so data is not lost.
            df["dimension_value_" + str(i)] = df["dimension_value_" + str(i)].fillna("None")
            i += 1

        result_df = pd.concat([result_df, df])
        result_df = result_df.dropna(axis="rows")
        return result_df
