import pandas as pd
from google.cloud import bigquery
from pandas import DataFrame

from analysis.logging import logger


class MetricLookupManager:
    SUBMISSION_DATE_FORMAT = "%Y-%m-%d"

    # TODO GLE Unwieldy, move to config_files files
    def __init__(self):
        # Note that for this query the returned column name must be metric_value for downstream
        # processing

        # Note that for this query the returned column name must be metric_value for downstream
        # processing
        self.metric_no_dim_aua = """
             SELECT window_average AS metric_value from (
                SELECT
                    *,
                    AVG(metric_value) OVER (ORDER BY submission_date
                    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS window_average
                FROM (
                    SELECT
                        submission_date,
                        app_name,
                        SUM(@metric) AS metric_value
                    FROM
                        `moz-fx-data-shared-prod.telemetry.active_users_aggregates` a
                    WHERE
                        submission_date >= @start_date
                        AND submission_date <= @end_date
                        AND app_name="@app_name"
                    GROUP BY
                        submission_date,
                        app_name
                ) AS t1
                ORDER BY
                submission_date
            )
            where  submission_date = @end_date
        """

        # Note that for this query the returned column name must be metric_value for downstream
        # processing
        self.metric_by_dim_aua = """
             SELECT @dimension as dimension_value, window_average AS metric_value from (
                SELECT
                *,
                AVG(metric_value) OVER (PARTITION BY @dimension ORDER BY submission_date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS window_average
                FROM (
                    SELECT
                        submission_date,
                        @dimension,
                        app_name,
                        SUM(@metric) AS metric_value
                    FROM
                        `moz-fx-data-shared-prod.telemetry.active_users_aggregates` a,
                        `mozdata.static.country_codes_v1` c
                    WHERE
                        submission_date >= @start_date
                        AND submission_date <= @end_date
                        AND app_name="@app_name"
                        AND a.country = c.code
                    GROUP BY
                        submission_date,
                        @dimension,
                        app_name
                ) AS t1
                ORDER BY
                    @dimension,
                    submission_date
            )
            WHERE submission_date = @end_date
        """

        self.metric_by_multi_dim_aua = """
            SELECT
                @full_dim_value_spec,
                window_average AS metric_value
            FROM (
                SELECT
                    *,
                    AVG(metric_value) OVER (
                    PARTITION BY @full_dim_spec ORDER BY submission_date
                    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS window_average
                FROM (
                    SELECT
                        submission_date,
                        @full_dim_spec,
                        SUM(@metric) AS metric_value
                    FROM
                        `moz-fx-data-shared-prod.telemetry.active_users_aggregates` a,
                        `mozdata.static.country_codes_v1` c
                    WHERE
                        submission_date >= @start_date
                        AND submission_date <= @end_date
                        AND app_name="@app_name"
                        AND a.country = c.code
                    GROUP BY
                        submission_date,
                        @full_dim_spec
                    ) AS t1
                    ORDER BY
                        @full_dim_spec,
                        submission_date
                )
            WHERE submission_date = @end_date
        """

        # Note that for this query the returned column name must be metric_value for downstream
        # processing
        self.metric_no_dim_sms = """
            SELECT window_average AS metric_value from (
                SELECT
                    *,
                    AVG(metric_value) OVER (ORDER BY submission_date
                    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS window_average
                FROM (
                    SELECT
                        date as submission_date,
                        sum(@metric) AS metric_value
                    FROM
                        `moz-fx-data-marketing-prod.ga_derived.www_site_metrics_summary_v1`
                    WHERE
                        date >= @start_date
                        AND date <= @end_date
                    GROUP BY date
                    ORDER BY date desc
                ) AS t1
                ORDER BY
                submission_date
            )
            where submission_date = @end_date
        """

        # Note that for this query the returned column name must be metric_value for downstream
        # processing
        self.metric_by_dim_sms = """
           SELECT dimension_value, window_average AS metric_value from (
                SELECT
                    *,
                    AVG(metric_value) OVER (PARTITION BY dimension_value ORDER BY submission_date
                    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS window_average
                FROM (
                    SELECT
                        date as submission_date,
                        @dimension as dimension_value,
                        sum(@metric) AS metric_value
                    FROM
                        `moz-fx-data-marketing-prod.ga_derived.www_site_metrics_summary_v1`
                    WHERE
                        date >= @start_date
                        AND date <= @end_date
                    GROUP BY @dimension, date
                    ORDER BY @dimension, date
                ) AS t1
                ORDER BY
                submission_date
            )
            where submission_date = @end_date
        """

        self.query_cache = {
            "active_user_aggregates_no_dim_by_date_range": self.metric_no_dim_aua,
            "active_user_aggregates_by_dim_by_date_range": self.metric_by_dim_aua,
            "www_site_metrics_summary_v1_no_dim_by_date_range": self.metric_no_dim_sms,
            "www_site_metrics_summary_v1_by_dim_by_date_range": self.metric_by_dim_sms,
            "active_user_aggregates_by_multi_dim_by_date_range": self.metric_by_multi_dim_aua,
        }

    def run_query(
        self,
        query: str,
        metric: str,
        app_name: str,
        date_range: dict,
        dimension: str = None,
        full_dim_spec: str = None,
        full_dim_value_spec: str = None,
    ) -> DataFrame:
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "start_date",
                    "STRING",
                    date_range.get("start_date").strftime(self.SUBMISSION_DATE_FORMAT),
                ),
                bigquery.ScalarQueryParameter(
                    "end_date",
                    "STRING",
                    date_range.get("end_date").strftime(self.SUBMISSION_DATE_FORMAT),
                ),
            ]
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
        df = query_job.to_dataframe()
        # run_version_1_poc includes the submission date column, run_version_2_poc does not.
        if "submission_date" in df.columns:
            df["submission_date"] = pd.to_datetime(df["submission_date"])
        df = df.rename(columns={"dimension_value": "dimension_value_0"})
        return df

    def get_metric_with_date_range(
        self,
        metric_name: str,
        table_name: str,
        app_name: str,
        date_range: dict,
    ) -> DataFrame:
        query = self.query_cache.get(table_name + "_no_dim_by_date_range")
        return self.run_query(
            query=query,
            metric=metric_name,
            app_name=app_name,
            date_range=date_range,
        )

    def get_metric_by_dimension_with_date_range(
        self,
        metric_name: str,
        table_name: str,
        app_name: str,
        date_range: dict,
        dimension: str,
    ) -> DataFrame:
        query = self.query_cache.get(table_name + "_by_dim_by_date_range")
        # Need to query each dimension individually and get the baseline and current.
        # return a dict of DataFrames keyed by dimension.

        logger.info(f"processing dimension: {dimension}")
        result_df = self.run_query(
            query=query,
            metric=metric_name,
            app_name=app_name,
            date_range=date_range,
            dimension=dimension,
        )
        result_df["dimension"] = dimension
        result_df = result_df.dropna(axis="rows")
        return result_df

    def get_metric_by_multi_dimensions_with_date_range(
        self,
        metric_name: str,
        table_name: str,
        app_name: str,
        date_range: dict,
        dimensions: list,  # indicates the permutation of the dimensions to evaluate.
    ) -> DataFrame:
        query = self.query_cache.get(table_name + "_by_multi_dim_by_date_range")
        # Need to query each dimension individually and get the baseline and current.
        # return a dict of DataFrames keyed by dimension.
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
