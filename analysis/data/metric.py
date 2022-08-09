from datetime import datetime, timedelta

import pandas as pd
from google.cloud import bigquery
from pandas import DataFrame


class MetricLookupManager:
    SUBMISSION_DATE_FORMAT = "%Y-%m-%d"

    def __init__(self):
        # Note that for this query the returned column name must be metric_value for downstream
        # processing
        self.new_profiles_no_dimensions_by_date = """
            SELECT
                SUM(new_profiles) AS metric_value
            FROM
                `moz-fx-data-shared-prod.telemetry.active_users_aggregates`
            WHERE
                submission_date = @date_of_interest
            AND app_name="Fenix"
            GROUP BY
                submission_date,
                app_name
        """

        # Note that for this query the returned column name must be metric_value for downstream
        # processing
        self.new_profiles_by_dimensions_by_date = """
             SELECT
                @dimension as dimension_value,
                SUM(new_profiles) AS metric_value,
            FROM
                `moz-fx-data-shared-prod.telemetry.active_users_aggregates` a,
                `mozdata.static.country_codes_v1` c
            WHERE
                submission_date = @date_of_interest
                AND a.country = c.code
                AND app_name="Fenix"
            GROUP BY
                submission_date,
                app_name,
                @dimension
            ORDER BY
                @dimension
                """

        # Note that for this query the returned column name must be metric_value for downstream
        # processing
        self.mau_no_dimensions_by_date = """
                    SELECT
                        SUM(mau) AS metric_value
                    FROM
                        `moz-fx-data-shared-prod.telemetry.active_users_aggregates`
                    WHERE
                        submission_date = @date_of_interest
                    AND app_name="Firefox Desktop"
                    GROUP BY
                        submission_date,
                        app_name
                """

        # Note that for this query the returned column name must be metric_value for downstream
        # processing
        self.mau_by_dimensions_by_date = """
                     SELECT
                        @dimension as dimension_value,
                        SUM(mau) AS metric_value,
                    FROM
                        `moz-fx-data-shared-prod.telemetry.active_users_aggregates` a,
                        `mozdata.static.country_codes_v1` c
                    WHERE
                        submission_date = @date_of_interest
                        AND a.country = c.code
                        AND app_name="Firefox Desktop"
                    GROUP BY
                        submission_date,
                        app_name,
                        @dimension
                    ORDER BY
                        @dimension
                        """

        self.query_cache = {
            "new_profiles_no_dimensions_by_date": self.new_profiles_no_dimensions_by_date,
            "new_profiles_by_dimensions_by_date": self.new_profiles_by_dimensions_by_date,
            "mau_no_dimensions_by_date": self.mau_no_dimensions_by_date,
            "mau_by_dimensions_by_date": self.mau_by_dimensions_by_date,
        }

    def run_query(
        self,
        query: str,
        date_of_interest: datetime,
        historical_days_for_compare: int = 0,
        dimension: str = None,
    ) -> DataFrame:
        start_date = date_of_interest - timedelta(historical_days_for_compare)
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "start_date",
                    "STRING",
                    start_date.strftime(self.SUBMISSION_DATE_FORMAT),
                ),
                bigquery.ScalarQueryParameter(
                    "end_date",
                    "STRING",
                    date_of_interest.strftime(self.SUBMISSION_DATE_FORMAT),
                ),
                bigquery.ScalarQueryParameter(
                    "date_of_interest",
                    "STRING",
                    date_of_interest.strftime(self.SUBMISSION_DATE_FORMAT),
                ),
            ]
        )

        if dimension:
            query = query.replace("@dimension", dimension)
        bq_client = bigquery.Client()
        rows = bq_client.query(query, job_config=job_config)
        # The submission_date is dbdate, convert to datetime
        df = rows.to_dataframe()
        # run_version_1_poc includes the submission date column, run_version_2_poc does not.
        if "submission_date" in df.columns:
            df["submission_date"] = pd.to_datetime(df["submission_date"])
        return df

    def get_data_for_metric(
        self,
        metric_name: str,
        date_of_interest: datetime,
        historical_days_for_compare: int,
    ) -> DataFrame:
        query = self.query_cache.get(metric_name)
        return self.run_query(
            query=query,
            date_of_interest=date_of_interest,
            historical_days_for_compare=historical_days_for_compare,
        )

    def get_data_for_metric_by_geolocation(
        self,
        metric_name: str,
        column_name: str,
        date_of_interest: datetime,
        historical_days_for_compare: int,
    ) -> DataFrame:
        query = self.query_cache.get(metric_name + "_by_geolocation")
        return self.run_query(
            query=query,
            date_of_interest=date_of_interest,
            historical_days_for_compare=historical_days_for_compare,
            dimension=column_name,
        )

    def get_data_for_metric_with_date(
        self, metric_name: str, date_of_interest: datetime
    ) -> DataFrame:
        query = self.query_cache.get(metric_name + "_no_dimensions_by_date")
        return self.run_query(
            query=query,
            date_of_interest=date_of_interest,
        )

    def get_data_for_metric_by_dimensions_with_date(
        self, metric_name: str, date_of_interest: datetime, dimensions: list
    ) -> dict:
        query = self.query_cache.get(metric_name + "_by_dimensions_by_date")
        # Need to query each dimension individually and get the baseline and current.
        # return a dict of DataFrames keyed by dimension.
        result_df = DataFrame()
        for dimension in dimensions:
            print(f"processing dimension: {dimension}")
            df = self.run_query(
                query=query, date_of_interest=date_of_interest, dimension=dimension
            )
            df["dimension"] = dimension
            result_df = pd.concat([result_df, df])
        return result_df
