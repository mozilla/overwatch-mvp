from datetime import datetime, timedelta

import pandas as pd
from google.cloud import bigquery
from pandas import DataFrame


class MetricLookupManager:
    SUBMISSION_DATE_FORMAT = "%Y-%m-%d"

    def __init__(self):
        # PoC ver 1
        self.new_profiles = """
            SELECT * from `automated-analysis-dev.sample_data.fenix_new_profiles`
            WHERE submission_date >= @start_date
            AND submission_date <= @end_date
            ORDER BY submission_date ASC;
            """

        self.new_profiles_by_geolocation = """
            SELECT
                *,
                AVG(t1.new_profiles) OVER (PARTITION BY @dimension ORDER BY submission_date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS new_profiles_7day_ma
            FROM (
                SELECT
                    submission_date,
                    @dimension,
                    app_name,
                    SUM(new_profiles) AS new_profiles
                FROM
                    `moz-fx-data-shared-prod.telemetry.active_users_aggregates` a,
                    `mozdata.static.country_codes_v1` c
                WHERE
                    submission_date >= @start_date
                    AND submission_date <= @end_date
                    AND app_name="Fenix"
                    AND a.country = c.code
                GROUP BY
                    submission_date,
                    @dimension,
                    app_name
            ) AS t1
            ORDER BY
            @dimension,
            submission_date
        """
        # PoC ver 2
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
        # PoC ver 2
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

        self.mau_by_geolocation = """
            SELECT
                @dimension,
                submission_date,
                SUM(mau) AS mau
            FROM
                `mozdata.telemetry.firefox_desktop_exact_mau28_by_dimensions_v1` m,
                `mozdata.static.country_codes_v1` c
            WHERE
                submission_date = @start_date
                OR submission_date = @end_date
                AND m.country = c.code
            GROUP BY
                @dimension,
                submission_date
            ORDER BY
                @dimension,
                submission_date
        """

        self.mau = """
            SELECT
                submission_date,
                SUM(mau) AS mau
            FROM
                `mozdata.telemetry.firefox_desktop_exact_mau28_by_dimensions_v1`
            WHERE
                submission_date = @start_date
                OR submission_date = @end_date
            GROUP BY
                submission_date
            ORDER BY
                submission_date
        """
        self.query_cache = {
            "new_profiles": self.new_profiles,
            "new_profiles_by_geolocation": self.new_profiles_by_geolocation,
            "new_profiles_no_dimensions_by_date": self.new_profiles_no_dimensions_by_date,
            "new_profiles_by_dimensions_by_date": self.new_profiles_by_dimensions_by_date,
            "mau": self.mau,
            "mau_by_geolocation": self.mau_by_geolocation,
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
