from datetime import datetime, timedelta

import pandas as pd
from google.cloud import bigquery
from pandas import DataFrame


class MetricLookupManager:
    SUBMISSION_DATE_FORMAT = "%Y-%m-%d"

    def __init__(self):
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
            "mau": self.mau,
            "mau_by_geolocation": self.mau_by_geolocation,
        }

    def run_query(
        self,
        query: str,
        date_of_interest: datetime,
        historical_days_for_compare: int,
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
            ]
        )

        if dimension:
            query = query.replace("@dimension", dimension)
        bq_client = bigquery.Client()
        rows = bq_client.query(query, job_config=job_config)
        # The submission_date is dbdate, convert to datetime
        df = rows.to_dataframe()
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

    def get_data_for_metric_with_column(
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
