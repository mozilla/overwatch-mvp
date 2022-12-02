from analysis.detection.results.bigquery_client import BigQueryClient
from analysis.logging import logger
from analysis.configuration.configs import AnalysisProfile
from analysis.configuration.processing_dates import ProcessingDateRange

from google.cloud.bigquery.schema import SchemaField
from google.cloud.bigquery.table import Table
from pandas import DataFrame
from datetime import datetime
from google.cloud.bigquery import TimePartitioning, TimePartitioningType

DESTINATION_PROJECT = "automated-analysis-dev"
DESTINATION_DATASET = "overwatch"
PROCESSING_INFO_TABLE_NAME = "overwatch_run_analysis_info_v1"
PROCESSING_INFO_TABLE = f"{DESTINATION_PROJECT}.{DESTINATION_DATASET}.{PROCESSING_INFO_TABLE_NAME}"


def _create_bq_table_if_not_exist(bq_client, table):
    if not bq_client.bq_table_exists(table):
        logger.info(f"table: {table.table_id} does not exist, creating.")
        # TODO GLE sometimes there is a lag from when the table is 'created' to when it can be used.
        bq_client.create_table(table)


def _build_table() -> Table:
    schema = [
        SchemaField(name="processing_date", field_type="DATE"),
        SchemaField(name="name", field_type="STRING"),
        SchemaField(name="table_name", field_type="STRING"),
        SchemaField(name="metric_name", field_type="STRING"),
        SchemaField(name="app_name", field_type="STRING"),
        SchemaField(name="baseline_start_date", field_type="DATE"),
        SchemaField(name="baseline_end_date", field_type="DATE"),
        SchemaField(name="current_start_date", field_type="DATE"),
        SchemaField(name="current_end_date", field_type="DATE"),
        SchemaField(name="dimension", field_type="STRING"),
        SchemaField(name="dimension_value", field_type="STRING"),
        SchemaField(name="rank", field_type="INTEGER"),
        SchemaField(name="percent_change", field_type="NUMERIC"),
        SchemaField(name="contrib_to_overall_change", field_type="NUMERIC"),
        SchemaField(name="change_in_proportion", field_type="NUMERIC"),
        SchemaField(name="change_distance", field_type="NUMERIC"),
    ]

    table = Table(
        PROCESSING_INFO_TABLE,
        schema=schema,
    )
    table.time_partitioning = TimePartitioning(
        type_=TimePartitioningType.DAY,
        field="processing_date",
    )
    return table


def _get_analysis_info_table():
    table = _build_table()
    bigquery = BigQueryClient(
        project_id=DESTINATION_PROJECT,
        dataset=DESTINATION_DATASET,
    )

    _create_bq_table_if_not_exist(bigquery, table)

    return table


def _prepare_df_for_storage(
    profile: AnalysisProfile,
    baseline: ProcessingDateRange,
    current: ProcessingDateRange,
    insert_data: DataFrame,
) -> DataFrame:

    prepared_df = insert_data.reset_index(drop=True)
    prepared_df["name"] = profile.name
    prepared_df["app_name"] = profile.dataset.app_name
    prepared_df["processing_date"] = datetime.now().date()
    prepared_df["rank"] = prepared_df.index + 1
    prepared_df["baseline_start_date"] = baseline.start_date.date()
    prepared_df["baseline_end_date"] = baseline.end_date.date()
    prepared_df["current_start_date"] = current.start_date.date()
    prepared_df["current_end_date"] = current.end_date.date()
    prepared_df["table_name"] = profile.dataset.table_name
    prepared_df["metric_name"] = profile.dataset.metric_name
    prepared_df.drop(["index"], axis=1, inplace=True)
    return prepared_df


def insert_processing_info(
    profile: AnalysisProfile,
    baseline: ProcessingDateRange,
    current: ProcessingDateRange,
    insert_data: DataFrame,
):
    bigquery = BigQueryClient(project_id=DESTINATION_PROJECT, dataset=DESTINATION_DATASET)

    processing_table = _get_analysis_info_table()
    prepared_df = _prepare_df_for_storage(profile, baseline, current, insert_data)
    result = bigquery.client.insert_rows_from_dataframe(processing_table, prepared_df)

    if len(result) > 0 and len(result[0]) > 0:
        logger.error(
            f"There has been a problem inserting processing info into: {str(processing_table)}"
        )
        logger.error(str(result))
    else:
        logger.info(f"Processing info inserted into: {PROCESSING_INFO_TABLE}.")
