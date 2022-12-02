from pandas import DataFrame
from analysis.detection.results.store import _build_table, _prepare_df_for_storage


def test_dataframe_matches_table_defn(
    mock_analysis_profile, mock_baseline_period, mock_current_period
):
    rows = [
        [0, 2.49, 95.58, 0.1, 96.58, "channel", "release"],
        [1, 7.67, 3.1, 0.1, 6.1, "channel", "beta"],
        [2, -3.98, -2.68, -2.68, 2.68, "channel", "nightly"],
    ]
    # The column 'index' is a result of how the dataframe is constructed in the processing.
    # Since this test os occurring post processing the column is artifically added here.
    cols = [
        "index",
        "percent_change",
        "contrib_to_overall_change",
        "change_in_proportion",
        "change_distance",
        "dimension",
        "dimension_value",
    ]
    df = DataFrame(rows, columns=cols)
    table = _build_table()
    prepared_df = _prepare_df_for_storage(
        mock_analysis_profile, mock_baseline_period, mock_current_period, df
    )

    # 1.  Need to get the list of columns from the table.
    # 2. Need to get the updated list of columns from the df.
    df_columns = prepared_df.columns
    table_fields = table.schema

    assert len(df_columns) == len(table_fields)
    for column in table_fields:
        assert column.name in df_columns
