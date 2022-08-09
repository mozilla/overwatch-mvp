from datetime import datetime

from analysis.detection.explorer.top_level import TopLevelEvaluator
from analysis.detection.profile import AnalysisProfile


def test_percent_change(parent_df):
    date_of_interest = datetime.strptime("2022-04-09", "%Y-%m-%d")
    new_profiles_ap = AnalysisProfile(
        metric_name="new_profiles",
        app_name="Fenix",
        dimensions=[
            "country",
        ],
    )
    expected_percent = round(100 * (124 - 116) / 116, 4)

    percent_change = TopLevelEvaluator(
        profile=new_profiles_ap, date_of_interest=date_of_interest
    )._calculate_percent_change(df=parent_df)
    assert percent_change == expected_percent
