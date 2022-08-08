import sys
from datetime import datetime

from analysis.data.metric import MetricLookupManager
from analysis.detection.detector import Detector
from analysis.detection.explorer.geolocation import Geolocation
from analysis.detection.explorer.one_dimension import OneDimensionEvaluator
from analysis.detection.explorer.top_level import TopLevelEvaluator
from analysis.detection.profile import AnalysisProfile, Detection
from analysis.notification.slack import SlackNotifier
from analysis.reports.generator import Generator, PercentChangeGenerator


def analyze_metric(profile: AnalysisProfile, date_of_interest: datetime) -> Detection:
    df = MetricLookupManager().get_data_for_metric(
        metric_name=profile.metric_name,
        date_of_interest=date_of_interest,
        historical_days_for_compare=profile.historical_days_for_compare,
    )
    return Detector().apply_profile(profile, df, date_of_interest)


def explore_detection_by_geolocation(
    profile: AnalysisProfile, date_of_interest: datetime
) -> dict:
    evaluation_result = Geolocation(
        profile=profile, date_of_interest=date_of_interest
    ).run_evaluation()
    return evaluation_result


def find_significant_dimensions(
    profile: AnalysisProfile, date_of_interest: datetime
) -> dict:
    # 1.  Find overall percent change
    top_level_evaluator = TopLevelEvaluator(
        profile=profile, date_of_interest=date_of_interest
    )
    top_level_evaluation = top_level_evaluator.evaluate()
    print(f"top_level_evaluation: {top_level_evaluation}")

    # 2. Find
    # - percent change
    # - contribution to overall change for each value of single dimension.
    # - change to contribution of overall value for each value of single dimension.

    one_dim_evaluator = OneDimensionEvaluator(
        profile=profile, date_of_interest=date_of_interest
    )
    one_dim_evaluation = one_dim_evaluator.evaluate()
    print(f"one_dim_evaluation: {one_dim_evaluation}")

    return top_level_evaluation | one_dim_evaluation


def issue_report_version_1(trigger: Detection, evaluation: dict):
    report_generator = Generator(
        working_dir=".",
        template="report.html.j2",
        analysis_details=trigger,
        evaluation=evaluation,
    )
    pdfreport_filename = report_generator.build_pdf_report()
    notifier = SlackNotifier(
        output_pdf=pdfreport_filename, metric_name=trigger.metric_name
    )
    notifier.publish_pdf_report()


def issue_report_version_2(
    profile: AnalysisProfile, evaluation: dict, date_of_interest
):
    evaluation["profile"] = profile

    report_generator = PercentChangeGenerator(
        working_dir=".",
        template="report_version2.html.j2",
        evaluation=evaluation,
        date_of_interest=date_of_interest,
    )

    pdfreport_filename = report_generator.build_pdf_report()
    notifier = SlackNotifier(
        output_pdf=pdfreport_filename, metric_name=profile.metric_name
    )
    notifier.publish_pdf_report()


def run_version_1_poc():
    # TODO GLE This will be retrieved from somewhere
    #  This format of profile breaks the plan of having a re-usable configuration
    # TODO GLE Current implementation of AnalysisProfile contains the historical_days_for_compare
    #  value needed for the data retrieval, not sure if there is any way to get around having to
    #  pull the AnalysisProfile before getting the data.

    # Scenario 1
    new_profiles_date_of_interest = datetime.strptime("2022-04-09", "%Y-%m-%d")
    new_profiles_ap = AnalysisProfile(
        metric_name="new_profiles",
        threshold_percent=-0.005,
        # historical_days_for_compare needs to be 1 for the current GeoLocation eval to run.
        historical_days_for_compare=1,
        index_fields=["submission_date", "app_name", "canonical_app_name"],
    )

    # Scenario #2
    mau_date_of_interest = datetime.strptime("2021-11-30", "%Y-%m-%d")
    mau_ap = AnalysisProfile(
        metric_name="mau",
        threshold_percent=-0.005,
        historical_days_for_compare=30,
        index_fields=["submission_date"],
    )

    analysis_profiles = [
        (new_profiles_ap, new_profiles_date_of_interest),
        (mau_ap, mau_date_of_interest),
    ]

    # Version 1 of PoC.
    for (profile, date) in analysis_profiles:
        detection = analyze_metric(profile=profile, date_of_interest=date)
        evaluation = explore_detection_by_geolocation(
            profile=profile, date_of_interest=date
        )
        issue_report_version_1(trigger=detection, evaluation=evaluation)


def run_version_2_poc():
    new_profiles_date_of_interest = datetime.strptime("2022-04-11", "%Y-%m-%d")

    new_profiles_ap = AnalysisProfile(
        # in this case threshold_percent is the threshold for contribution to overall change
        threshold_percent=1,
        metric_name="new_profiles",
        historical_days_for_compare=14,  # 14 days highlighted the new_profiles drop in early April
        dimensions=[
            "region_name",
            "subregion_name",
            "country",
            # "segment",  # not a good example
            # "channel",
            # "os",  # not a good example
            # "os_version",
            # "attribution_medium",
            # "attribution_source",
        ],
        sort_by=[
            "contrib_to_overall_change",
            "percent_change",
            "change_to_contrib",
            "percent_significance",
        ],
    )

    significant_dims = find_significant_dimensions(
        profile=new_profiles_ap, date_of_interest=new_profiles_date_of_interest
    )
    issue_report_version_2(
        profile=new_profiles_ap,
        evaluation=significant_dims,
        date_of_interest=new_profiles_date_of_interest,
    )


if __name__ == "__main__":
    if len(sys.argv) == 2:
        version = sys.argv[1]
    else:
        sys.exit(1)

    if version == "1":
        run_version_1_poc()
    else:
        run_version_2_poc()
