from datetime import datetime

from analysis.data.metric import MetricLookupManager
from analysis.detection.detector import Detector
from analysis.detection.explorer.geolocation import Geolocation
from analysis.detection.profile import AnalysisProfile, Detection
from analysis.notification.slack import SlackNotifier
from analysis.reports.generator import Generator


def analyze_metric(profile: AnalysisProfile, date_of_interest: datetime) -> Detection:
    df = MetricLookupManager().get_data_for_metric(
        metric_name=profile.metric_name,
        date_of_interest=date_of_interest,
        historical_days_for_compare=profile.historical_days_for_compare,
    )
    return Detector().apply_profile(profile, df, date_of_interest)


def explore_detection(profile: AnalysisProfile, date_of_interest: datetime) -> dict:
    evaluation_result = Geolocation(
        profile=profile, date_of_interest=date_of_interest
    ).run_evaluation()
    return evaluation_result


def issue_report(trigger: Detection, evaluation: dict):
    report_generator = Generator(
        working_dir=".", analysis_details=trigger, evaluation=evaluation
    )
    pdfreport_filename = report_generator.build_pdf_report()
    notifier = SlackNotifier(
        output_pdf=pdfreport_filename, metric_name=trigger.metric_name
    )
    notifier.publish_pdf_report()


if __name__ == "__main__":
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

    for (profile, date) in analysis_profiles:
        detection = analyze_metric(profile=profile, date_of_interest=date)
        evaluation = explore_detection(profile=profile, date_of_interest=date)
        issue_report(trigger=detection, evaluation=evaluation)
