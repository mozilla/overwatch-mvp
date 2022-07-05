from datetime import datetime, timedelta

from pandas import DataFrame

from analysis.data.metric import MetricLookupManager
from analysis.detection.detector import Detector
from analysis.detection.explorer.geolocation import Geolocation
from analysis.detection.explorer.single_dimension import SingleDimensionEvaluator
from analysis.detection.profile import AnalysisProfile, Detection
from analysis.notification.slack import SlackNotifier
from analysis.reports.generator import Generator


def analyze_metric(profile: AnalysisProfile, date_of_interest: datetime) -> Detection:
    # TODO GLE Current implementation of AnalysisProfile contains the historical_days_for_compare
    #  value needed for the data retrieval, not sure if there is any way to get around having to
    #  pull the AnalysisProfile before getting the data.
    df = MetricLookupManager().get_data_for_metric(
        metric_name=name,
        date_of_interest=date_of_interest,
        historical_days_for_compare=profile.historical_days_for_compare,
    )
    return Detector().apply_profile(profile, df, date_of_interest)


# TODO GLE Need to return EvaluationResult
def explore_detection(profile: AnalysisProfile, date_of_interest: datetime) -> dict:
    evaluation_result = Geolocation(
        profile=profile, date_of_interest=date_of_interest
    ).run_evaluation()
    return evaluation_result


def issue_report(trigger: Detection, evaluation: dict):
    report_generator = Generator(working_dir=".")
    pdfreport_filename = report_generator.build_pdf_report(
        analysis_details=trigger, evaluation=evaluation
    )
    notifier = SlackNotifier(
        output_pdf=pdfreport_filename, metric_name=trigger.metric_name
    )
    notifier.publish_pdf_report()


if __name__ == "__main__":
    name = "new_profiles"
    date_of_interest = "2022-04-09"
    date_of_interest = datetime.strptime(date_of_interest, "%Y-%m-%d")

    # TODO GLE This will be retrieved from somewhere
    # This format of profile breaks the plan of having a re-usable configuration
    profile = AnalysisProfile(
        name,
        threshold_percent=-0.005,
        historical_days_for_compare=1,
    )
    detection = analyze_metric(profile=profile, date_of_interest=date_of_interest)

    # detection = Detection(metric_name="new_profiles", date_of_interest=date_of_interest,
    # baseline_value=624450, current_value=619355, threshold_percent=-0.005)
    evaluation = explore_detection(profile=profile, date_of_interest=date_of_interest)
    issue_report(trigger=detection, evaluation=evaluation)
